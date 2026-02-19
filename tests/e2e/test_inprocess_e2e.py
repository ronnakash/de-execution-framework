"""In-process E2E tests (Option A).

All pipeline services (REST Starter, Kafka Starter, Normalizer, Persistence,
Algos, Data API) run as real asyncio tasks sharing in-memory stubs. Tests
interact with the pipeline via:

  - REST path:   real HTTP requests to RestStarterModule's HTTP server
  - Kafka path:  publishing directly to MemoryQueue "client_*" topics
  - Files path:  running FileProcessorModule.run() inline (it's a Job)

Each test gets a fresh pipeline instance (function-scoped fixture) so there is
no cross-test state leakage.  The 34 tests mirror the scenarios in e2e_plan.md:

  Matrix (3 methods × 3 event types × 3 scenarios = 27 tests):
    1. 100 valid events  → 100 rows in the correct table, visible via Data API
    2. 100 invalid events → 100 rows in normalization_errors, 0 in valid table
    3. Same event 100×  → 1 valid row + 99 in duplicates table

  General (7 tests):
    - Internal dedup (same message_id silently dropped)
    - Alert generation via REST / Kafka / Files
    - LargeNotional algorithm
    - Velocity algorithm
    - SuspiciousCounterparty algorithm
"""

from __future__ import annotations

import asyncio
import json
import socket
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import aiohttp
import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.algos.main import AlgosModule
from de_platform.modules.data_api.main import DataApiModule
from de_platform.modules.file_processor.main import FileProcessorModule
from de_platform.modules.kafka_starter.main import KafkaStarterModule
from de_platform.modules.normalizer.main import NormalizerModule
from de_platform.modules.persistence.main import PersistenceModule
from de_platform.modules.rest_starter.main import RestStarterModule
from de_platform.pipeline.topics import TRADE_NORMALIZATION
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue


# ── Event factories ───────────────────────────────────────────────────────────


def _order(
    event_id: str | None = None,
    currency: str = "USD",
    quantity: float = 100.0,
    price: float = 200.0,
) -> dict[str, Any]:
    return {
        "event_type": "order",
        "id": event_id or f"o-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "order_type": "limit",
        "currency": currency,
    }


def _execution(event_id: str | None = None, currency: str = "EUR") -> dict[str, Any]:
    return {
        "event_type": "execution",
        "id": event_id or f"e-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "filled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "order_id": "o-abc",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": 50.0,
        "price": 210.0,
        "execution_venue": "NYSE",
        "currency": currency,
    }


def _transaction(
    event_id: str | None = None,
    currency: str = "GBP",
    amount: float = 500.0,
    counterparty_id: str = "cp1",
) -> dict[str, Any]:
    return {
        "event_type": "transaction",
        "id": event_id or f"tx-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": "acc1",
        "counterparty_id": counterparty_id,
        "amount": amount,
        "currency": currency,
        "transaction_type": "wire",
    }


def _invalid_event(event_type: str) -> dict[str, Any]:
    """A valid event with only 'status' removed → exactly one ValidationError.

    Removing just one field ensures exactly 1 error message per event lands in
    the normalization_errors topic, making the expected count deterministic.
    """
    event = _EVENT_FACTORY[event_type]()
    event.pop("status")  # status is required for all event types
    return event


_EVENT_FACTORY: dict[str, Any] = {
    "order": _order,
    "execution": _execution,
    "transaction": _transaction,
}
_EVENT_TABLE: dict[str, str] = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}
_REST_ENDPOINT: dict[str, str] = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}
_CLIENT_TOPIC: dict[str, str] = {
    "order": "client_orders",
    "execution": "client_executions",
    "transaction": "client_transactions",
}


# ── Utilities ─────────────────────────────────────────────────────────────────


def _free_port() -> int:
    """Bind to port 0, read the OS-assigned port, then release it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def poll_until(predicate: Any, timeout: float = 20.0, interval: float = 0.05) -> None:
    """Repeatedly call predicate() until it returns truthy or timeout expires."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise TimeoutError("poll_until: condition not met within timeout")
        await asyncio.sleep(interval)


async def _wait_for_http(url: str, timeout: float = 10.0) -> None:
    """Poll an HTTP endpoint until it responds with a non-5xx status."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status < 500:
                        return
        except (aiohttp.ClientConnectorError, aiohttp.ServerConnectionError, OSError):
            pass
        if loop.time() > deadline:
            raise TimeoutError(f"HTTP service at {url} not ready within {timeout}s")
        await asyncio.sleep(0.05)


# ── Pipeline context ──────────────────────────────────────────────────────────


@dataclass
class PipelineCtx:
    mq: MemoryQueue
    db: MemoryDatabase        # shared store — events (persistence) + alerts (algos/data_api)
    fs: MemoryFileSystem      # output files written by Persistence
    input_fs: MemoryFileSystem  # input files consumed by FileProcessor
    cache: MemoryCache
    rest_port: int
    api_port: int


@asynccontextmanager
async def _make_pipeline(algos_config: dict[str, Any] | None = None):
    """Start all 6 pipeline services as asyncio tasks, yield PipelineCtx, then shut down."""
    mq = MemoryQueue()
    db = MemoryDatabase()
    db.connect()
    fs = MemoryFileSystem()
    input_fs = MemoryFileSystem()
    cache = MemoryCache()

    # Pre-populate cache so the Normalizer can do currency conversion without
    # making a two-column WHERE query that MemoryDatabase cannot parse.
    cache.set("currency_rate:EUR_USD", 1.10)
    cache.set("currency_rate:GBP_USD", 1.25)

    rest_port = _free_port()
    api_port = _free_port()

    lifecycles = [LifecycleManager() for _ in range(6)]
    lc_rest, lc_kafka, lc_norm, lc_pers, lc_algos, lc_api = lifecycles

    logger = LoggerFactory(default_impl="memory")

    modules: list[Any] = [
        RestStarterModule(
            config=ModuleConfig({"port": rest_port}),
            logger=logger,
            mq=mq,
            lifecycle=lc_rest,
        ),
        KafkaStarterModule(
            config=ModuleConfig({}),
            logger=logger,
            mq=mq,
            lifecycle=lc_kafka,
        ),
        NormalizerModule(
            config=ModuleConfig({}),
            logger=logger,
            mq=mq,
            cache=cache,
            db=db,
            lifecycle=lc_norm,
        ),
        PersistenceModule(
            # flush-threshold=1: each row is flushed to DB immediately for test
            # determinism (no need to wait for a batch to fill).
            config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
            logger=logger,
            mq=mq,
            db=db,
            fs=fs,
            lifecycle=lc_pers,
        ),
        AlgosModule(
            config=ModuleConfig(algos_config or {}),
            logger=logger,
            mq=mq,
            db=db,
            cache=cache,
            lifecycle=lc_algos,
        ),
        DataApiModule(
            config=ModuleConfig({"port": api_port}),
            logger=logger,
            mq=mq,
            db=db,
            lifecycle=lc_api,
        ),
    ]

    # Kick off all services as concurrent asyncio tasks.
    tasks = [asyncio.create_task(m.run()) for m in modules]

    # Wait for the two HTTP servers to accept connections before yielding.
    await _wait_for_http(f"http://127.0.0.1:{rest_port}/api/v1/health")
    await _wait_for_http(f"http://127.0.0.1:{api_port}/api/v1/alerts")

    ctx = PipelineCtx(
        mq=mq, db=db, fs=fs, input_fs=input_fs, cache=cache,
        rest_port=rest_port, api_port=api_port,
    )
    try:
        yield ctx
    finally:
        # Signal each service to stop, then wait for all tasks to finish.
        for lc in lifecycles:
            await lc.shutdown()
        done, pending = await asyncio.wait(tasks, timeout=5.0)
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


@pytest.fixture
async def pipeline():
    """Fresh pipeline with default configuration."""
    async with _make_pipeline() as ctx:
        yield ctx


@pytest.fixture
async def pipeline_suspicious_cp():
    """Pipeline where AlgosModule is configured to flag counterparty 'bad-cp-1'."""
    async with _make_pipeline(algos_config={"suspicious-counterparty-ids": "bad-cp-1"}) as ctx:
        yield ctx


# ── Ingestion helpers ─────────────────────────────────────────────────────────


async def _ingest_rest(
    ctx: PipelineCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    """POST events to the RestStarterModule's HTTP endpoint."""
    url = f"http://127.0.0.1:{ctx.rest_port}/api/v1/{_REST_ENDPOINT[event_type]}"
    async with aiohttp.ClientSession() as session:
        resp = await session.post(url, json={"events": events})
        assert resp.status == 200


def _ingest_kafka(
    ctx: PipelineCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    """Publish raw events to the KafkaStarterModule's inbound topics."""
    topic = _CLIENT_TOPIC[event_type]
    for event in events:
        ctx.mq.publish(topic, event)


def _ingest_files(
    ctx: PipelineCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    """Write events to input_fs and run FileProcessorModule inline."""
    path = f"input/{event_type}/{uuid.uuid4().hex}.jsonl"
    ctx.input_fs.write(
        path, ("\n".join(json.dumps(e) for e in events)).encode()
    )
    fp = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": event_type}),
        logger=LoggerFactory(default_impl="memory"),
        fs=ctx.input_fs,
        mq=ctx.mq,
    )
    fp.run()


async def _ingest(
    ctx: PipelineCtx, method: str, event_type: str, events: list[dict[str, Any]]
) -> None:
    if method == "rest":
        await _ingest_rest(ctx, event_type, events)
    elif method == "kafka":
        _ingest_kafka(ctx, event_type, events)
    elif method == "files":
        _ingest_files(ctx, event_type, events)
    else:
        raise ValueError(f"Unknown ingestion method: {method!r}")


# ── Parametrize matrix ────────────────────────────────────────────────────────

_MATRIX = pytest.mark.parametrize(
    "method,event_type",
    [
        ("rest", "order"),
        ("rest", "execution"),
        ("rest", "transaction"),
        ("kafka", "order"),
        ("kafka", "execution"),
        ("kafka", "transaction"),
        ("files", "order"),
        ("files", "execution"),
        ("files", "transaction"),
    ],
)


# ── Scenario 1: 100 valid events ──────────────────────────────────────────────


@_MATRIX
async def test_100_valid_events_stored_and_accessible(pipeline, method, event_type):
    """100 valid events land in the correct DB table and are queryable via the API."""
    events = [_EVENT_FACTORY[event_type]() for _ in range(100)]
    await _ingest(pipeline, method, event_type, events)

    table = _EVENT_TABLE[event_type]
    await poll_until(lambda: len(pipeline.db.fetch_all(f"SELECT * FROM {table}")) >= 100)
    assert len(pipeline.db.fetch_all(f"SELECT * FROM {table}")) == 100

    # Verify accessibility via Data API HTTP endpoint
    url = (
        f"http://127.0.0.1:{pipeline.api_port}"
        f"/api/v1/events/{table}?tenant_id=acme&limit=200"
    )
    async with aiohttp.ClientSession() as session:
        resp = await session.get(url)
        assert resp.status == 200
        rows = await resp.json()
    assert len(rows) == 100


# ── Scenario 2: 100 invalid events ───────────────────────────────────────────


@_MATRIX
async def test_100_invalid_events_go_to_errors_table(pipeline, method, event_type):
    """100 invalid events are rejected at the starter and land in normalization_errors."""
    invalid_events = [_invalid_event(event_type) for _ in range(100)]
    await _ingest(pipeline, method, event_type, invalid_events)

    await poll_until(
        lambda: len(pipeline.db.fetch_all("SELECT * FROM normalization_errors")) >= 100
    )
    assert len(pipeline.db.fetch_all("SELECT * FROM normalization_errors")) == 100
    # Nothing should reach the valid table
    table = _EVENT_TABLE[event_type]
    assert pipeline.db.fetch_all(f"SELECT * FROM {table}") == []


# ── Scenario 3: same event 100 times ─────────────────────────────────────────


@_MATRIX
async def test_same_event_100_times_yields_1_valid_99_duplicates(
    pipeline, method, event_type
):
    """Sending the same business event 100× results in 1 stored + 99 duplicates.

    Each starter (REST / Kafka / Files) assigns a fresh message_id on every
    ingest, so the Normalizer sees the same primary_key with 100 distinct
    message_ids: first → valid, subsequent → external duplicate.
    """
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    # All 100 copies share the same (tenant_id, event_type, id, transact_time)
    # → same primary_key computed by the Normalizer.
    same_events = [_EVENT_FACTORY[event_type](event_id=fixed_id) for _ in range(100)]
    await _ingest(pipeline, method, event_type, same_events)

    table = _EVENT_TABLE[event_type]
    await poll_until(
        lambda: (
            len(pipeline.db.fetch_all(f"SELECT * FROM {table}")) >= 1
            and len(pipeline.db.fetch_all("SELECT * FROM duplicates")) >= 99
        )
    )
    assert len(pipeline.db.fetch_all(f"SELECT * FROM {table}")) == 1
    assert len(pipeline.db.fetch_all("SELECT * FROM duplicates")) == 99


# ── General: internal deduplication ──────────────────────────────────────────


async def test_internal_dedup_same_message_id_is_silently_discarded(pipeline):
    """The same message_id arriving at the Normalizer twice is silently dropped.

    This simulates Kafka at-least-once delivery replaying a message.  We bypass
    the starters (which always assign new message_ids) and publish directly to
    the normalization topic with a fixed message_id.
    """
    event = {
        **_order(),
        "message_id": "dedup-test-fixed-msg-id",
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }
    pipeline.mq.publish(TRADE_NORMALIZATION, event)
    await poll_until(lambda: len(pipeline.db.fetch_all("SELECT * FROM orders")) >= 1)
    assert len(pipeline.db.fetch_all("SELECT * FROM orders")) == 1

    # Publish the exact same message_id again — Normalizer must drop it.
    pipeline.mq.publish(TRADE_NORMALIZATION, dict(event))
    await asyncio.sleep(0.3)

    assert len(pipeline.db.fetch_all("SELECT * FROM orders")) == 1, (
        "Internal duplicate (same message_id) must be silently discarded"
    )


# ── General: alert generation per ingestion method ────────────────────────────


async def test_alert_generation_via_rest(pipeline):
    """Large-notional order submitted via REST triggers a large_notional alert."""
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    await _ingest_rest(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "large_notional"
            for r in pipeline.db.fetch_all("SELECT * FROM alerts")
        )
    )


async def test_alert_generation_via_kafka(pipeline):
    """Large-notional order submitted via Kafka triggers a large_notional alert."""
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    _ingest_kafka(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "large_notional"
            for r in pipeline.db.fetch_all("SELECT * FROM alerts")
        )
    )


async def test_alert_generation_via_files(pipeline):
    """Large-notional order submitted via files triggers a large_notional alert."""
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    _ingest_files(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "large_notional"
            for r in pipeline.db.fetch_all("SELECT * FROM alerts")
        )
    )


# ── General: per-algorithm alert tests ───────────────────────────────────────


async def test_large_notional_algorithm(pipeline):
    """LargeNotionalAlgo fires for trades where notional_usd > $1 M."""
    # USD order: notional_usd = 100 × 15 000 × 1.0 = 1 500 000 > 1 000 000
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    await _ingest_rest(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "large_notional" and r.get("severity") == "high"
            for r in pipeline.db.fetch_all("SELECT * FROM alerts")
        )
    )


async def test_velocity_algorithm(pipeline):
    """VelocityAlgo fires after a tenant submits more than 100 events in the window."""
    # Send 101 unique orders — all valid, all reach the Algos service.
    # After the 101st, the per-tenant velocity counter exceeds max_events=100.
    orders = [_order() for _ in range(101)]
    await _ingest_rest(pipeline, "order", orders)

    await poll_until(
        lambda: any(
            r.get("algorithm") == "velocity"
            for r in pipeline.db.fetch_all("SELECT * FROM alerts")
        )
    )


async def test_suspicious_counterparty_algorithm(pipeline_suspicious_cp):
    """SuspiciousCounterpartyAlgo fires for counterparty IDs in the blocklist.

    Uses the pipeline_suspicious_cp fixture which configures AlgosModule with
    suspicious-counterparty-ids=bad-cp-1.
    """
    suspicious_tx = _transaction(counterparty_id="bad-cp-1")
    await _ingest_rest(pipeline_suspicious_cp, "transaction", [suspicious_tx])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "suspicious_counterparty"
            for r in pipeline_suspicious_cp.db.fetch_all("SELECT * FROM alerts")
        )
    )
