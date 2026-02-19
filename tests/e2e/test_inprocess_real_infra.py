"""In-process E2E tests with real infrastructure.

All 6 pipeline modules run as concurrent asyncio tasks sharing real
Postgres, ClickHouse, Redis, Kafka, and MinIO connections. Each module
gets its own KafkaQueue instance with a unique consumer group suffix.

Same 34 test cases as tests/e2e/test_inprocess_e2e.py but with real infra.

Requires: ``pytest -m real_infra`` or ``make test-real-infra``.
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
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory

pytestmark = [pytest.mark.real_infra]


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
    event = _EVENT_FACTORY[event_type]()
    event.pop("status")
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
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def poll_until(predicate: Any, timeout: float = 30.0, interval: float = 0.1) -> None:
    """Repeatedly call predicate() until truthy or timeout."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise TimeoutError("poll_until: condition not met within timeout")
        await asyncio.sleep(interval)


async def _wait_for_http(url: str, timeout: float = 15.0) -> None:
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
        await asyncio.sleep(0.1)


# ── Pipeline context ──────────────────────────────────────────────────────────


@dataclass
class RealPipelineCtx:
    # For assertions
    clickhouse_db: ClickHouseDatabase
    alerts_db: PostgresDatabase
    # For ingestion
    kafka_producer: KafkaQueue
    minio_fs: MinioFileSystem
    # HTTP ports
    rest_port: int
    api_port: int


@asynccontextmanager
async def _make_real_pipeline(infra, algos_config: dict[str, Any] | None = None):
    """Start all 6 pipeline modules as asyncio tasks with real infrastructure."""
    from de_platform.services.cache.redis_cache import RedisCache
    from de_platform.services.database.clickhouse_database import ClickHouseDatabase
    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.filesystem.minio_filesystem import MinioFileSystem
    from de_platform.services.message_queue.kafka_queue import KafkaQueue
    from de_platform.services.secrets.env_secrets import EnvSecrets

    rest_port = _free_port()
    api_port = _free_port()
    group_base = uuid.uuid4().hex[:8]

    def _make_secrets(suffix: str) -> EnvSecrets:
        return EnvSecrets(overrides=infra.to_env_overrides(
            group_id=f"inproc-{group_base}-{suffix}"
        ))

    logger = LoggerFactory(default_impl="memory")
    lifecycles = [LifecycleManager() for _ in range(6)]
    lc_rest, lc_kafka, lc_norm, lc_pers, lc_algos, lc_api = lifecycles

    # Create per-module KafkaQueue instances with unique group IDs
    mq_rest = KafkaQueue(_make_secrets("rest"))
    mq_rest.connect()
    mq_kafka = KafkaQueue(_make_secrets("kafka"))
    mq_kafka.connect()
    mq_norm = KafkaQueue(_make_secrets("norm"))
    mq_norm.connect()
    mq_pers = KafkaQueue(_make_secrets("pers"))
    mq_pers.connect()
    mq_algos = KafkaQueue(_make_secrets("algos"))
    mq_algos.connect()
    mq_api = KafkaQueue(_make_secrets("api"))
    mq_api.connect()

    # Shared service connections
    warehouse_secrets = _make_secrets("warehouse")
    warehouse_db = PostgresDatabase(secrets=warehouse_secrets, prefix="DB_WAREHOUSE")
    await warehouse_db.connect_async()

    alerts_secrets = _make_secrets("alerts")
    alerts_db = PostgresDatabase(secrets=alerts_secrets, prefix="DB_ALERTS")
    await alerts_db.connect_async()

    ch_db = ClickHouseDatabase(secrets=_make_secrets("ch"))
    ch_db.connect()

    cache = RedisCache(secrets=_make_secrets("cache"))
    cache.connect()

    fs = MinioFileSystem(secrets=_make_secrets("fs"))

    # Build DataApi's DatabaseFactory
    api_db_factory = DatabaseFactory({})
    api_db_factory.register_instance("events", ch_db)
    api_db_factory.register_instance("alerts", alerts_db)

    modules: list[Any] = [
        RestStarterModule(
            config=ModuleConfig({"port": rest_port}),
            logger=logger,
            mq=mq_rest,
            lifecycle=lc_rest,
        ),
        KafkaStarterModule(
            config=ModuleConfig({}),
            logger=logger,
            mq=mq_kafka,
            lifecycle=lc_kafka,
        ),
        NormalizerModule(
            config=ModuleConfig({}),
            logger=logger,
            mq=mq_norm,
            cache=cache,
            db=warehouse_db,
            lifecycle=lc_norm,
        ),
        PersistenceModule(
            config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
            logger=logger,
            mq=mq_pers,
            db=ch_db,
            fs=fs,
            lifecycle=lc_pers,
        ),
        AlgosModule(
            config=ModuleConfig(algos_config or {}),
            logger=logger,
            mq=mq_algos,
            db=alerts_db,
            cache=cache,
            lifecycle=lc_algos,
        ),
        DataApiModule(
            config=ModuleConfig({"port": api_port}),
            logger=logger,
            mq=mq_api,
            db_factory=api_db_factory,
            lifecycle=lc_api,
        ),
    ]

    tasks = [asyncio.create_task(m.run()) for m in modules]

    await _wait_for_http(f"http://127.0.0.1:{rest_port}/api/v1/health")
    await _wait_for_http(f"http://127.0.0.1:{api_port}/api/v1/alerts")

    # Producer for Kafka ingestion
    producer_mq = KafkaQueue(_make_secrets("producer"))
    producer_mq.connect()

    ctx = RealPipelineCtx(
        clickhouse_db=ch_db,
        alerts_db=alerts_db,
        kafka_producer=producer_mq,
        minio_fs=fs,
        rest_port=rest_port,
        api_port=api_port,
    )
    try:
        yield ctx
    finally:
        for lc in lifecycles:
            await lc.shutdown()
        done, pending = await asyncio.wait(tasks, timeout=10.0)
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        # Cleanup connections
        for mq in [mq_rest, mq_kafka, mq_norm, mq_pers, mq_algos, mq_api, producer_mq]:
            mq.disconnect()
        await warehouse_db.disconnect_async()
        await alerts_db.disconnect_async()
        ch_db.disconnect()
        cache.disconnect()


@pytest.fixture
async def pipeline(infra):
    async with _make_real_pipeline(infra) as ctx:
        yield ctx


@pytest.fixture
async def pipeline_suspicious_cp(infra):
    async with _make_real_pipeline(
        infra, algos_config={"suspicious-counterparty-ids": "bad-cp-1"}
    ) as ctx:
        yield ctx


# ── Ingestion helpers ─────────────────────────────────────────────────────────


async def _ingest_rest(
    ctx: RealPipelineCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    url = f"http://127.0.0.1:{ctx.rest_port}/api/v1/{_REST_ENDPOINT[event_type]}"
    async with aiohttp.ClientSession() as session:
        resp = await session.post(url, json={"events": events})
        assert resp.status == 200


def _ingest_kafka(
    ctx: RealPipelineCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    topic = _CLIENT_TOPIC[event_type]
    for event in events:
        ctx.kafka_producer.publish(topic, event)


def _ingest_files(
    ctx: RealPipelineCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    from de_platform.services.message_queue.kafka_queue import KafkaQueue
    from de_platform.services.secrets.env_secrets import EnvSecrets

    path = f"input/{event_type}/{uuid.uuid4().hex}.jsonl"
    ctx.minio_fs.write(
        path, ("\n".join(json.dumps(e) for e in events)).encode()
    )
    fp_secrets = EnvSecrets(overrides={
        "MQ_KAFKA_BOOTSTRAP_SERVERS": ctx.kafka_producer._bootstrap,
        "MQ_KAFKA_GROUP_ID": f"fp-{uuid.uuid4().hex[:8]}",
    })
    fp_mq = KafkaQueue(fp_secrets)
    fp_mq.connect()
    fp = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": event_type}),
        logger=LoggerFactory(default_impl="memory"),
        fs=ctx.minio_fs,
        mq=fp_mq,
    )
    fp.run()
    fp_mq.disconnect()


async def _ingest(
    ctx: RealPipelineCtx, method: str, event_type: str, events: list[dict[str, Any]]
) -> None:
    if method == "rest":
        await _ingest_rest(ctx, event_type, events)
    elif method == "kafka":
        _ingest_kafka(ctx, event_type, events)
    elif method == "files":
        _ingest_files(ctx, event_type, events)


# ── Assertion helpers ─────────────────────────────────────────────────────────


def _ch_row_count(ctx: RealPipelineCtx, table: str) -> int:
    return len(ctx.clickhouse_db.fetch_all(f"SELECT * FROM {table}"))


def _pg_alerts(ctx: RealPipelineCtx) -> list[dict]:
    return ctx.alerts_db.fetch_all("SELECT * FROM alerts")


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
    """100 valid events land in ClickHouse and are queryable via API."""
    events = [_EVENT_FACTORY[event_type]() for _ in range(100)]
    await _ingest(pipeline, method, event_type, events)

    table = _EVENT_TABLE[event_type]
    await poll_until(lambda: _ch_row_count(pipeline, table) >= 100, timeout=60.0)
    assert _ch_row_count(pipeline, table) == 100

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
    """100 invalid events -> normalization_errors, not the valid table."""
    invalid_events = [_invalid_event(event_type) for _ in range(100)]
    await _ingest(pipeline, method, event_type, invalid_events)

    await poll_until(
        lambda: _ch_row_count(pipeline, "normalization_errors") >= 100,
        timeout=60.0,
    )
    assert _ch_row_count(pipeline, "normalization_errors") == 100
    table = _EVENT_TABLE[event_type]
    assert _ch_row_count(pipeline, table) == 0


# ── Scenario 3: same event 100 times ─────────────────────────────────────────


@_MATRIX
async def test_same_event_100_times_yields_1_valid_99_duplicates(
    pipeline, method, event_type
):
    """Same event 100x -> 1 valid + 99 duplicates."""
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    same_events = [_EVENT_FACTORY[event_type](event_id=fixed_id) for _ in range(100)]
    await _ingest(pipeline, method, event_type, same_events)

    table = _EVENT_TABLE[event_type]
    await poll_until(
        lambda: (
            _ch_row_count(pipeline, table) >= 1
            and _ch_row_count(pipeline, "duplicates") >= 99
        ),
        timeout=60.0,
    )
    assert _ch_row_count(pipeline, table) == 1
    assert _ch_row_count(pipeline, "duplicates") == 99


# ── General: internal deduplication ──────────────────────────────────────────


async def test_internal_dedup_same_message_id_is_silently_discarded(pipeline):
    """Same message_id arriving twice -> second copy dropped."""
    event = {
        **_order(),
        "message_id": "dedup-test-fixed-msg-id",
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }
    pipeline.kafka_producer.publish(TRADE_NORMALIZATION, event)
    await poll_until(lambda: _ch_row_count(pipeline, "orders") >= 1, timeout=60.0)
    assert _ch_row_count(pipeline, "orders") == 1

    pipeline.kafka_producer.publish(TRADE_NORMALIZATION, dict(event))
    await asyncio.sleep(3.0)
    assert _ch_row_count(pipeline, "orders") == 1


# ── General: alert generation per ingestion method ────────────────────────────


async def test_alert_generation_via_rest(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    await _ingest_rest(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(r.get("algorithm") == "large_notional" for r in _pg_alerts(pipeline)),
        timeout=60.0,
    )


async def test_alert_generation_via_kafka(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    _ingest_kafka(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(r.get("algorithm") == "large_notional" for r in _pg_alerts(pipeline)),
        timeout=60.0,
    )


async def test_alert_generation_via_files(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    _ingest_files(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(r.get("algorithm") == "large_notional" for r in _pg_alerts(pipeline)),
        timeout=60.0,
    )


# ── General: per-algorithm alert tests ───────────────────────────────────────


async def test_large_notional_algorithm(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    await _ingest_rest(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "large_notional" and r.get("severity") == "high"
            for r in _pg_alerts(pipeline)
        ),
        timeout=60.0,
    )


async def test_velocity_algorithm(pipeline):
    orders = [_order() for _ in range(101)]
    await _ingest_rest(pipeline, "order", orders)

    await poll_until(
        lambda: any(r.get("algorithm") == "velocity" for r in _pg_alerts(pipeline)),
        timeout=60.0,
    )


async def test_suspicious_counterparty_algorithm(pipeline_suspicious_cp):
    suspicious_tx = _transaction(counterparty_id="bad-cp-1")
    await _ingest_rest(pipeline_suspicious_cp, "transaction", [suspicious_tx])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "suspicious_counterparty"
            for r in _pg_alerts(pipeline_suspicious_cp)
        ),
        timeout=60.0,
    )
