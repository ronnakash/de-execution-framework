"""Matrix E2E tests with real infrastructure.

Same 27 matrix + 7 general test cases as tests/integration/test_matrix_e2e.py,
but using real Postgres, ClickHouse, Redis, Kafka, and MinIO instead of
in-memory stubs.  Modules are instantiated directly and stepped manually.

Requires: ``pytest -m real_infra`` or ``make test-real-infra``.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.algos.main import AlgosModule
from de_platform.modules.file_processor.main import FileProcessorModule
from de_platform.modules.kafka_starter.main import KafkaStarterModule
from de_platform.modules.normalizer.main import NormalizerModule
from de_platform.modules.persistence.buffer import BufferKey
from de_platform.modules.persistence.main import PersistenceModule
from de_platform.modules.rest_starter.main import dto_to_message_from_raw
from de_platform.pipeline.algorithms import (
    SuspiciousCounterpartyAlgo,
    VelocityAlgo,
)
from de_platform.pipeline.serialization import error_to_dict
from de_platform.pipeline.topics import (
    ALERTS,
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TRADE_NORMALIZATION,
    TRADES_ALGOS,
    TRANSACTIONS_ALGOS,
    TRANSACTIONS_PERSISTENCE,
    TX_NORMALIZATION,
)
from de_platform.pipeline.validation import validate_events
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue

pytestmark = [pytest.mark.real_infra]

# ── Parametrization ───────────────────────────────────────────────────────────

_MATRIX_PARAMS = [
    pytest.param(method, etype, id=f"{method}-{etype}")
    for method in ("rest", "kafka", "files")
    for etype in ("order", "execution", "transaction")
]

# ── Topic / table routing maps ────────────────────────────────────────────────

_NORM_TOPIC = {
    "order": TRADE_NORMALIZATION,
    "execution": TRADE_NORMALIZATION,
    "transaction": TX_NORMALIZATION,
}
_CATEGORY = {
    "order": "trade",
    "execution": "trade",
    "transaction": "transaction",
}
_PERSIST_TOPIC_TABLE = {
    "order": (ORDERS_PERSISTENCE, "orders"),
    "execution": (EXECUTIONS_PERSISTENCE, "executions"),
    "transaction": (TRANSACTIONS_PERSISTENCE, "transactions"),
}
_ALGOS_TOPIC = {
    "order": TRADES_ALGOS,
    "execution": TRADES_ALGOS,
    "transaction": TRANSACTIONS_ALGOS,
}
_REST_PATH = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}

# ── Event factories ───────────────────────────────────────────────────────────


def _make_order(
    id_: str | None = None,
    tenant: str = "t1",
    quantity: float = 10.0,
    price: float = 100.0,
    currency: str = "USD",
) -> dict:
    return {
        "id": id_ or f"o-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant,
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "order_type": "limit",
        "currency": currency,
    }


def _make_execution(
    id_: str | None = None,
    tenant: str = "t1",
    quantity: float = 50.0,
    price: float = 200.0,
    currency: str = "USD",
) -> dict:
    return {
        "id": id_ or f"e-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant,
        "status": "filled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "order_id": "o-abc",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "execution_venue": "NYSE",
        "currency": currency,
    }


def _make_transaction(
    id_: str | None = None,
    tenant: str = "t1",
    amount: float = 500.0,
    currency: str = "USD",
    counterparty_id: str = "cp-safe",
) -> dict:
    return {
        "id": id_ or f"tx-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant,
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": "acc-1",
        "counterparty_id": counterparty_id,
        "amount": amount,
        "currency": currency,
        "transaction_type": "wire",
    }


_FACTORIES: dict[str, Any] = {
    "order": _make_order,
    "execution": _make_execution,
    "transaction": _make_transaction,
}


def _invalid_event(event_type: str) -> dict:
    """Return an event with an empty id — fails validation."""
    evt = _FACTORIES[event_type]()
    evt["id"] = ""
    return evt


# ── Polling helpers ──────────────────────────────────────────────────────────


def _wait_for_rows(
    db: DatabaseInterface, query: str, expected: int, timeout: float = 15.0
) -> list[dict[str, Any]]:
    """Poll a DB query until the row count reaches `expected` or timeout."""
    deadline = time.monotonic() + timeout
    rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        rows = db.fetch_all(query)
        if len(rows) >= expected:
            return rows
        time.sleep(0.3)
    return rows


# ── Module fixtures (real infra) ─────────────────────────────────────────────
# We use MemoryQueue for inter-module communication within the test process,
# while the actual data stores (Postgres, ClickHouse, Redis, MinIO) are real.


@pytest.fixture
def mq() -> MemoryQueue:
    return MemoryQueue()


@pytest.fixture
async def normalizer(mq, redis_cache, warehouse_db):
    module = NormalizerModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        cache=redis_cache,
        db=warehouse_db,
        lifecycle=LifecycleManager(),
    )
    await module.initialize()
    return module


@pytest.fixture
async def persistence(mq, clickhouse_db, minio_fs):
    module = PersistenceModule(
        config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=clickhouse_db,
        fs=minio_fs,
        lifecycle=LifecycleManager(),
    )
    await module.initialize()
    return module


@pytest.fixture
async def algos(mq, alerts_db, redis_cache):
    module = AlgosModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=alerts_db,
        cache=redis_cache,
        lifecycle=LifecycleManager(),
    )
    await module.initialize()
    return module


# ── Pipeline step helpers ─────────────────────────────────────────────────────


def _run_normalizer(normalizer: NormalizerModule, event_type: str, count: int) -> None:
    topic = _NORM_TOPIC[event_type]
    category = _CATEGORY[event_type]
    for _ in range(count):
        normalizer._poll_and_process(topic, category)


def _drain_all_into_persistence(
    persistence: PersistenceModule, mq: MemoryQueue
) -> None:
    assert persistence.buffer is not None
    all_topic_tables = [
        (ORDERS_PERSISTENCE, "orders"),
        (EXECUTIONS_PERSISTENCE, "executions"),
        (TRANSACTIONS_PERSISTENCE, "transactions"),
        (DUPLICATES, "duplicates"),
        (NORMALIZATION_ERRORS, "normalization_errors"),
    ]
    for topic, table in all_topic_tables:
        while True:
            msg = mq.consume_one(topic)
            if msg is None:
                break
            key = BufferKey(tenant_id=msg.get("tenant_id", "unknown"), table=table)
            persistence.buffer.append(key, msg)
    persistence._flush_all()


def _drain_algos_topics(
    algos: AlgosModule, mq: MemoryQueue, event_type: str
) -> None:
    topic = _ALGOS_TOPIC[event_type]
    while True:
        msg = mq.consume_one(topic)
        if msg is None:
            break
        algos._evaluate(msg)


# ── Ingestion helpers ─────────────────────────────────────────────────────────


def _build_rest_app(mq: MemoryQueue) -> web.Application:
    async def _handle(request: web.Request, event_type: str) -> web.Response:
        body = await request.json()
        events = body.get("events", [])
        valid, errors = validate_events(event_type, events)
        topic = _NORM_TOPIC[event_type]
        for raw in valid:
            mq.publish(topic, dto_to_message_from_raw(raw, event_type))
        for err in errors:
            raw_event = events[err.event_index] if err.event_index < len(events) else {}
            mq.publish(NORMALIZATION_ERRORS, error_to_dict(raw_event, event_type, [err]))
        rejected = {e.event_index for e in errors}
        return web.json_response({"accepted": len(valid), "rejected": len(rejected)})

    app = web.Application()
    for et, path in _REST_PATH.items():
        app.router.add_post(f"/api/v1/{path}", lambda r, et=et: _handle(r, et))
    return app


async def _ingest_rest(mq: MemoryQueue, event_type: str, events: list[dict]) -> None:
    app = _build_rest_app(mq)
    async with TestClient(TestServer(app)) as client:
        await client.post(f"/api/v1/{_REST_PATH[event_type]}", json={"events": events})


async def _ingest_kafka(mq: MemoryQueue, event_type: str, events: list[dict]) -> None:
    starter = KafkaStarterModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        lifecycle=LifecycleManager(),
    )
    await starter.initialize()
    topic = _NORM_TOPIC[event_type]
    for raw in events:
        starter._process_message(event_type, topic, raw)


def _ingest_files(minio_fs, mq: MemoryQueue, event_type: str, events: list[dict]) -> None:
    path = f"ingest/{event_type}/{uuid.uuid4().hex[:8]}.jsonl"
    content = "\n".join(json.dumps(e) for e in events).encode()
    minio_fs.write(path, content)
    module = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": event_type}),
        logger=LoggerFactory(default_impl="memory"),
        fs=minio_fs,
        mq=mq,
    )
    module.initialize()
    module.validate()
    module.execute()


async def _ingest(method, event_type, events, mq, minio_fs) -> None:
    if method == "rest":
        await _ingest_rest(mq, event_type, events)
    elif method == "kafka":
        await _ingest_kafka(mq, event_type, events)
    elif method == "files":
        _ingest_files(minio_fs, mq, event_type, events)


# ══════════════════════════════════════════════════════════════════════════════
# Matrix tests: Case 1 — 100 valid events
# ══════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("method,event_type", _MATRIX_PARAMS)
async def test_100_valid_events_reach_clickhouse_and_api(
    method: str,
    event_type: str,
    mq: MemoryQueue,
    minio_fs,
    clickhouse_db,
    alerts_db,
    normalizer: NormalizerModule,
    persistence: PersistenceModule,
) -> None:
    """100 valid events -> 100 enriched rows in ClickHouse, 0 errors, accessible via API."""
    events = [_FACTORIES[event_type]() for _ in range(100)]

    await _ingest(method, event_type, events, mq, minio_fs)
    _run_normalizer(normalizer, event_type, 100)
    _drain_all_into_persistence(persistence, mq)

    _, table = _PERSIST_TOPIC_TABLE[event_type]
    rows = _wait_for_rows(clickhouse_db, f"SELECT * FROM {table}", 100)
    assert len(rows) == 100, f"Expected 100 rows in {table}, got {len(rows)}"

    for row in rows:
        assert "primary_key" in row
        assert "normalized_at" in row
        if event_type in ("order", "execution"):
            assert "notional_usd" in row
        else:
            assert "amount_usd" in row

    error_rows = clickhouse_db.fetch_all("SELECT * FROM normalization_errors")
    assert error_rows == []

    # Verify via Data API
    from de_platform.modules.data_api.main import DataApiModule

    db_factory = DatabaseFactory({})
    db_factory.register_instance("events", clickhouse_db)
    db_factory.register_instance("alerts", alerts_db)
    api = DataApiModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db_factory=db_factory,
        lifecycle=LifecycleManager(),
    )
    await api.initialize()
    app = api._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            f"/api/v1/events/{_REST_PATH[event_type]}?tenant_id=t1&limit=100"
        )
        assert resp.status == 200
        body = await resp.json()
        assert len(body) == 100


# ══════════════════════════════════════════════════════════════════════════════
# Matrix tests: Case 2 — 100 invalid events
# ══════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("method,event_type", _MATRIX_PARAMS)
async def test_100_invalid_events_go_to_errors_table(
    method: str,
    event_type: str,
    mq: MemoryQueue,
    minio_fs,
    clickhouse_db,
    normalizer: NormalizerModule,
    persistence: PersistenceModule,
) -> None:
    """100 invalid events -> 100 rows in normalization_errors, 0 in the valid table."""
    events = [_invalid_event(event_type) for _ in range(100)]

    await _ingest(method, event_type, events, mq, minio_fs)
    _run_normalizer(normalizer, event_type, 10)
    _drain_all_into_persistence(persistence, mq)

    _, table = _PERSIST_TOPIC_TABLE[event_type]
    valid_rows = clickhouse_db.fetch_all(f"SELECT * FROM {table}")
    assert valid_rows == []

    error_rows = _wait_for_rows(
        clickhouse_db, "SELECT * FROM normalization_errors", 100
    )
    assert len(error_rows) == 100


# ══════════════════════════════════════════════════════════════════════════════
# Matrix tests: Case 3 — same event 100 times (external deduplication)
# ══════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("method,event_type", _MATRIX_PARAMS)
async def test_same_event_100_times_yields_1_valid_and_99_duplicates(
    method: str,
    event_type: str,
    mq: MemoryQueue,
    minio_fs,
    clickhouse_db,
    normalizer: NormalizerModule,
    persistence: PersistenceModule,
) -> None:
    """Same event 100x -> 1 valid row, 99 external duplicates."""
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    events = [_FACTORIES[event_type](id_=fixed_id) for _ in range(100)]

    await _ingest(method, event_type, events, mq, minio_fs)
    _run_normalizer(normalizer, event_type, 100)
    _drain_all_into_persistence(persistence, mq)

    _, table = _PERSIST_TOPIC_TABLE[event_type]
    valid_rows = _wait_for_rows(clickhouse_db, f"SELECT * FROM {table}", 1)
    dup_rows = _wait_for_rows(clickhouse_db, "SELECT * FROM duplicates", 99)

    assert len(valid_rows) == 1
    assert len(dup_rows) == 99


# ══════════════════════════════════════════════════════════════════════════════
# General: internal duplicate (same message_id)
# ══════════════════════════════════════════════════════════════════════════════


async def test_internal_duplicate_same_message_id_is_silently_discarded(
    mq: MemoryQueue,
    normalizer: NormalizerModule,
) -> None:
    """Same message_id twice -> second copy dropped silently."""
    fixed_msg_id = uuid.uuid4().hex
    msg = {
        **_make_order(id_="o-fixed"),
        "message_id": fixed_msg_id,
        "ingested_at": "2026-01-15T10:00:00+00:00",
        "event_type": "order",
    }

    mq.publish(TRADE_NORMALIZATION, msg)
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")
    assert mq.consume_one(ORDERS_PERSISTENCE) is not None

    mq.publish(TRADE_NORMALIZATION, dict(msg))
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")
    assert mq.consume_one(ORDERS_PERSISTENCE) is None
    assert mq.consume_one(DUPLICATES) is None


# ══════════════════════════════════════════════════════════════════════════════
# General: alert generation — one test per ingestion method
# ══════════════════════════════════════════════════════════════════════════════


async def _assert_large_notional_alert(
    method: str,
    mq: MemoryQueue,
    minio_fs,
    normalizer: NormalizerModule,
    algos: AlgosModule,
    alerts_db,
) -> None:
    big_order = _make_order(quantity=5_000.0, price=300.0, currency="USD")
    await _ingest(method, "order", [big_order], mq, minio_fs)
    _run_normalizer(normalizer, "order", 1)
    _drain_algos_topics(algos, mq, "order")

    alerts = alerts_db.fetch_all("SELECT * FROM alerts")
    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert large_notional, f"No large_notional alert via {method}"
    assert large_notional[0]["severity"] == "high"
    assert large_notional[0]["tenant_id"] == "t1"

    alert_msg = mq.consume_one(ALERTS)
    assert alert_msg is not None


async def test_alert_generation_via_rest(mq, minio_fs, normalizer, algos, alerts_db):
    await _assert_large_notional_alert("rest", mq, minio_fs, normalizer, algos, alerts_db)


async def test_alert_generation_via_kafka(mq, minio_fs, normalizer, algos, alerts_db):
    await _assert_large_notional_alert("kafka", mq, minio_fs, normalizer, algos, alerts_db)


async def test_alert_generation_via_files(mq, minio_fs, normalizer, algos, alerts_db):
    await _assert_large_notional_alert("files", mq, minio_fs, normalizer, algos, alerts_db)


# ══════════════════════════════════════════════════════════════════════════════
# General: alert generation — one test per algorithm
# ══════════════════════════════════════════════════════════════════════════════


async def test_large_notional_algorithm(mq, minio_fs, normalizer, algos, alerts_db):
    """LargeNotionalAlgo: fires above $1 M, silent below."""
    small = _make_order(quantity=1.0, price=100.0, currency="USD")
    await _ingest("kafka", "order", [small], mq, minio_fs)
    _run_normalizer(normalizer, "order", 1)
    _drain_algos_topics(algos, mq, "order")
    assert alerts_db.fetch_all("SELECT * FROM alerts") == []

    big = _make_order(quantity=5_000.0, price=300.0, currency="USD")
    await _ingest("kafka", "order", [big], mq, minio_fs)
    _run_normalizer(normalizer, "order", 1)
    _drain_algos_topics(algos, mq, "order")

    rows = alerts_db.fetch_all("SELECT * FROM alerts")
    assert len(rows) == 1
    assert rows[0]["algorithm"] == "large_notional"
    assert rows[0]["severity"] == "high"


async def test_velocity_algorithm(redis_cache) -> None:
    """VelocityAlgo: no alert until max_events exceeded."""
    algo = VelocityAlgo(cache=redis_cache, max_events=5, window_seconds=60)
    event = {
        "event_type": "order",
        "id": "o-vel",
        "tenant_id": "t-velocity",
        "message_id": uuid.uuid4().hex,
        "notional_usd": 1_000.0,
    }

    for i in range(5):
        result = algo.evaluate(dict(event))
        assert result is None

    alert = algo.evaluate(dict(event))
    assert alert is not None
    assert alert.algorithm == "velocity"
    assert alert.severity == "medium"
    assert alert.details["event_count"] == 6


async def test_suspicious_counterparty_algorithm(
    mq, minio_fs, redis_cache, normalizer, alerts_db
):
    """SuspiciousCounterpartyAlgo: fires on blocklisted counterparty."""
    blocklist = {"cp-sanctioned", "cp-bad-actor"}
    algo = SuspiciousCounterpartyAlgo(suspicious_ids=blocklist)

    flagged_tx = {
        "event_type": "transaction",
        "id": "tx-bad",
        "tenant_id": "t1",
        "message_id": uuid.uuid4().hex,
        "counterparty_id": "cp-sanctioned",
        "amount_usd": 1_000.0,
    }
    alert = algo.evaluate(flagged_tx)
    assert alert is not None
    assert alert.algorithm == "suspicious_counterparty"

    clean_tx = {**flagged_tx, "counterparty_id": "cp-legit"}
    assert algo.evaluate(clean_tx) is None

    # Full pipeline test
    algos_module = AlgosModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=alerts_db,
        cache=redis_cache,
        lifecycle=LifecycleManager(),
    )
    await algos_module.initialize()
    algos_module.algorithms = [SuspiciousCounterpartyAlgo(suspicious_ids=blocklist)]

    suspicious_event = _make_transaction(
        id_="tx-flagged", counterparty_id="cp-bad-actor", currency="USD"
    )
    await _ingest("kafka", "transaction", [suspicious_event], mq, minio_fs)
    _run_normalizer(normalizer, "transaction", 1)

    while True:
        msg = mq.consume_one(TRANSACTIONS_ALGOS)
        if msg is None:
            break
        algos_module._evaluate(msg)

    persisted = alerts_db.fetch_all("SELECT * FROM alerts")
    assert len(persisted) >= 1
    assert any(r["algorithm"] == "suspicious_counterparty" for r in persisted)

    kafka_alert = mq.consume_one(ALERTS)
    assert kafka_alert is not None
    assert kafka_alert["algorithm"] == "suspicious_counterparty"
