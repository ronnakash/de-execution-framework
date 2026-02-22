"""End-to-end pipeline integration test.

Exercises the full fraud detection pipeline using in-memory implementations
so the test runs without any external services (Kafka, ClickHouse, Redis, PostgreSQL).

Flow (mirrors the 9-step plan in pipeline_plan.md §Step 12):
  1. Bootstrap in-memory infrastructure
  2. Load currency rates into the currency database
  3. Publish a batch of mixed events (order/execution/transaction) to normalization topics
  4. Run Normalizer (poll each topic once per event)
  5. Verify enriched events appear on persistence and algos topics
  6. Run Persistence (flush to "ClickHouse" + filesystem)
  7. Verify rows in the ClickHouse substitute (MemoryDatabase) and files in MemoryFileSystem
  8. Run Algos with a large-notional event
  9. Verify alert in the alerts table and on the alerts Kafka topic
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.algos.main import AlgosModule
from de_platform.modules.normalizer.main import NormalizerModule
from de_platform.modules.persistence.buffer import BufferKey
from de_platform.modules.persistence.main import PersistenceModule
from de_platform.pipeline.topics import (
    ALERTS,
    EXECUTIONS_PERSISTENCE,
    ORDERS_PERSISTENCE,
    TRADE_NORMALIZATION,
    TRADES_ALGOS,
    TRANSACTIONS_ALGOS,
    TRANSACTIONS_PERSISTENCE,
    TX_NORMALIZATION,
)
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


# ── Event factories ───────────────────────────────────────────────────────────


def _order(currency: str = "USD", quantity: float = 100.0, price: float = 200.0) -> dict:
    return {
        "event_type": "order",
        "id": f"o-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "order_type": "limit",
        "currency": currency,
        "message_id": uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


def _execution(currency: str = "EUR") -> dict:
    return {
        "event_type": "execution",
        "id": f"e-{uuid.uuid4().hex[:8]}",
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
        "message_id": uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


def _transaction(currency: str = "GBP", amount: float = 500.0) -> dict:
    return {
        "event_type": "transaction",
        "id": f"tx-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": "acc1",
        "counterparty_id": "cp1",
        "amount": amount,
        "currency": currency,
        "transaction_type": "wire",
        "message_id": uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


# ── E2E test ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pipeline_end_to_end() -> None:
    """Full pipeline: starters → Normalizer → Persistence + Algos → alerts."""

    # ── Step 1: Bootstrap in-memory infrastructure ────────────────────────
    mq = MemoryQueue()
    cache = MemoryCache()
    fs = MemoryFileSystem()

    # Separate "databases" for different roles (mirrors production topology)
    currency_db = MemoryDatabase()   # PostgreSQL currency instance
    clickhouse_db = MemoryDatabase() # ClickHouse event storage
    alerts_db = MemoryDatabase()     # PostgreSQL alerts instance

    currency_db.connect()
    clickhouse_db.connect()
    alerts_db.connect()

    # ── Step 2: Load currency rates ───────────────────────────────────────
    currency_db.bulk_insert("currency_rates", [
        {"from_currency": "EUR", "to_currency": "USD", "rate": 1.10},
        {"from_currency": "GBP", "to_currency": "USD", "rate": 1.25},
    ])
    # Pre-populate cache so CurrencyConverter bypasses MemoryDatabase's
    # limited SQL parser (which cannot handle two-column WHERE clauses).
    cache.set("currency_rate:EUR_USD", 1.10)
    cache.set("currency_rate:GBP_USD", 1.25)

    # ── Initialize Normalizer ─────────────────────────────────────────────
    normalizer = NormalizerModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        cache=cache,
        db=currency_db,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await normalizer.initialize()

    # ── Step 3: Publish mixed events to normalization topics ──────────────
    order_raw      = _order(currency="USD", quantity=100.0, price=200.0)
    execution_raw  = _execution(currency="EUR")  # will be converted EUR→USD
    transaction_raw = _transaction(currency="GBP", amount=500.0)  # GBP→USD

    mq.publish(TRADE_NORMALIZATION, order_raw)
    mq.publish(TRADE_NORMALIZATION, execution_raw)
    mq.publish(TX_NORMALIZATION, transaction_raw)

    # ── Step 4: Run Normalizer ────────────────────────────────────────────
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")   # → order
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")   # → execution
    normalizer._poll_and_process(TX_NORMALIZATION, "transaction") # → transaction

    # ── Step 5: Verify events on persistence and algos topics ─────────────
    order_enriched = mq.consume_one(ORDERS_PERSISTENCE)
    assert order_enriched is not None, "Order not on persistence topic"
    assert order_enriched["notional"] == pytest.approx(100.0 * 200.0)
    assert order_enriched["notional_usd"] == pytest.approx(20_000.0)  # USD rate = 1.0
    assert "primary_key" in order_enriched
    assert "normalized_at" in order_enriched

    execution_enriched = mq.consume_one(EXECUTIONS_PERSISTENCE)
    assert execution_enriched is not None, "Execution not on persistence topic"
    assert execution_enriched["notional_usd"] == pytest.approx(50.0 * 210.0 * 1.10)

    tx_enriched = mq.consume_one(TRANSACTIONS_PERSISTENCE)
    assert tx_enriched is not None, "Transaction not on persistence topic"
    assert tx_enriched["amount_usd"] == pytest.approx(500.0 * 1.25)

    # Both trade events should appear on trades_algos
    algos_event_1 = mq.consume_one(TRADES_ALGOS)
    algos_event_2 = mq.consume_one(TRADES_ALGOS)
    assert algos_event_1 is not None
    assert algos_event_2 is not None

    tx_algos_event = mq.consume_one(TRANSACTIONS_ALGOS)
    assert tx_algos_event is not None

    # ── Step 6: Run Persistence ───────────────────────────────────────────
    persistence = PersistenceModule(
        # flush every single row immediately for test determinism
        config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=clickhouse_db,
        fs=fs,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await persistence.initialize()

    # Feed the saved enriched events into the buffer and force a flush
    for msg, table in [
        (order_enriched,     "orders"),
        (execution_enriched, "executions"),
        (tx_enriched,        "transactions"),
    ]:
        key = BufferKey(tenant_id=msg["tenant_id"], table=table)
        persistence.buffer.append(key, msg)  # type: ignore[union-attr]
    persistence._flush_all()

    # ── Step 7: Verify ClickHouse and filesystem ──────────────────────────
    assert len(clickhouse_db.fetch_all("SELECT * FROM orders")) == 1
    assert len(clickhouse_db.fetch_all("SELECT * FROM executions")) == 1
    assert len(clickhouse_db.fetch_all("SELECT * FROM transactions")) == 1

    assert len(fs.list("acme/orders/")) == 1
    assert len(fs.list("acme/executions/")) == 1
    assert len(fs.list("acme/transactions/")) == 1

    # Verify the JSONL content of one file
    order_file = fs.list("acme/orders/")[0]
    import json
    order_row = json.loads(fs.read(order_file).decode())
    assert order_row["tenant_id"] == "acme"
    assert order_row["notional_usd"] == pytest.approx(20_000.0)

    # ── Step 8: Initialize Algos and run with a large-notional event ──────
    algos = AlgosModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=alerts_db,
        cache=cache,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await algos.initialize()

    # Construct a trade event whose notional_usd exceeds the $1 M threshold
    big_trade = {
        "event_type": "order",
        "id": "o-big",
        "tenant_id": "acme",
        "message_id": uuid.uuid4().hex,
        "notional_usd": 2_500_000.0,
        "notional": 2_500_000.0,
        "currency": "USD",
        "transact_time": "2026-01-15T10:00:00+00:00",
    }
    await algos._evaluate(big_trade)

    # ── Step 9: Verify alert ──────────────────────────────────────────────
    alert_msg = mq.consume_one(ALERTS)
    assert alert_msg is not None, "No alert published to ALERTS topic"
    assert alert_msg["algorithm"] == "large_notional"
    assert alert_msg["tenant_id"] == "acme"
    assert alert_msg["severity"] == "high"

    alert_rows = alerts_db.fetch_all("SELECT * FROM alerts")
    assert len(alert_rows) >= 1
    assert any(r["algorithm"] == "large_notional" for r in alert_rows)


@pytest.mark.asyncio
async def test_normalizer_dedup_prevents_double_processing() -> None:
    """Duplicate events (same message_id) must not appear on downstream topics."""
    mq = MemoryQueue()
    cache = MemoryCache()
    db = MemoryDatabase()

    normalizer = NormalizerModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq, cache=cache, db=db,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await normalizer.initialize()

    msg = _order(currency="USD")
    mq.publish(TRADE_NORMALIZATION, msg)
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")

    # Drain persistence topic
    mq.consume_one(ORDERS_PERSISTENCE)
    mq.consume_one(TRADES_ALGOS)

    # Re-publish the exact same message (same message_id)
    mq.publish(TRADE_NORMALIZATION, dict(msg))
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")

    assert mq.consume_one(ORDERS_PERSISTENCE) is None, "Duplicate reached persistence"
    assert mq.consume_one(TRADES_ALGOS) is None, "Duplicate reached algos"


@pytest.mark.asyncio
async def test_normalizer_external_duplicate_routed_to_duplicates_topic() -> None:
    """Same primary_key, different message_id → external duplicate → duplicates topic."""
    mq = MemoryQueue()
    cache = MemoryCache()
    db = MemoryDatabase()

    normalizer = NormalizerModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq, cache=cache, db=db,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await normalizer.initialize()

    original = _order(currency="USD")
    mq.publish(TRADE_NORMALIZATION, original)
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")
    # Drain
    mq.consume_one(ORDERS_PERSISTENCE)
    mq.consume_one(TRADES_ALGOS)

    # Same order id + tenant + date (same primary_key), new message_id
    duplicate = dict(original)
    duplicate["message_id"] = uuid.uuid4().hex
    mq.publish(TRADE_NORMALIZATION, duplicate)
    normalizer._poll_and_process(TRADE_NORMALIZATION, "trade")

    from de_platform.pipeline.topics import DUPLICATES
    dup_event = mq.consume_one(DUPLICATES)
    assert dup_event is not None, "External duplicate not routed to duplicates topic"
    assert mq.consume_one(ORDERS_PERSISTENCE) is None


@pytest.mark.asyncio
async def test_persistence_shutdown_flushes_all_buffers() -> None:
    """Buffered rows that haven't hit threshold must still flush on shutdown."""
    mq = MemoryQueue()
    db = MemoryDatabase()
    fs = MemoryFileSystem()

    persistence = PersistenceModule(
        config=ModuleConfig({"flush-threshold": 10_000, "flush-interval": 3600}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq, db=db, fs=fs,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await persistence.initialize()

    # Add 3 rows — well below the threshold, would not flush normally
    for _ in range(3):
        key = BufferKey(tenant_id="test_tenant", table="orders")
        persistence.buffer.append(key, _order())  # type: ignore[union-attr]

    # Simulate shutdown
    persistence._flush_all()

    rows = db.fetch_all("SELECT * FROM orders")
    assert len(rows) == 3
    assert len(fs.list("test_tenant/orders/")) == 1


@pytest.mark.asyncio
async def test_algos_no_alert_for_small_trade() -> None:
    """Events well below the $1 M threshold must not generate alerts."""
    mq = MemoryQueue()
    db = MemoryDatabase()
    cache = MemoryCache()

    algos = AlgosModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq, db=db, cache=cache,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await algos.initialize()

    small_trade = {
        "event_type": "order", "id": "o1", "tenant_id": "t1",
        "message_id": uuid.uuid4().hex,
        "notional_usd": 5_000.0, "notional": 5_000.0,
    }
    await algos._evaluate(small_trade)

    assert mq.consume_one(ALERTS) is None
    assert db.fetch_all("SELECT * FROM alerts") == []
