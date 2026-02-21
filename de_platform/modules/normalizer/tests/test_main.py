"""Tests for the Normalizer service module.

All tests use MemoryQueue + MemoryCache + MemoryDatabase (no external services).
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.normalizer.main import NormalizerModule
from de_platform.pipeline.topics import (
    DUPLICATES,
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
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_order_msg(
    message_id: str | None = None,
    tenant_id: str = "t1",
    event_id: str = "o1",
) -> dict:
    return {
        "event_type": "order",
        "id": event_id,
        "tenant_id": tenant_id,
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": 100.0,
        "price": 150.0,
        "order_type": "limit",
        "currency": "USD",
        "message_id": message_id or uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


def _make_execution_msg(message_id: str | None = None) -> dict:
    return {
        "event_type": "execution",
        "id": "e1",
        "tenant_id": "t1",
        "status": "filled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "order_id": "o1",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": 100.0,
        "price": 150.0,
        "execution_venue": "NYSE",
        "currency": "USD",
        "message_id": message_id or uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


def _make_tx_msg(message_id: str | None = None) -> dict:
    return {
        "event_type": "transaction",
        "id": "tx1",
        "tenant_id": "t1",
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": "acc1",
        "counterparty_id": "cp1",
        "amount": 1000.0,
        "currency": "USD",
        "transaction_type": "wire",
        "message_id": message_id or uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


async def _setup() -> tuple[NormalizerModule, MemoryQueue, MemoryCache, MemoryDatabase]:
    mq = MemoryQueue()
    cache = MemoryCache()
    db = MemoryDatabase()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({})

    module = NormalizerModule(
        config=config, logger=logger, mq=mq, cache=cache, db=db, lifecycle=lifecycle,
        metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, mq, cache, db


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_order_normalized_and_published_to_persistence_and_algos() -> None:
    module, mq, cache, db = await _setup()
    mq.publish(TRADE_NORMALIZATION, _make_order_msg())
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    assert mq.consume_one(ORDERS_PERSISTENCE) is not None
    assert mq.consume_one(TRADES_ALGOS) is not None


@pytest.mark.asyncio
async def test_execution_normalized_and_published() -> None:
    module, mq, cache, db = await _setup()
    mq.publish(TRADE_NORMALIZATION, _make_execution_msg())
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    assert mq.consume_one(EXECUTIONS_PERSISTENCE) is not None
    assert mq.consume_one(TRADES_ALGOS) is not None


@pytest.mark.asyncio
async def test_transaction_normalized_and_published() -> None:
    module, mq, cache, db = await _setup()
    mq.publish(TX_NORMALIZATION, _make_tx_msg())
    module._poll_and_process(TX_NORMALIZATION, "transaction")

    assert mq.consume_one(TRANSACTIONS_PERSISTENCE) is not None
    assert mq.consume_one(TRANSACTIONS_ALGOS) is not None


@pytest.mark.asyncio
async def test_notional_calculated() -> None:
    module, mq, cache, db = await _setup()
    msg = _make_order_msg()
    msg["quantity"] = 50.0
    msg["price"] = 200.0
    mq.publish(TRADE_NORMALIZATION, msg)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    enriched = mq.consume_one(ORDERS_PERSISTENCE)
    assert enriched is not None
    assert enriched["notional"] == pytest.approx(10_000.0)


@pytest.mark.asyncio
async def test_currency_conversion_applied() -> None:
    """USD -> USD should produce notional_usd == notional (rate = 1.0)."""
    module, mq, cache, db = await _setup()
    msg = _make_order_msg()
    msg["quantity"] = 10.0
    msg["price"] = 100.0
    mq.publish(TRADE_NORMALIZATION, msg)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    enriched = mq.consume_one(ORDERS_PERSISTENCE)
    assert enriched is not None
    assert "notional_usd" in enriched
    assert enriched["notional_usd"] == pytest.approx(1_000.0)


@pytest.mark.asyncio
async def test_internal_duplicate_silently_dropped() -> None:
    module, mq, cache, db = await _setup()
    mid = uuid.uuid4().hex
    msg = _make_order_msg(message_id=mid)

    mq.publish(TRADE_NORMALIZATION, msg)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")
    # Drain persistence & algos queues
    mq.consume_one(ORDERS_PERSISTENCE)
    mq.consume_one(TRADES_ALGOS)

    # Publish the same message_id again
    mq.publish(TRADE_NORMALIZATION, dict(msg))
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    assert mq.consume_one(ORDERS_PERSISTENCE) is None
    assert mq.consume_one(TRADES_ALGOS) is None


@pytest.mark.asyncio
async def test_external_duplicate_sent_to_duplicates_topic() -> None:
    module, mq, cache, db = await _setup()
    msg1 = _make_order_msg()

    mq.publish(TRADE_NORMALIZATION, msg1)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")
    mq.consume_one(ORDERS_PERSISTENCE)
    mq.consume_one(TRADES_ALGOS)

    # Same primary_key, different message_id
    msg2 = dict(msg1)
    msg2["message_id"] = uuid.uuid4().hex
    mq.publish(TRADE_NORMALIZATION, msg2)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    dup = mq.consume_one(DUPLICATES)
    assert dup is not None
    assert "original_event" in dup
    assert dup["original_event"]["id"] == msg2["id"]
    assert mq.consume_one(ORDERS_PERSISTENCE) is None


@pytest.mark.asyncio
async def test_external_duplicate_contains_original_event() -> None:
    """The duplicate record should contain the full original event data."""
    module, mq, cache, db = await _setup()
    msg1 = _make_order_msg()
    mq.publish(TRADE_NORMALIZATION, msg1)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")
    mq.consume_one(ORDERS_PERSISTENCE)
    mq.consume_one(TRADES_ALGOS)

    msg2 = dict(msg1)
    msg2["message_id"] = uuid.uuid4().hex
    mq.publish(TRADE_NORMALIZATION, msg2)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    dup = mq.consume_one(DUPLICATES)
    assert dup is not None
    original = dup["original_event"]
    assert original["id"] == msg2["id"]
    assert original["event_type"] == "order"
    assert original["tenant_id"] == msg2["tenant_id"]
    assert original["message_id"] == msg2["message_id"]


@pytest.mark.asyncio
async def test_external_duplicate_has_metadata_fields() -> None:
    """Duplicate record should have event_type, primary_key, message_id, tenant_id, received_at."""
    module, mq, cache, db = await _setup()
    msg1 = _make_order_msg()
    mq.publish(TRADE_NORMALIZATION, msg1)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")
    mq.consume_one(ORDERS_PERSISTENCE)
    mq.consume_one(TRADES_ALGOS)

    msg2 = dict(msg1)
    msg2["message_id"] = uuid.uuid4().hex
    mq.publish(TRADE_NORMALIZATION, msg2)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    dup = mq.consume_one(DUPLICATES)
    assert dup is not None
    assert dup["event_type"] == "order"
    assert "primary_key" in dup and dup["primary_key"]
    assert dup["message_id"] == msg2["message_id"]
    assert dup["tenant_id"] == msg2["tenant_id"]
    assert "received_at" in dup and dup["received_at"]


@pytest.mark.asyncio
async def test_primary_key_format() -> None:
    module, mq, cache, db = await _setup()
    msg = _make_order_msg(event_id="order123", tenant_id="tenant_abc")
    msg["transact_time"] = "2026-01-15T10:00:00+00:00"

    mq.publish(TRADE_NORMALIZATION, msg)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    enriched = mq.consume_one(ORDERS_PERSISTENCE)
    assert enriched is not None
    assert enriched["primary_key"] == "tenant_abc_order_order123_2026-01-15"
