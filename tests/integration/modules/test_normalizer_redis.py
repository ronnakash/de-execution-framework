"""Integration tests: Normalizer deduplication with real Redis.

Verifies that the normalizer's EventDeduplicator works correctly with
a real Redis cache — checking dedup state persistence across calls.
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.normalizer.main import NormalizerModule
from de_platform.pipeline.topics import (
    DUPLICATES,
    ORDERS_PERSISTENCE,
    TRADE_NORMALIZATION,
    TRADES_ALGOS,
)
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


pytestmark = pytest.mark.integration


def _make_raw_order(id_: str | None = None, tenant_id: str = "integ-test") -> dict:
    return {
        "id": id_ or f"ord-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant_id,
        "event_type": "order",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": 100.0,
        "price": 150.0,
        "order_type": "limit",
        "currency": "USD",
        "message_id": uuid.uuid4().hex,
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }


async def test_normalizer_dedup_with_redis(redis_cache, warehouse_db):
    """Normalizer correctly deduplicates using real Redis."""
    mq = MemoryQueue()

    module = NormalizerModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        cache=redis_cache,
        db=warehouse_db,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await module.initialize()

    fixed_id = f"ord-{uuid.uuid4().hex[:8]}"

    # First message: should be processed normally
    msg1 = _make_raw_order(id_=fixed_id)
    mq.publish(TRADE_NORMALIZATION, msg1)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    orders = []
    while True:
        m = mq.consume_one(ORDERS_PERSISTENCE)
        if m is None:
            break
        orders.append(m)
    assert len(orders) == 1

    # Second message with same primary_key but different message_id: external duplicate
    msg2 = _make_raw_order(id_=fixed_id)
    mq.publish(TRADE_NORMALIZATION, msg2)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    dups = []
    while True:
        m = mq.consume_one(DUPLICATES)
        if m is None:
            break
        dups.append(m)
    assert len(dups) == 1
    assert dups[0]["primary_key"].endswith(f"{fixed_id}_2026-01-15")

    # Third message with same message_id as first: internal duplicate (dropped)
    msg3 = dict(msg1)  # same message_id
    mq.publish(TRADE_NORMALIZATION, msg3)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    # Nothing new in orders or duplicates
    assert mq.consume_one(ORDERS_PERSISTENCE) is None
    assert mq.consume_one(DUPLICATES) is None


async def test_normalizer_enrichment_with_redis_cache(redis_cache, warehouse_db):
    """Normalizer enriches events using currency rates from Redis cache."""
    mq = MemoryQueue()

    # Pre-seed currency rate in Redis (EUR→USD)
    redis_cache.set("currency_rate:EUR_USD", 1.10)

    module = NormalizerModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        cache=redis_cache,
        db=warehouse_db,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await module.initialize()

    order = _make_raw_order()
    order["currency"] = "EUR"
    order["quantity"] = 100.0
    order["price"] = 200.0
    mq.publish(TRADE_NORMALIZATION, order)
    module._poll_and_process(TRADE_NORMALIZATION, "trade")

    enriched = mq.consume_one(ORDERS_PERSISTENCE)
    assert enriched is not None
    # notional_usd should be quantity * price * EUR_USD rate
    assert enriched["notional_usd"] == pytest.approx(100.0 * 200.0 * 1.10, rel=0.01)
