"""Integration tests: AlgosModule persists alerts to real Postgres.

Verifies that the algos module can evaluate events and insert resulting
alerts into the Postgres alerts table.
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.algos.main import AlgosModule
from de_platform.pipeline.topics import ALERTS, TRADES_ALGOS
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


pytestmark = pytest.mark.integration


def _make_enriched_order(
    quantity: float = 100.0,
    price: float = 150.0,
    currency: str = "USD",
    tenant_id: str = "integ-test",
) -> dict:
    uid = uuid.uuid4().hex[:8]
    return {
        "id": f"ord-{uid}",
        "tenant_id": tenant_id,
        "event_type": "order",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "order_type": "limit",
        "currency": currency,
        "message_id": uuid.uuid4().hex,
        "notional": quantity * price,
        "notional_usd": quantity * price,
        "ingested_at": "2026-01-15T10:00:00+00:00",
        "normalized_at": "2026-01-15T10:00:01+00:00",
        "primary_key": f"{tenant_id}_order_{uid}_2026-01-15",
    }


async def test_algos_persists_large_notional_alert_to_postgres(alerts_db, redis_cache):
    """AlgosModule inserts large_notional alert into Postgres alerts table."""
    mq = MemoryQueue()

    module = AlgosModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=alerts_db,
        cache=redis_cache,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await module.initialize()

    # $1.5M notional: should trigger large_notional alert
    big_order = _make_enriched_order(quantity=5000.0, price=300.0)
    await module._evaluate(big_order)

    # Check alert was published to Kafka
    alert_msg = mq.consume_one(ALERTS)
    assert alert_msg is not None
    assert alert_msg["algorithm"] == "large_notional"
    assert alert_msg["severity"] == "high"

    # Check alert was persisted to Postgres
    rows = await alerts_db.fetch_all_async("SELECT * FROM alerts")
    integ_alerts = [r for r in rows if r.get("tenant_id") == "integ-test"]
    assert len(integ_alerts) >= 1
    assert integ_alerts[0]["algorithm"] == "large_notional"


async def test_algos_no_alert_for_small_trade(alerts_db, redis_cache):
    """AlgosModule does not fire for small trades."""
    mq = MemoryQueue()

    module = AlgosModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        db=alerts_db,
        cache=redis_cache,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    await module.initialize()

    small_order = _make_enriched_order(quantity=10.0, price=100.0)
    await module._evaluate(small_order)

    assert mq.consume_one(ALERTS) is None
