"""Tests for batch_algos --algos CLI override (Phase 4).

Verifies that specifying --algos filters which algorithms run,
overriding client config.
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.batch_algos.main import BatchAlgosModule
from de_platform.pipeline.topics import ALERTS
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


def _make_order(
    tenant_id: str = "t1",
    notional_usd: float = 2_000_000,
    transact_time: str = "2026-01-15T10:01:00+00:00",
) -> dict:
    return {
        "id": f"o-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant_id,
        "event_type": "order",
        "message_id": uuid.uuid4().hex,
        "notional_usd": notional_usd,
        "currency": "USD",
        "transact_time": transact_time,
        "primary_key": f"{tenant_id}_order_{uuid.uuid4().hex[:8]}_2026-01-15",
    }


async def _setup(config_overrides: dict | None = None, orders: list | None = None):
    mq = MemoryQueue()
    events_db = MemoryDatabase()
    events_db.connect()
    cache = MemoryCache()
    logger = LoggerFactory(default_impl="memory")

    cfg = {"tenant-id": "t1", "start-date": "2026-01-15", "end-date": "2026-01-15"}
    if config_overrides:
        cfg.update(config_overrides)

    db_factory = DatabaseFactory(configs={})
    db_factory.register_instance("events", events_db)

    if orders:
        for o in orders:
            events_db.insert_one("orders", o)

    module = BatchAlgosModule(
        config=ModuleConfig(cfg), logger=logger, db_factory=db_factory,
        cache=cache, mq=mq, metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, mq


def _drain_alerts(mq: MemoryQueue) -> list[dict]:
    alerts = []
    while True:
        a = mq.consume_one(ALERTS)
        if a is None:
            break
        alerts.append(a)
    return alerts


@pytest.mark.asyncio
async def test_algo_override_filters_to_large_notional_only() -> None:
    """--algos large_notional runs only large_notional, not velocity."""
    orders = [_make_order() for _ in range(3)]
    module, mq = await _setup(
        config_overrides={"algos": "large_notional"},
        orders=orders,
    )
    assert module._algo_override == {"large_notional"}

    await module.execute()
    alerts = _drain_alerts(mq)

    algos_used = {a["algorithm"] for a in alerts}
    assert "large_notional" in algos_used or len(alerts) == 0
    assert "velocity" not in algos_used
    assert "suspicious_counterparty" not in algos_used


@pytest.mark.asyncio
async def test_algo_override_empty_runs_all() -> None:
    """No --algos means all algorithms are eligible (default behavior)."""
    module, mq = await _setup(orders=[_make_order()])
    assert module._algo_override is None

    # _get_algo_config should use client config (default: enabled)
    enabled, _ = module._get_algo_config("t1", "large_notional")
    assert enabled is True
    enabled_vel, _ = module._get_algo_config("t1", "velocity")
    assert enabled_vel is True


@pytest.mark.asyncio
async def test_algo_override_disables_non_specified() -> None:
    """--algos velocity should disable large_notional."""
    module, mq = await _setup(
        config_overrides={"algos": "velocity"},
        orders=[_make_order()],
    )
    enabled_ln, _ = module._get_algo_config("t1", "large_notional")
    assert enabled_ln is False
    enabled_v, _ = module._get_algo_config("t1", "velocity")
    assert enabled_v is True


@pytest.mark.asyncio
async def test_algo_override_multiple_algos() -> None:
    """--algos velocity,large_notional enables both."""
    module, mq = await _setup(
        config_overrides={"algos": "velocity,large_notional"},
    )
    assert module._algo_override == {"velocity", "large_notional"}
    enabled_ln, _ = module._get_algo_config("t1", "large_notional")
    enabled_v, _ = module._get_algo_config("t1", "velocity")
    enabled_sc, _ = module._get_algo_config("t1", "suspicious_counterparty")
    assert enabled_ln is True
    assert enabled_v is True
    assert enabled_sc is False
