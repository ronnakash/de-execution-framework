"""Tests for the Batch Algos module."""

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
    notional_usd: float = 500_000,
    transact_time: str = "2026-01-15T10:00:00+00:00",
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


async def _setup_module(
    config_overrides: dict | None = None,
    orders: list[dict] | None = None,
) -> tuple[BatchAlgosModule, MemoryQueue, MemoryDatabase]:
    mq = MemoryQueue()
    events_db = MemoryDatabase()
    events_db.connect()
    cache = MemoryCache()
    logger = LoggerFactory(default_impl="memory")

    cfg = {
        "tenant-id": "t1",
        "start-date": "2026-01-15",
        "end-date": "2026-01-15",
    }
    if config_overrides:
        cfg.update(config_overrides)
    config = ModuleConfig(cfg)

    db_factory = DatabaseFactory(configs={})
    db_factory.register_instance("events", events_db)

    # Pre-populate events
    if orders:
        for order in orders:
            events_db.insert_one("orders", order)

    module = BatchAlgosModule(
        config=config, logger=logger, db_factory=db_factory,
        cache=cache, mq=mq, metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, mq, events_db


@pytest.mark.asyncio
async def test_batch_algos_empty_range() -> None:
    """No events in range -> exit 0, no alerts."""
    module, mq, db = await _setup_module()
    rc = await module.execute()
    assert rc == 0
    assert mq.consume_one(ALERTS) is None


@pytest.mark.asyncio
async def test_batch_algos_publishes_alerts() -> None:
    """Events above threshold produce alerts on the Kafka topic."""
    orders = [
        _make_order(notional_usd=2_000_000, transact_time="2026-01-15T10:01:00+00:00"),
        _make_order(notional_usd=500_000, transact_time="2026-01-15T10:02:00+00:00"),
    ]
    module, mq, db = await _setup_module(orders=orders)
    rc = await module.execute()
    assert rc == 0

    alerts = []
    while True:
        a = mq.consume_one(ALERTS)
        if a is None:
            break
        alerts.append(a)

    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert len(large_notional) >= 1


@pytest.mark.asyncio
async def test_batch_algos_dedup_in_run() -> None:
    """Same event in overlapping windows produces a single alert."""
    # One large order — with default 5-min window, 1-min slide it appears in
    # multiple windows. Dedup should collapse to one alert per (algo, event_id).
    order = _make_order(
        notional_usd=2_000_000,
        transact_time="2026-01-15T10:02:00+00:00",
    )
    # Need enough events to span a window — add padding events
    orders = [
        _make_order(notional_usd=100, transact_time="2026-01-15T10:00:00+00:00"),
        order,
        _make_order(notional_usd=100, transact_time="2026-01-15T10:06:00+00:00"),
    ]
    module, mq, db = await _setup_module(orders=orders)
    rc = await module.execute()
    assert rc == 0

    alerts = []
    while True:
        a = mq.consume_one(ALERTS)
        if a is None:
            break
        alerts.append(a)

    # The large order should appear in multiple windows but dedup to one alert
    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    event_ids = {a.get("event_id") for a in large_notional}
    assert len(event_ids) == len(large_notional), "Each event_id should appear at most once"


@pytest.mark.asyncio
async def test_batch_algos_missing_args() -> None:
    """Missing required args produces exit code 1."""
    mq = MemoryQueue()
    events_db = MemoryDatabase()
    cache = MemoryCache()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({})  # no tenant-id, start-date, end-date

    db_factory = DatabaseFactory(configs={})
    db_factory.register_instance("events", events_db)

    module = BatchAlgosModule(
        config=config, logger=logger, db_factory=db_factory,
        cache=cache, mq=mq, metrics=NoopMetrics(),
    )
    await module.initialize()
    rc = await module.execute()
    assert rc == 1
