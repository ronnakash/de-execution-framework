"""Tests for the Algos fraud detection module.

Algorithm unit tests are standalone; module integration tests use
MemoryQueue + MemoryDatabase + MemoryCache.
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.algos.main import AlgosModule
from de_platform.pipeline.algorithms import (
    LargeNotionalAlgo,
    SuspiciousCounterpartyAlgo,
    VelocityAlgo,
)
from de_platform.pipeline.topics import ALERTS, TRADES_ALGOS, TRANSACTIONS_ALGOS
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_trade_event(notional_usd: float = 500_000) -> dict:
    return {
        "event_type": "order",
        "id": "o1",
        "tenant_id": "t1",
        "message_id": uuid.uuid4().hex,
        "notional_usd": notional_usd,
        "notional": notional_usd,
        "currency": "USD",
        "transact_time": "2026-01-15T10:00:00+00:00",
    }


def _make_tx_event(counterparty_id: str = "cp1", amount_usd: float = 100.0) -> dict:
    return {
        "event_type": "transaction",
        "id": "tx1",
        "tenant_id": "t1",
        "counterparty_id": counterparty_id,
        "message_id": uuid.uuid4().hex,
        "amount_usd": amount_usd,
        "currency": "USD",
        "transact_time": "2026-01-15T10:00:00+00:00",
    }


async def _setup_module() -> tuple[AlgosModule, MemoryQueue, MemoryDatabase]:
    mq = MemoryQueue()
    db = MemoryDatabase()
    cache = MemoryCache()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({})

    module = AlgosModule(
        config=config, logger=logger, mq=mq, db=db, cache=cache, lifecycle=lifecycle,
        metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, mq, db


# ── Algorithm unit tests ──────────────────────────────────────────────────────


def test_large_notional_triggers_alert() -> None:
    algo = LargeNotionalAlgo(threshold_usd=1_000_000)
    alert = algo.evaluate(_make_trade_event(notional_usd=2_000_000))
    assert alert is not None
    assert alert.algorithm == "large_notional"
    assert alert.severity == "high"


def test_normal_notional_no_alert() -> None:
    algo = LargeNotionalAlgo(threshold_usd=1_000_000)
    assert algo.evaluate(_make_trade_event(notional_usd=500_000)) is None


def test_velocity_check_triggers_on_burst() -> None:
    cache = MemoryCache()
    algo = VelocityAlgo(cache=cache, max_events=5, window_seconds=60)
    event = _make_trade_event()

    for _ in range(5):
        assert algo.evaluate(event) is None  # under threshold

    alert = algo.evaluate(event)  # 6th event → triggers
    assert alert is not None
    assert alert.algorithm == "velocity"


def test_suspicious_counterparty_triggers_alert() -> None:
    algo = SuspiciousCounterpartyAlgo(suspicious_ids={"bad_actor"})
    alert = algo.evaluate(_make_tx_event(counterparty_id="bad_actor"))
    assert alert is not None
    assert alert.algorithm == "suspicious_counterparty"
    assert alert.severity == "critical"


def test_unknown_counterparty_no_alert() -> None:
    algo = SuspiciousCounterpartyAlgo(suspicious_ids={"bad_actor"})
    assert algo.evaluate(_make_tx_event(counterparty_id="normal_cp")) is None


# ── Module integration tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_alert_published_to_kafka() -> None:
    module, mq, db = await _setup_module()
    event = _make_trade_event(notional_usd=2_000_000)
    await module._evaluate(event)

    assert mq.consume_one(ALERTS) is not None


@pytest.mark.asyncio
async def test_alert_written_to_db() -> None:
    module, mq, db = await _setup_module()
    event = _make_trade_event(notional_usd=2_000_000)
    await module._evaluate(event)

    alerts = db.fetch_all("SELECT * FROM alerts")
    assert len(alerts) >= 1
    assert any(a["algorithm"] == "large_notional" for a in alerts)


@pytest.mark.asyncio
async def test_no_alert_for_normal_event() -> None:
    module, mq, db = await _setup_module()
    event = _make_trade_event(notional_usd=100)
    await module._evaluate(event)

    # LargeNotionalAlgo and SuspiciousCounterpartyAlgo should not fire
    # VelocityAlgo: first event in window → under max_events=100
    assert mq.consume_one(ALERTS) is None
    assert db.fetch_all("SELECT * FROM alerts") == []


# ── Per-tenant config tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_algo_skipped() -> None:
    """When large_notional is disabled for a tenant, no alert is generated."""
    module, mq, db = await _setup_module()
    module.config_cache._cache.set("algo_config:t1:large_notional", {
        "enabled": False, "thresholds": {},
    })

    event = _make_trade_event(notional_usd=2_000_000)
    await module._evaluate(event)

    # large_notional disabled → no alert from it
    alerts = db.fetch_all("SELECT * FROM alerts")
    large_notional_alerts = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert len(large_notional_alerts) == 0


@pytest.mark.asyncio
async def test_custom_threshold_applied() -> None:
    """Custom threshold_usd for a tenant lowers the alert trigger level."""
    module, mq, db = await _setup_module()
    module.config_cache._cache.set("algo_config:t1:large_notional", {
        "enabled": True, "thresholds": {"threshold_usd": 100_000},
    })

    # 500K exceeds the custom 100K threshold but not the default 1M
    event = _make_trade_event(notional_usd=500_000)
    await module._evaluate(event)

    alerts = db.fetch_all("SELECT * FROM alerts")
    large_notional_alerts = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert len(large_notional_alerts) == 1


@pytest.mark.asyncio
async def test_unconfigured_tenant_uses_defaults() -> None:
    """Tenants without config use default thresholds (all algos enabled)."""
    module, mq, db = await _setup_module()
    # No config for tenant — defaults apply

    event = _make_trade_event(notional_usd=2_000_000)
    await module._evaluate(event)

    alerts = db.fetch_all("SELECT * FROM alerts")
    large_notional_alerts = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert len(large_notional_alerts) == 1


# ── Algorithm with custom thresholds ─────────────────────────────────────────


def test_large_notional_with_custom_threshold() -> None:
    algo = LargeNotionalAlgo(threshold_usd=1_000_000)
    event = _make_trade_event(notional_usd=500_000)

    # Default threshold: no alert
    assert algo.evaluate(event) is None

    # Custom threshold: alert
    alert = algo.evaluate(event, thresholds={"threshold_usd": 100_000})
    assert alert is not None
    assert alert.details["threshold_usd"] == 100_000


def test_velocity_with_custom_thresholds() -> None:
    cache = MemoryCache()
    algo = VelocityAlgo(cache=cache, max_events=100, window_seconds=60)
    event = _make_trade_event()

    # With custom max_events=2, should trigger on 3rd event
    for _ in range(2):
        assert algo.evaluate(event, thresholds={"max_events": 2}) is None

    alert = algo.evaluate(event, thresholds={"max_events": 2})
    assert alert is not None


def test_suspicious_counterparty_with_custom_ids() -> None:
    algo = SuspiciousCounterpartyAlgo(suspicious_ids={"bad_actor"})
    event = _make_tx_event(counterparty_id="custom_bad")

    # Default IDs: no alert
    assert algo.evaluate(event) is None

    # Custom IDs: alert
    alert = algo.evaluate(event, thresholds={"suspicious_ids": ["custom_bad"]})
    assert alert is not None
