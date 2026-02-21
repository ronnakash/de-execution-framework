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
