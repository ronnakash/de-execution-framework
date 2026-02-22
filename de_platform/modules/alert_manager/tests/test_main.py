"""Tests for the Alert Management Service."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.alert_manager.main import AlertManagerModule, _generate_title
from de_platform.pipeline.topics import ALERTS
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets


def _make_alert(
    algorithm: str = "large_notional",
    event_id: str = "o1",
    tenant_id: str = "t1",
    severity: str = "high",
    alert_id: str | None = None,
) -> dict:
    return {
        "alert_id": alert_id or uuid.uuid4().hex,
        "tenant_id": tenant_id,
        "event_type": "order",
        "event_id": event_id,
        "message_id": uuid.uuid4().hex,
        "algorithm": algorithm,
        "severity": severity,
        "description": f"Test alert from {algorithm}",
        "details": '{"test": true}',
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


async def _setup_module() -> tuple[AlertManagerModule, MemoryQueue, MemoryDatabase]:
    mq = MemoryQueue()
    db = MemoryDatabase()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({})
    secrets = EnvSecrets()

    module = AlertManagerModule(
        config=config, logger=logger, mq=mq, db=db,
        lifecycle=lifecycle, metrics=NoopMetrics(), secrets=secrets,
    )
    await module.initialize()
    return module, mq, db


# ── Dedup tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_alert_persisted() -> None:
    module, mq, db = await _setup_module()
    alert = _make_alert()
    mq.publish(ALERTS, alert)

    await module._consume_and_process()

    rows = db.fetch_all("SELECT * FROM alerts")
    assert len(rows) == 1
    assert rows[0]["algorithm"] == "large_notional"


@pytest.mark.asyncio
async def test_duplicate_alert_not_persisted() -> None:
    module, mq, db = await _setup_module()
    alert = _make_alert(event_id="o1", algorithm="large_notional")
    mq.publish(ALERTS, alert)
    await module._consume_and_process()

    # Same (algorithm, event_id) — should be deduped
    dup = _make_alert(event_id="o1", algorithm="large_notional", alert_id=uuid.uuid4().hex)
    mq.publish(ALERTS, dup)
    await module._consume_and_process()

    rows = db.fetch_all("SELECT * FROM alerts")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_different_algo_same_event_both_persisted() -> None:
    module, mq, db = await _setup_module()
    a1 = _make_alert(event_id="o1", algorithm="large_notional")
    a2 = _make_alert(event_id="o1", algorithm="velocity", severity="medium")
    mq.publish(ALERTS, a1)
    mq.publish(ALERTS, a2)

    await module._consume_and_process()
    await module._consume_and_process()

    rows = db.fetch_all("SELECT * FROM alerts")
    assert len(rows) == 2


# ── Case aggregation tests ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_first_alert_creates_new_case() -> None:
    module, mq, db = await _setup_module()
    alert = _make_alert()
    mq.publish(ALERTS, alert)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 1
    assert cases[0]["tenant_id"] == "t1"
    assert cases[0]["status"] == "open"
    assert cases[0]["alert_count"] == 1


@pytest.mark.asyncio
async def test_same_algo_within_window_groups_into_case() -> None:
    module, mq, db = await _setup_module()
    a1 = _make_alert(event_id="o1", algorithm="large_notional")
    a2 = _make_alert(event_id="o2", algorithm="large_notional")
    mq.publish(ALERTS, a1)
    await module._consume_and_process()

    mq.publish(ALERTS, a2)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 1
    assert cases[0]["alert_count"] == 2


@pytest.mark.asyncio
async def test_same_event_id_cross_algo_groups_into_case() -> None:
    module, mq, db = await _setup_module()
    a1 = _make_alert(event_id="o1", algorithm="large_notional", severity="high")
    a2 = _make_alert(event_id="o1", algorithm="velocity", severity="medium")
    mq.publish(ALERTS, a1)
    await module._consume_and_process()

    mq.publish(ALERTS, a2)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 1
    assert cases[0]["alert_count"] == 2
    algos = cases[0]["algorithms"]
    assert "large_notional" in algos
    assert "velocity" in algos


@pytest.mark.asyncio
async def test_case_severity_escalates() -> None:
    module, mq, db = await _setup_module()
    a1 = _make_alert(event_id="o1", algorithm="large_notional", severity="medium")
    a2 = _make_alert(event_id="o2", algorithm="large_notional", severity="critical")
    mq.publish(ALERTS, a1)
    await module._consume_and_process()

    mq.publish(ALERTS, a2)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 1
    assert cases[0]["severity"] == "critical"


@pytest.mark.asyncio
async def test_different_tenants_separate_cases() -> None:
    module, mq, db = await _setup_module()
    a1 = _make_alert(tenant_id="t1", event_id="o1")
    a2 = _make_alert(tenant_id="t2", event_id="o2")
    mq.publish(ALERTS, a1)
    await module._consume_and_process()

    mq.publish(ALERTS, a2)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 2


# ── Case title tests ─────────────────────────────────────────────────────────


def test_generate_title_single_algo() -> None:
    title = _generate_title(3, ["large_notional"], "acme")
    assert "Large notional detected" in title
    assert "acme" in title
    assert "3 alerts" in title


def test_generate_title_multiple_algos() -> None:
    title = _generate_title(5, ["large_notional", "velocity"], "acme")
    assert "Multiple fraud signals" in title
    assert "2 algorithms" in title


# ── Case status tests ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_case_status() -> None:
    module, mq, db = await _setup_module()
    alert = _make_alert()
    mq.publish(ALERTS, alert)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    case_id = cases[0]["case_id"]

    # Simulate the status update directly (not via HTTP)
    case = cases[0]
    updated = dict(case)
    updated["status"] = "investigating"
    await db.execute_async("DELETE FROM cases WHERE case_id = $1", [case_id])
    await db.insert_one_async("cases", updated)

    cases_after = db.fetch_all("SELECT * FROM cases")
    assert cases_after[0]["status"] == "investigating"
