"""Tests for the Data API module.

Uses MemoryQueue + MemoryDatabase and aiohttp TestClient/TestServer
for HTTP endpoint testing.
"""

from __future__ import annotations

import uuid

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.data_api.main import DataApiModule
from de_platform.pipeline.topics import ALERTS
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_alert(
    alert_id: str | None = None,
    tenant_id: str = "t1",
    severity: str = "high",
) -> dict:
    return {
        "alert_id": alert_id or uuid.uuid4().hex,
        "tenant_id": tenant_id,
        "event_type": "order",
        "event_id": "o1",
        "message_id": uuid.uuid4().hex,
        "algorithm": "large_notional",
        "severity": severity,
        "description": "Large trade detected",
        "details": {},
        "created_at": "2026-01-15T10:00:00+00:00",
    }


def _make_db_factory(db: MemoryDatabase | None = None) -> tuple[DatabaseFactory, MemoryDatabase]:
    db = db or MemoryDatabase()
    factory = DatabaseFactory({})
    factory.register_instance("events", db)
    factory.register_instance("alerts", db)
    return factory, db


async def _setup_module(
    db: MemoryDatabase | None = None,
) -> tuple[DataApiModule, MemoryQueue, MemoryDatabase]:
    mq = MemoryQueue()
    db_factory, db = _make_db_factory(db)
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})

    module = DataApiModule(
        config=config, logger=logger, mq=mq, db_factory=db_factory, lifecycle=lifecycle,
        metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, mq, db


# ── Kafka consumer tests ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_alerts_consumed_from_kafka_and_stored() -> None:
    module, mq, db = await _setup_module()
    alert = _make_alert()
    mq.publish(ALERTS, alert)
    await module._consume_alerts()

    rows = db.fetch_all("SELECT * FROM alerts")
    assert len(rows) == 1
    assert rows[0]["alert_id"] == alert["alert_id"]


# ── REST endpoint tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_alerts_with_filters() -> None:
    db = MemoryDatabase()
    module, mq, db = await _setup_module(db=db)
    db.bulk_insert("alerts", [
        _make_alert(tenant_id="t1", severity="high"),
        _make_alert(tenant_id="t1", severity="low"),
        _make_alert(tenant_id="t2", severity="high"),
    ])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts?tenant_id=t1&severity=high")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["tenant_id"] == "t1"
        assert data[0]["severity"] == "high"


@pytest.mark.asyncio
async def test_get_alert_by_id() -> None:
    db = MemoryDatabase()
    module, mq, db = await _setup_module(db=db)
    alert_id = uuid.uuid4().hex
    db.bulk_insert("alerts", [_make_alert(alert_id=alert_id)])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(f"/api/v1/alerts/{alert_id}")
        assert resp.status == 200
        data = await resp.json()
        assert data["alert_id"] == alert_id


@pytest.mark.asyncio
async def test_get_alert_not_found_returns_404() -> None:
    module, mq, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts/nonexistent")
        assert resp.status == 404


@pytest.mark.asyncio
async def test_get_events_returns_list() -> None:
    db = MemoryDatabase()
    module, mq, db = await _setup_module(db=db)
    db.bulk_insert("orders", [
        {"id": "o1", "tenant_id": "t1", "transact_time": "2026-01-15T10:00:00"},
        {"id": "o2", "tenant_id": "t2", "transact_time": "2026-01-15T11:00:00"},
    ])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/orders?tenant_id=t1")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["id"] == "o1"


@pytest.mark.asyncio
async def test_get_alerts_limit_and_offset() -> None:
    db = MemoryDatabase()
    module, mq, db = await _setup_module(db=db)
    db.bulk_insert("alerts", [_make_alert() for _ in range(10)])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts?limit=3&offset=2")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 3
