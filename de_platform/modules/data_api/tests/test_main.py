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
from de_platform.pipeline.auth_middleware import encode_token
from de_platform.pipeline.topics import ALERTS
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets

JWT_SECRET = "test-data-api-secret-at-least-32-bytes"


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


# ── Auth-enabled tests ───────────────────────────────────────────────────────


async def _setup_module_with_auth(
    db: MemoryDatabase | None = None,
) -> tuple[DataApiModule, MemoryQueue, MemoryDatabase]:
    mq = MemoryQueue()
    db_factory, db = _make_db_factory(db)
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})
    secrets = EnvSecrets(overrides={"JWT_SECRET": JWT_SECRET})

    module = DataApiModule(
        config=config, logger=logger, mq=mq, db_factory=db_factory, lifecycle=lifecycle,
        metrics=NoopMetrics(), secrets=secrets,
    )
    await module.initialize()
    return module, mq, db


def _auth_header(user_id: str, tenant_id: str, role: str) -> dict[str, str]:
    token = encode_token(user_id, tenant_id, role, JWT_SECRET)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_auth_required_when_jwt_secret_set() -> None:
    """Without a token, requests are rejected when JWT_SECRET is set."""
    module, mq, db = await _setup_module_with_auth()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts")
        assert resp.status == 401


@pytest.mark.asyncio
async def test_auth_tenant_scoped_alerts() -> None:
    """Authenticated user sees only their tenant's alerts."""
    db = MemoryDatabase()
    module, mq, db = await _setup_module_with_auth(db=db)
    db.bulk_insert("alerts", [
        _make_alert(tenant_id="acme", severity="high"),
        _make_alert(tenant_id="other", severity="high"),
    ])

    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        # Should only see acme's alert (tenant_id from JWT)
        assert len(data) == 1
        assert data[0]["tenant_id"] == "acme"


@pytest.mark.asyncio
async def test_auth_admin_override_tenant() -> None:
    """Admin can use ?tenant_id= to query other tenants."""
    db = MemoryDatabase()
    module, mq, db = await _setup_module_with_auth(db=db)
    db.bulk_insert("alerts", [
        _make_alert(tenant_id="acme", severity="high"),
        _make_alert(tenant_id="other", severity="high"),
    ])

    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts?tenant_id=other", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["tenant_id"] == "other"


@pytest.mark.asyncio
async def test_auth_non_admin_cannot_override_tenant() -> None:
    """Non-admin cannot use ?tenant_id= to access other tenants."""
    db = MemoryDatabase()
    module, mq, db = await _setup_module_with_auth(db=db)
    db.bulk_insert("alerts", [
        _make_alert(tenant_id="acme", severity="high"),
        _make_alert(tenant_id="other", severity="high"),
    ])

    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")
    async with TestClient(TestServer(app)) as client:
        # Viewer tries to pass ?tenant_id=other — should be ignored, gets own tenant
        resp = await client.get("/api/v1/alerts?tenant_id=other", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["tenant_id"] == "acme"


@pytest.mark.asyncio
async def test_auth_events_tenant_scoped() -> None:
    """Events endpoint respects JWT tenant_id."""
    db = MemoryDatabase()
    module, mq, db = await _setup_module_with_auth(db=db)
    db.bulk_insert("orders", [
        {"id": "o1", "tenant_id": "acme", "transact_time": "2026-01-15T10:00:00"},
        {"id": "o2", "tenant_id": "other", "transact_time": "2026-01-15T11:00:00"},
    ])

    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/orders", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["id"] == "o1"
