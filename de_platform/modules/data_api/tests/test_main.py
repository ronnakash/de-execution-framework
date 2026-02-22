"""Tests for the Data API module.

Uses MemoryDatabase and aiohttp TestClient/TestServer
for HTTP endpoint testing.
"""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.data_api.main import DataApiModule
from de_platform.pipeline.auth_middleware import encode_token
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets

JWT_SECRET = "test-data-api-secret-at-least-32-bytes"


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_db_factory(db: MemoryDatabase | None = None) -> tuple[DatabaseFactory, MemoryDatabase]:
    db = db or MemoryDatabase()
    factory = DatabaseFactory({})
    factory.register_instance("events", db)
    return factory, db


async def _setup_module(
    db: MemoryDatabase | None = None,
) -> tuple[DataApiModule, MemoryDatabase]:
    db_factory, db = _make_db_factory(db)
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})

    module = DataApiModule(
        config=config, logger=logger, db_factory=db_factory, lifecycle=lifecycle,
        metrics=NoopMetrics(), secrets=EnvSecrets(overrides={}),
    )
    await module.initialize()
    return module, db


# ── REST endpoint tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_events_returns_list() -> None:
    db = MemoryDatabase()
    module, db = await _setup_module(db=db)
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
async def test_get_executions_returns_list() -> None:
    db = MemoryDatabase()
    module, db = await _setup_module(db=db)
    db.bulk_insert("executions", [
        {"id": "e1", "tenant_id": "t1", "transact_time": "2026-01-15T10:00:00"},
    ])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/executions?tenant_id=t1")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["id"] == "e1"


@pytest.mark.asyncio
async def test_get_transactions_returns_list() -> None:
    db = MemoryDatabase()
    module, db = await _setup_module(db=db)
    db.bulk_insert("transactions", [
        {"id": "tx1", "tenant_id": "t1", "transact_time": "2026-01-15T10:00:00"},
    ])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/transactions?tenant_id=t1")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["id"] == "tx1"


@pytest.mark.asyncio
async def test_events_date_filter() -> None:
    db = MemoryDatabase()
    module, db = await _setup_module(db=db)
    db.bulk_insert("orders", [
        {"id": "o1", "tenant_id": "t1", "transact_time": "2026-01-15T10:00:00"},
        {"id": "o2", "tenant_id": "t1", "transact_time": "2026-01-16T10:00:00"},
    ])

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/orders?tenant_id=t1&date=2026-01-15")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["id"] == "o1"


# ── Auth-enabled tests ───────────────────────────────────────────────────────


async def _setup_module_with_auth(
    db: MemoryDatabase | None = None,
) -> tuple[DataApiModule, MemoryDatabase]:
    db_factory, db = _make_db_factory(db)
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})
    secrets = EnvSecrets(overrides={"JWT_SECRET": JWT_SECRET})

    module = DataApiModule(
        config=config, logger=logger, db_factory=db_factory, lifecycle=lifecycle,
        metrics=NoopMetrics(), secrets=secrets,
    )
    await module.initialize()
    return module, db


def _auth_header(user_id: str, tenant_id: str, role: str) -> dict[str, str]:
    token = encode_token(user_id, tenant_id, role, JWT_SECRET)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_auth_required_when_jwt_secret_set() -> None:
    """Without a token, requests are rejected when JWT_SECRET is set."""
    module, db = await _setup_module_with_auth()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/orders")
        assert resp.status == 401


@pytest.mark.asyncio
async def test_auth_events_tenant_scoped() -> None:
    """Events endpoint respects JWT tenant_id."""
    db = MemoryDatabase()
    module, db = await _setup_module_with_auth(db=db)
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
