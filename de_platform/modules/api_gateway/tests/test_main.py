"""Tests for the API Gateway module using memory implementations."""

from __future__ import annotations

import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.api_gateway.main import ApiGatewayModule
from de_platform.services.cache.interface import CacheInterface
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.memory_logger import MemoryLogger


def _make_module(
    db: MemoryDatabase | None = None,
    cache: MemoryCache | None = None,
) -> ApiGatewayModule:
    config = ModuleConfig({"port": 0})
    logger = LoggerFactory(default_impl="memory")
    if db is None:
        db = MemoryDatabase()
    if cache is None:
        cache = MemoryCache()
    lifecycle = LifecycleManager()
    module = ApiGatewayModule(
        config=config, logger=logger, db=db, cache=cache, lifecycle=lifecycle
    )
    module.log = logger.create()
    module.port = 0
    return module


@pytest.fixture
async def client():
    module = _make_module()
    module.db.connect()
    app = module._create_app()
    # Stash module on app for test access
    app["_module"] = module
    async with TestClient(TestServer(app)) as c:
        yield c


@pytest.fixture
async def seeded_client():
    db = MemoryDatabase()
    db.connect()
    db.bulk_insert("events", [
        {"id": "1", "event_type": "click", "payload": "{}"},
        {"id": "2", "event_type": "view", "payload": "{}"},
    ])
    module = _make_module(db=db)
    module.db = db  # use the pre-seeded one
    app = module._create_app()
    app["_module"] = module
    async with TestClient(TestServer(app)) as c:
        yield c


@pytest.mark.asyncio
async def test_get_events_returns_list(seeded_client: TestClient) -> None:
    resp = await seeded_client.get("/events")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)
    assert len(data) == 2


@pytest.mark.asyncio
async def test_get_events_cache_hit(seeded_client: TestClient) -> None:
    module = seeded_client.app["_module"]  # type: ignore[index]

    # Pre-populate cache
    module.cache.set("events:list", [{"id": "cached", "event_type": "test"}])

    resp = await seeded_client.get("/events")
    assert resp.status == 200
    data = await resp.json()
    # Should return cached data, not DB data
    assert len(data) == 1
    assert data[0]["id"] == "cached"


@pytest.mark.asyncio
async def test_get_events_cache_miss_fills_cache(seeded_client: TestClient) -> None:
    module = seeded_client.app["_module"]  # type: ignore[index]

    assert module.cache.get("events:list") is None
    resp = await seeded_client.get("/events")
    assert resp.status == 200
    # Cache should now be populated
    cached = module.cache.get("events:list")
    assert cached is not None
    assert len(cached) == 2


@pytest.mark.asyncio
async def test_get_event_by_id(seeded_client: TestClient) -> None:
    resp = await seeded_client.get("/events/1")
    assert resp.status == 200
    data = await resp.json()
    assert data["id"] == "1"


@pytest.mark.asyncio
async def test_get_event_not_found(seeded_client: TestClient) -> None:
    resp = await seeded_client.get("/events/999")
    assert resp.status == 404


@pytest.mark.asyncio
async def test_correlation_id_propagated(client: TestClient) -> None:
    resp = await client.get("/status", headers={"X-Correlation-ID": "test-123"})
    assert resp.status == 200
    assert resp.headers["X-Correlation-ID"] == "test-123"


@pytest.mark.asyncio
async def test_correlation_id_generated(client: TestClient) -> None:
    resp = await client.get("/status")
    assert resp.status == 200
    assert "X-Correlation-ID" in resp.headers
    assert len(resp.headers["X-Correlation-ID"]) > 0


@pytest.mark.asyncio
async def test_status_endpoint(client: TestClient) -> None:
    resp = await client.get("/status")
    assert resp.status == 200
    data = await resp.json()
    assert data["module"] == "api_gateway"
    assert "uptime_seconds" in data


@pytest.mark.asyncio
async def test_logging_middleware(seeded_client: TestClient) -> None:
    module = seeded_client.app["_module"]  # type: ignore[index]
    assert isinstance(module.log, MemoryLogger)

    await seeded_client.get("/events")
    # Should have logged the request
    assert any("Request handled" in m for m in module.log.messages)
