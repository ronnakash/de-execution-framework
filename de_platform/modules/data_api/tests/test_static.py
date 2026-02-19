"""Verify that the Data API serves static UI files."""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.data_api.main import DataApiModule
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue


async def _make_app() -> DataApiModule:
    db = MemoryDatabase()
    db_factory = DatabaseFactory({})
    db_factory.register_instance("events", db)
    db_factory.register_instance("alerts", db)
    module = DataApiModule(
        config=ModuleConfig({"port": 0}),
        logger=LoggerFactory(default_impl="memory"),
        mq=MemoryQueue(),
        db_factory=db_factory,
        lifecycle=LifecycleManager(),
    )
    await module.initialize()
    return module


@pytest.mark.asyncio
async def test_static_index_served() -> None:
    module = await _make_app()
    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ui/index.html")
        assert resp.status == 200
        assert "text/html" in resp.content_type
        body = await resp.text()
        assert "Fraud Detection Dashboard" in body


@pytest.mark.asyncio
async def test_static_app_js_served() -> None:
    module = await _make_app()
    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ui/app.js")
        assert resp.status == 200
        assert "javascript" in resp.content_type


@pytest.mark.asyncio
async def test_static_style_css_served() -> None:
    module = await _make_app()
    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ui/style.css")
        assert resp.status == 200
        assert "css" in resp.content_type
