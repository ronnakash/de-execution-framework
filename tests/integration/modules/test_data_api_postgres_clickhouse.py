"""Integration tests: DataApiModule queries real ClickHouse.

Verifies that data_api can serve events from ClickHouse through its REST
endpoints.  Alert serving has moved to the alert_manager module.
"""

from __future__ import annotations

import uuid

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.data_api.main import DataApiModule
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets


pytestmark = pytest.mark.integration


async def test_data_api_serves_events_from_clickhouse(clickhouse_db):
    """DataApiModule returns orders inserted directly into ClickHouse."""
    uid = uuid.uuid4().hex[:8]
    order = {
        "id": f"ord-{uid}",
        "tenant_id": "integ-test",
        "event_type": "order",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00Z",
        "symbol": "GOOG",
        "side": "sell",
        "quantity": 50.0,
        "price": 200.0,
        "order_type": "market",
        "currency": "USD",
        "message_id": uuid.uuid4().hex,
        "notional": 10000.0,
        "notional_usd": 10000.0,
        "ingested_at": "2026-01-15T10:00:00+00:00",
        "normalized_at": "2026-01-15T10:00:01+00:00",
        "primary_key": f"integ-test_order_{uid}_2026-01-15",
    }
    clickhouse_db.insert_one("orders", order)

    db_factory = DatabaseFactory({})
    db_factory.register_instance("events", clickhouse_db)
    api = DataApiModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        db_factory=db_factory,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
        secrets=EnvSecrets(overrides={}),
    )
    await api.initialize()
    app = api._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/events/orders?tenant_id=integ-test&limit=200")
        assert resp.status == 200
        body = await resp.json()
        matching = [e for e in body if e["id"] == f"ord-{uid}"]
        assert len(matching) == 1
        assert matching[0]["symbol"] == "GOOG"
