"""Integration tests: DataApiModule queries real Postgres and ClickHouse.

Verifies that data_api can serve alerts from Postgres and events from
ClickHouse through its REST endpoints.
"""

from __future__ import annotations

import json
import uuid

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.data_api.main import DataApiModule
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets


pytestmark = pytest.mark.integration


async def test_data_api_serves_alerts_from_postgres(alerts_db, clickhouse_db):
    """DataApiModule returns alerts inserted directly into Postgres."""
    # Insert an alert directly into Postgres
    alert = {
        "alert_id": uuid.uuid4().hex,
        "tenant_id": "integ-test",
        "event_type": "order",
        "event_id": "ord-1",
        "message_id": uuid.uuid4().hex,
        "algorithm": "large_notional",
        "severity": "high",
        "description": "Test alert",
        "details": json.dumps({"notional_usd": 1500000}),
        "created_at": "2026-01-15T10:00:00",
    }
    # Convert created_at to datetime for Postgres
    from datetime import datetime
    db_row = dict(alert)
    db_row["created_at"] = datetime.fromisoformat(db_row["created_at"])
    await alerts_db.insert_one_async("alerts", db_row)

    # Create DataApi and query
    db_factory = DatabaseFactory({})
    db_factory.register_instance("events", clickhouse_db)
    db_factory.register_instance("alerts", alerts_db)
    api = DataApiModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=MemoryQueue(),
        db_factory=db_factory,
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
        secrets=EnvSecrets(overrides={}),
    )
    await api.initialize()
    app = api._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/alerts?tenant_id=integ-test")
        assert resp.status == 200
        body = await resp.json()
        assert len(body) >= 1
        matching = [a for a in body if a["alert_id"] == alert["alert_id"]]
        assert len(matching) == 1
        assert matching[0]["algorithm"] == "large_notional"


async def test_data_api_serves_events_from_clickhouse(alerts_db, clickhouse_db):
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
    db_factory.register_instance("alerts", alerts_db)
    api = DataApiModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=MemoryQueue(),
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
