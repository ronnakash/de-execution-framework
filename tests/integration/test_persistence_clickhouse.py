"""Integration tests: PersistenceModule flushes rows to real ClickHouse.

Verifies that the persistence module can buffer events and flush them
to ClickHouse tables (orders, executions, transactions, duplicates,
normalization_errors) using the real ClickHouseDatabase.
"""

from __future__ import annotations

import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.persistence.buffer import BufferKey
from de_platform.modules.persistence.main import PersistenceModule
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager


pytestmark = pytest.mark.real_infra


def _make_order(tenant_id: str = "integ-test") -> dict:
    uid = uuid.uuid4().hex[:8]
    return {
        "id": f"ord-{uid}",
        "tenant_id": tenant_id,
        "event_type": "order",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": 100.0,
        "price": 150.0,
        "order_type": "limit",
        "currency": "USD",
        "message_id": uuid.uuid4().hex,
        "notional": 15000.0,
        "notional_usd": 15000.0,
        "ingested_at": "2026-01-15T10:00:00+00:00",
        "normalized_at": "2026-01-15T10:00:01+00:00",
        "primary_key": f"integ-test_order_{uid}_2026-01-15",
    }


def _make_error(tenant_id: str = "integ-test") -> dict:
    return {
        "event_type": "order",
        "tenant_id": tenant_id,
        "errors": [{"field": "price", "message": "must be positive"}],
        "raw_data": {"id": "bad-1"},
        "created_at": "2026-01-15T10:00:00+00:00",
    }


async def test_persistence_flush_orders_to_clickhouse(clickhouse_db, kafka_mq):
    """PersistenceModule flushes buffered orders to ClickHouse."""
    from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem

    module = PersistenceModule(
        config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
        logger=LoggerFactory(default_impl="memory"),
        mq=kafka_mq,
        db=clickhouse_db,
        fs=MemoryFileSystem(),
        lifecycle=LifecycleManager(),
    )
    await module.initialize()

    # Manually buffer and flush
    orders = [_make_order() for _ in range(5)]
    key = BufferKey(tenant_id="integ-test", table="orders")
    for order in orders:
        module.buffer.append(key, order)
    module._flush(key)

    rows = clickhouse_db.fetch_all("SELECT * FROM orders")
    assert len(rows) >= 5
    for row in rows:
        if row["tenant_id"] == "integ-test":
            assert "primary_key" in row
            assert row["symbol"] == "AAPL"


async def test_persistence_flush_errors_to_clickhouse(clickhouse_db, kafka_mq):
    """PersistenceModule flushes normalization errors (with JSON fields) to ClickHouse."""
    from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem

    module = PersistenceModule(
        config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
        logger=LoggerFactory(default_impl="memory"),
        mq=kafka_mq,
        db=clickhouse_db,
        fs=MemoryFileSystem(),
        lifecycle=LifecycleManager(),
    )
    await module.initialize()

    errors = [_make_error() for _ in range(3)]
    key = BufferKey(tenant_id="integ-test", table="normalization_errors")
    for err in errors:
        module.buffer.append(key, err)
    module._flush(key)

    rows = clickhouse_db.fetch_all("SELECT * FROM normalization_errors")
    integ_rows = [r for r in rows if r.get("tenant_id") == "integ-test"]
    assert len(integ_rows) >= 3
