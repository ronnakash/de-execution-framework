"""Tests for the Persistence module.

Buffer unit tests are standalone. Module integration tests use
MemoryQueue + MemoryDatabase + MemoryFileSystem.
"""

from __future__ import annotations

import json
import time
import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.persistence.buffer import BufferKey, TenantBuffer
from de_platform.modules.persistence.main import PersistenceModule
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_order(tenant_id: str = "t1") -> dict:
    return {
        "event_type": "order",
        "id": uuid.uuid4().hex,
        "tenant_id": tenant_id,
        "status": "new",
        "primary_key": f"{tenant_id}_order_x_2026-01-15",
        "notional": 1_000.0,
        "notional_usd": 1_000.0,
        "transact_time": "2026-01-15T10:00:00+00:00",
    }


async def _setup() -> tuple[PersistenceModule, MemoryQueue, MemoryDatabase, MemoryFileSystem]:
    mq = MemoryQueue()
    db = MemoryDatabase()
    fs = MemoryFileSystem()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"flush-interval": 60, "flush-threshold": 100_000})

    module = PersistenceModule(
        config=config, logger=logger, mq=mq, db=db, fs=fs, lifecycle=lifecycle,
        metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, mq, db, fs


# ── Buffer unit tests ─────────────────────────────────────────────────────────


def test_events_buffered_by_tenant_and_table() -> None:
    buf = TenantBuffer(flush_interval=60, flush_threshold=100)
    key1 = BufferKey(tenant_id="t1", table="orders")
    key2 = BufferKey(tenant_id="t2", table="orders")

    buf.append(key1, {"id": "1"})
    buf.append(key2, {"id": "2"})
    buf.append(key1, {"id": "3"})

    assert len(buf.drain(key1)) == 2
    assert len(buf.drain(key2)) == 1


def test_flush_on_threshold() -> None:
    buf = TenantBuffer(flush_interval=3600, flush_threshold=5)
    key = BufferKey(tenant_id="t1", table="orders")

    for i in range(4):
        buf.append(key, {"id": str(i)})
        assert not buf.should_flush(key)

    buf.append(key, {"id": "4"})
    assert buf.should_flush(key)


def test_flush_on_interval() -> None:
    # flush_interval=0 means any elapsed time triggers a flush
    buf = TenantBuffer(flush_interval=0, flush_threshold=100_000)
    key = BufferKey(tenant_id="t1", table="orders")
    buf.append(key, {"id": "1"})
    # Time has advanced by at least 0s — should_flush must be True
    assert buf.should_flush(key)


def test_drain_clears_buffer() -> None:
    buf = TenantBuffer(flush_interval=60, flush_threshold=100)
    key = BufferKey(tenant_id="t1", table="orders")
    buf.append(key, {"id": "1"})
    drained = buf.drain(key)
    assert len(drained) == 1
    assert buf.drain(key) == []


def test_drain_all_returns_all_non_empty() -> None:
    buf = TenantBuffer(flush_interval=60, flush_threshold=100)
    key1 = BufferKey(tenant_id="t1", table="orders")
    key2 = BufferKey(tenant_id="t2", table="orders")
    buf.append(key1, {"id": "1"})
    buf.append(key2, {"id": "2"})

    result = buf.drain_all()
    assert len(result) == 2
    # After drain_all, all_keys should be empty
    assert buf.all_keys() == []


# ── Module integration tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_flush_writes_to_db_and_fs() -> None:
    module, mq, db, fs = await _setup()
    msg = _make_order()
    module.buffer.append(BufferKey("t1", "orders"), msg)
    module._flush(BufferKey("t1", "orders"))

    rows = db.fetch_all("SELECT * FROM orders")
    assert len(rows) == 1

    files = fs.list("t1/orders/")
    assert len(files) == 1
    loaded = json.loads(fs.read(files[0]).decode())
    assert loaded["id"] == msg["id"]


@pytest.mark.asyncio
async def test_tenant_isolation() -> None:
    module, mq, db, fs = await _setup()
    msg_t1 = _make_order(tenant_id="t1")
    msg_t2 = _make_order(tenant_id="t2")

    module.buffer.append(BufferKey("t1", "orders"), msg_t1)
    module.buffer.append(BufferKey("t2", "orders"), msg_t2)
    module._flush(BufferKey("t1", "orders"))
    module._flush(BufferKey("t2", "orders"))

    assert len(fs.list("t1/orders/")) == 1
    assert len(fs.list("t2/orders/")) == 1


@pytest.mark.asyncio
async def test_shutdown_flushes_all_remaining() -> None:
    module, mq, db, fs = await _setup()
    msg = _make_order()
    module.buffer.append(BufferKey("t1", "orders"), msg)
    module._flush_all()

    rows = db.fetch_all("SELECT * FROM orders")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_fs_paths_separated_by_tenant() -> None:
    module, mq, db, fs = await _setup()
    module.buffer.append(BufferKey("alice", "orders"), _make_order("alice"))
    module.buffer.append(BufferKey("bob", "orders"), _make_order("bob"))
    module._flush(BufferKey("alice", "orders"))
    module._flush(BufferKey("bob", "orders"))

    alice_files = fs.list("alice/orders/")
    bob_files = fs.list("bob/orders/")
    assert len(alice_files) == 1
    assert len(bob_files) == 1
    assert all(f.startswith("alice/") for f in alice_files)
    assert all(f.startswith("bob/") for f in bob_files)
