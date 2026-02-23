"""Tests for the Data Audit Service."""

from __future__ import annotations

import uuid

from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.data_audit.main import DataAuditModule, _extract_date
from de_platform.pipeline.topics import (
    AUDIT_COUNTS,
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE,
)
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets


def _make_msg(
    event_type: str = "order",
    tenant_id: str = "t1",
    transact_time: str = "2026-01-15T10:00:00+00:00",
) -> dict:
    return {
        "id": uuid.uuid4().hex,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "transact_time": transact_time,
        "message_id": uuid.uuid4().hex,
    }


def _make_audit_count(
    tenant_id: str = "t1",
    source: str = "rest",
    event_type: str = "order",
    received: int = 1,
    timestamp: str = "2026-01-15T10:00:00+00:00",
) -> dict:
    return {
        "tenant_id": tenant_id,
        "source": source,
        "event_type": event_type,
        "received": received,
        "timestamp": timestamp,
    }


async def _setup_module(
    flush_threshold: int = 100,
    flush_interval: int = 999,
) -> tuple[DataAuditModule, MemoryQueue, MemoryDatabase]:
    mq = MemoryQueue()
    db = MemoryDatabase()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({
        "flush-threshold": flush_threshold,
        "flush-interval": flush_interval,
    })
    secrets = EnvSecrets()

    module = DataAuditModule(
        config=config, logger=logger, mq=mq, db=db,
        lifecycle=lifecycle, metrics=NoopMetrics(), secrets=secrets,
    )
    await module.initialize()
    return module, mq, db


# ── Counter accumulation tests ────────────────────────────────────────────


async def test_received_count_from_audit_counts() -> None:
    module, mq, db = await _setup_module()
    mq.publish(AUDIT_COUNTS, _make_audit_count(
        tenant_id="t1", source="rest", event_type="order", received=1,
    ))

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1"
    assert rows[0]["event_type"] == "order"
    assert rows[0]["source"] == "rest"
    assert rows[0]["received_count"] == 1
    assert rows[0]["processed_count"] == 0


async def test_received_count_from_audit_counts_transaction() -> None:
    module, mq, db = await _setup_module()
    mq.publish(AUDIT_COUNTS, _make_audit_count(
        tenant_id="t1", source="kafka", event_type="transaction", received=3,
    ))

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "transaction"
    assert rows[0]["source"] == "kafka"
    assert rows[0]["received_count"] == 3


async def test_processed_count_from_orders_persistence() -> None:
    module, mq, db = await _setup_module()
    msg = _make_msg()
    mq.publish(ORDERS_PERSISTENCE, msg)

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "order"
    assert rows[0]["processed_count"] == 1
    assert rows[0]["received_count"] == 0


async def test_processed_count_from_executions_persistence() -> None:
    module, mq, db = await _setup_module()
    msg = _make_msg()
    mq.publish(EXECUTIONS_PERSISTENCE, msg)

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "execution"
    assert rows[0]["processed_count"] == 1


async def test_processed_count_from_transactions_persistence() -> None:
    module, mq, db = await _setup_module()
    msg = _make_msg()
    mq.publish(TRANSACTIONS_PERSISTENCE, msg)

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "transaction"
    assert rows[0]["processed_count"] == 1


async def test_error_count_from_normalization_errors() -> None:
    module, mq, db = await _setup_module()
    msg = _make_msg(event_type="order")
    mq.publish(NORMALIZATION_ERRORS, msg)

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["error_count"] == 1


async def test_duplicate_count_from_duplicates_topic() -> None:
    module, mq, db = await _setup_module()
    msg = _make_msg(event_type="execution")
    mq.publish(DUPLICATES, msg)

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["duplicate_count"] == 1
    assert rows[0]["event_type"] == "execution"


# ── Multi-tenant isolation ────────────────────────────────────────────────


async def test_multi_tenant_isolation() -> None:
    module, mq, db = await _setup_module()
    mq.publish(AUDIT_COUNTS, _make_audit_count(tenant_id="t1", event_type="order"))
    mq.publish(AUDIT_COUNTS, _make_audit_count(tenant_id="t2", event_type="order"))

    module._consume_all_topics()
    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 2
    tenants = {r["tenant_id"] for r in rows}
    assert tenants == {"t1", "t2"}


# ── Date extraction ──────────────────────────────────────────────────────


def test_extract_date_from_transact_time() -> None:
    msg = {"transact_time": "2026-03-15T10:30:00+00:00"}
    assert _extract_date(msg) == "2026-03-15"


def test_extract_date_from_timestamp() -> None:
    msg = {"timestamp": "2026-03-15T10:30:00+00:00"}
    assert _extract_date(msg) == "2026-03-15"


def test_extract_date_fallback_to_today() -> None:
    from datetime import date

    msg = {}
    assert _extract_date(msg) == date.today().isoformat()


# ── Upsert across flushes ────────────────────────────────────────────────


async def test_counters_accumulate_across_flushes() -> None:
    module, mq, db = await _setup_module()

    # First batch
    mq.publish(AUDIT_COUNTS, _make_audit_count(event_type="order"))
    module._consume_all_topics()
    await module._flush_counters()

    # Second batch (same tenant/date/event_type/source)
    mq.publish(AUDIT_COUNTS, _make_audit_count(event_type="order"))
    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["received_count"] == 2


# ── Flush threshold trigger ──────────────────────────────────────────────


async def test_flush_triggered_by_threshold() -> None:
    module, mq, db = await _setup_module(flush_threshold=3, flush_interval=999)

    for _ in range(3):
        mq.publish(AUDIT_COUNTS, _make_audit_count(event_type="order"))
        module._consume_all_topics()

    # _maybe_flush should trigger because threshold (3) is reached
    await module._maybe_flush()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["received_count"] == 3


# ── Source dimension tests ────────────────────────────────────────────────


async def test_source_column_in_daily_audit_row() -> None:
    module, mq, db = await _setup_module()
    mq.publish(AUDIT_COUNTS, _make_audit_count(source="kafka"))

    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 1
    assert rows[0]["source"] == "kafka"


async def test_different_sources_create_separate_rows() -> None:
    module, mq, db = await _setup_module()
    mq.publish(AUDIT_COUNTS, _make_audit_count(source="rest"))
    mq.publish(AUDIT_COUNTS, _make_audit_count(source="kafka"))

    module._consume_all_topics()
    module._consume_all_topics()
    await module._flush_counters()

    rows = db.fetch_all("SELECT * FROM daily_audit")
    assert len(rows) == 2
    sources = {r["source"] for r in rows}
    assert sources == {"rest", "kafka"}


# ── REST endpoint tests ──────────────────────────────────────────────────


async def test_rest_get_daily() -> None:
    module, mq, db = await _setup_module()

    mq.publish(AUDIT_COUNTS, _make_audit_count(tenant_id="t1", event_type="order"))
    module._consume_all_topics()
    await module._flush_counters()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/daily?tenant_id=t1")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["tenant_id"] == "t1"


async def test_rest_get_daily_filtered_by_date() -> None:
    module, mq, db = await _setup_module()

    mq.publish(AUDIT_COUNTS, _make_audit_count(
        tenant_id="t1", event_type="order",
        timestamp="2026-01-15T10:00:00+00:00",
    ))
    module._consume_all_topics()
    await module._flush_counters()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/daily?tenant_id=t1&date=2026-01-15")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1

        # Wrong date returns empty
        resp2 = await client.get("/api/v1/audit/daily?tenant_id=t1&date=2026-01-16")
        data2 = await resp2.json()
        assert len(data2) == 0


async def test_rest_get_daily_filter_by_source() -> None:
    module, mq, db = await _setup_module()

    mq.publish(AUDIT_COUNTS, _make_audit_count(source="rest"))
    mq.publish(AUDIT_COUNTS, _make_audit_count(source="kafka"))
    module._consume_all_topics()
    module._consume_all_topics()
    await module._flush_counters()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/daily?source=rest")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["source"] == "rest"


async def test_rest_get_summary() -> None:
    module, mq, db = await _setup_module()

    mq.publish(AUDIT_COUNTS, _make_audit_count(tenant_id="t1", event_type="order"))
    mq.publish(ORDERS_PERSISTENCE, _make_msg(tenant_id="t1"))
    module._consume_all_topics()
    await module._flush_counters()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/summary?tenant_id=t1")
        assert resp.status == 200
        data = await resp.json()
        assert data["total_received"] == 1
        assert data["total_processed"] == 1
        assert data["total_errors"] == 0
        assert data["total_duplicates"] == 0


async def test_rest_get_summary_includes_by_source() -> None:
    module, mq, db = await _setup_module()

    mq.publish(AUDIT_COUNTS, _make_audit_count(source="rest", received=3))
    mq.publish(AUDIT_COUNTS, _make_audit_count(source="kafka", received=2))
    module._consume_all_topics()
    module._consume_all_topics()
    await module._flush_counters()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/summary")
        assert resp.status == 200
        data = await resp.json()
        assert "by_source" in data
        assert data["by_source"]["rest"]["received"] == 3
        assert data["by_source"]["kafka"]["received"] == 2


async def test_rest_list_files_empty() -> None:
    module, mq, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/files")
        assert resp.status == 200
        data = await resp.json()
        assert data == []


async def test_rest_get_file_not_found() -> None:
    module, mq, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/audit/files/nonexistent")
        assert resp.status == 404


async def test_flush_single_fetch_all() -> None:
    """Verify that flush fetches the table only once (not per key)."""
    module, mq, db = await _setup_module()

    mq.publish(AUDIT_COUNTS, _make_audit_count(tenant_id="t1", source="rest"))
    mq.publish(AUDIT_COUNTS, _make_audit_count(tenant_id="t2", source="kafka"))
    module._consume_all_topics()
    module._consume_all_topics()

    call_count = 0
    original = db.fetch_all_async

    async def counting_fetch(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return await original(*args, **kwargs)

    db.fetch_all_async = counting_fetch
    await module._flush_counters()
    # Should be exactly 1 fetch_all_async call, not 2 (one per key)
    assert call_count == 1
