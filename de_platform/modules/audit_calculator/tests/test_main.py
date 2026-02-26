"""Tests for the Audit Calculator job module (Phase 5)."""

from __future__ import annotations

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.audit_calculator.main import AuditCalculatorModule
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics


async def _setup(
    config_overrides: dict | None = None,
    received_rows: list[dict] | None = None,
) -> tuple[AuditCalculatorModule, MemoryDatabase]:
    db = MemoryDatabase()
    db.connect()
    logger = LoggerFactory(default_impl="memory")

    cfg = {}
    if config_overrides:
        cfg.update(config_overrides)

    if received_rows:
        for row in received_rows:
            db.insert_one("audit_received", row)

    module = AuditCalculatorModule(
        config=ModuleConfig(cfg), logger=logger,
        db=db, metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, db


@pytest.mark.asyncio
async def test_empty_audit_received() -> None:
    """No received rows -> exit 0, no audit_daily rows."""
    module, db = await _setup()
    rc = await module.execute()
    assert rc == 0
    rows = db.fetch_all("SELECT * FROM audit_daily")
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_aggregate_received_creates_daily_entry() -> None:
    """Received rows are aggregated into audit_daily."""
    received = [
        {
            "tenant_id": "t1",
            "event_type": "order",
            "ingestion_method": "rest",
            "count": 10,
            "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
        {
            "tenant_id": "t1",
            "event_type": "order",
            "ingestion_method": "rest",
            "count": 5,
            "window_start": "2026-01-15T10:00:05Z",
            "window_end": "2026-01-15T10:00:10Z",
        },
    ]
    module, db = await _setup(received_rows=received)
    rc = await module.execute()
    assert rc == 0

    rows = db.fetch_all("SELECT * FROM audit_daily")
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1"
    assert rows[0]["received_count"] == 15  # 10 + 5


@pytest.mark.asyncio
async def test_aggregate_received_by_tenant() -> None:
    """Different tenants create separate audit_daily entries."""
    received = [
        {
            "tenant_id": "t1", "event_type": "order", "ingestion_method": "rest",
            "count": 10, "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
        {
            "tenant_id": "t2", "event_type": "order", "ingestion_method": "rest",
            "count": 20, "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
    ]
    module, db = await _setup(received_rows=received)
    rc = await module.execute()
    assert rc == 0

    rows = db.fetch_all("SELECT * FROM audit_daily")
    assert len(rows) == 2
    t1_row = next(r for r in rows if r["tenant_id"] == "t1")
    t2_row = next(r for r in rows if r["tenant_id"] == "t2")
    assert t1_row["received_count"] == 10
    assert t2_row["received_count"] == 20


@pytest.mark.asyncio
async def test_tenant_id_filter() -> None:
    """When tenant-id is specified, only that tenant's data is calculated."""
    received = [
        {
            "tenant_id": "t1", "event_type": "order", "ingestion_method": "rest",
            "count": 10, "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
        {
            "tenant_id": "t2", "event_type": "order", "ingestion_method": "rest",
            "count": 20, "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
    ]
    module, db = await _setup(
        config_overrides={"tenant-id": "t1"},
        received_rows=received,
    )
    rc = await module.execute()
    assert rc == 0

    rows = db.fetch_all("SELECT * FROM audit_daily")
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "t1"


@pytest.mark.asyncio
async def test_separate_by_ingestion_method() -> None:
    """Different ingestion methods create separate audit_daily entries."""
    received = [
        {
            "tenant_id": "t1", "event_type": "order", "ingestion_method": "rest",
            "count": 10, "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
        {
            "tenant_id": "t1", "event_type": "order", "ingestion_method": "kafka",
            "count": 5, "window_start": "2026-01-15T10:00:00Z",
            "window_end": "2026-01-15T10:00:05Z",
        },
    ]
    module, db = await _setup(received_rows=received)
    rc = await module.execute()
    assert rc == 0

    rows = db.fetch_all("SELECT * FROM audit_daily")
    assert len(rows) == 2
