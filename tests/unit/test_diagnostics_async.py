"""Tests for async TestDiagnostics.snapshot() (Phase 1 - WS3).

Verifies that snapshot() works as an async method with both
MemoryDatabase and no database at all.
"""

from __future__ import annotations

import pytest

from tests.helpers.diagnostics import TestDiagnostics


@pytest.mark.asyncio
async def test_snapshot_returns_pipeline_snapshot() -> None:
    diag = TestDiagnostics()
    snap = await diag.snapshot()
    assert snap.timestamp > 0
    assert snap.db_tables == {}
    assert snap.kafka_topics == {}


@pytest.mark.asyncio
async def test_snapshot_with_memory_database() -> None:
    """Verify async snapshot works with MemoryDatabase without raising.

    Note: MemoryDatabase can't parse ``SELECT count(*) ...``, so the count
    won't be accurate. The important thing is that the async path runs
    without RuntimeError (the bug this fixes).
    """
    from de_platform.services.database.memory_database import MemoryDatabase

    db = MemoryDatabase()
    db.connect()
    db.insert_one("alerts", {"alert_id": "a1", "tenant_id": "t1"})

    diag = TestDiagnostics(postgres_db=db, tenant_id="t1")
    snap = await diag.snapshot()
    # MemoryDatabase can't parse count(*), so result is 0 or -1, not -1 from RuntimeError
    assert snap.db_tables.get("postgres.alerts") is not None
    assert snap.db_tables["postgres.alerts"] != -1


@pytest.mark.asyncio
async def test_snapshot_with_memory_queue() -> None:
    from de_platform.services.message_queue.memory_queue import MemoryQueue

    mq = MemoryQueue()
    mq.publish("test_topic", {"key": "val"})

    diag = TestDiagnostics(memory_queue=mq)
    snap = await diag.snapshot()
    assert "test_topic" in snap.kafka_topics
    assert snap.kafka_topics["test_topic"].messages_available == 1


@pytest.mark.asyncio
async def test_snapshot_with_memory_metrics() -> None:
    from de_platform.services.metrics.memory_metrics import MemoryMetrics

    metrics = MemoryMetrics()
    metrics.counter("events_total")

    diag = TestDiagnostics(memory_metrics=metrics)
    snap = await diag.snapshot()
    assert "events_total" in snap.metrics
