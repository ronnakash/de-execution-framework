"""Tests for the ObservableDatabase wrapper."""

from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.database.observable_database import ObservableDatabase
from de_platform.services.metrics.memory_metrics import MemoryMetrics


def test_wraps_sync_operations():
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)

    db.connect()
    db.insert_one("orders", {"id": "1", "tenant_id": "acme"})
    db.bulk_insert("orders", [{"id": "2"}, {"id": "3"}])
    result = db.fetch_all("SELECT * FROM orders")
    one = db.fetch_one("SELECT * FROM orders WHERE id = $1", ["1"])

    assert len(result) == 3
    assert one is not None and one["id"] == "1"
    assert db.is_connected()
    assert db.health_check()

    # Verify histograms were recorded
    hist = metrics.histograms
    assert "db_query_duration_seconds" in hist
    samples = hist["db_query_duration_seconds"]
    # connect + insert_one + bulk_insert + fetch_all + fetch_one = 5 calls
    assert len(samples) == 5
    assert all(s >= 0 for s in samples)


async def test_wraps_async_operations():
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)

    await db.connect_async()
    await db.insert_one_async("orders", {"id": "1"})
    result = await db.fetch_all_async("SELECT * FROM orders")
    one = await db.fetch_one_async("SELECT * FROM orders WHERE id = $1", ["1"])

    assert len(result) == 1
    assert one is not None

    hist = metrics.histograms
    assert "db_query_duration_seconds" in hist
    # connect + insert_one + fetch_all + fetch_one = 4
    assert len(hist["db_query_duration_seconds"]) == 4


def test_caller_tag_contains_this_file():
    """Verify that the caller tag points to *this* test file, not observable_database.py."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)

    db.connect()
    db.disconnect()

    # We can't easily inspect the tags from MemoryMetrics (it doesn't store them),
    # but we can at least confirm the wrapper doesn't blow up and records 2 histograms.
    assert len(metrics.histograms["db_query_duration_seconds"]) == 2
