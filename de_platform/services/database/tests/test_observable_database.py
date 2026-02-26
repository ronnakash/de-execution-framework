"""Tests for the ObservableDatabase wrapper."""

import pytest

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


def test_error_counter_on_sync_exception():
    """When the inner DB raises, an error counter is emitted and the exception propagates."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    # Patch inner to raise on execute
    def failing_execute(query, params=None):
        raise RuntimeError("connection lost")

    inner.execute = failing_execute

    with pytest.raises(RuntimeError, match="connection lost"):
        db.execute("SELECT 1")

    # Error counter should be recorded
    assert "db_query_errors_total" in metrics.counters
    assert metrics.counters["db_query_errors_total"] == 1

    # No histogram should be recorded for the failed call
    # (connect recorded 1, the failed execute should not add another)
    assert len(metrics.histograms["db_query_duration_seconds"]) == 1  # connect only


async def test_error_counter_on_async_exception():
    """Async methods also emit error counters on failure."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    await db.connect_async()

    def failing_fetch(query, params=None):
        raise RuntimeError("timeout")

    inner.fetch_one = failing_fetch

    with pytest.raises(RuntimeError, match="timeout"):
        await db.fetch_one_async("SELECT 1")

    assert "db_query_errors_total" in metrics.counters
    assert metrics.counters["db_query_errors_total"] == 1


def test_db_prefix_in_histogram_tags():
    """The db_prefix is included in histogram tags."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics, db_prefix="warehouse")
    db.connect()

    # Inspect the tag-aware histogram_calls list
    assert len(metrics.histogram_calls) == 1
    name, value, tags = metrics.histogram_calls[0]
    assert name == "db_query_duration_seconds"
    assert tags is not None
    assert tags["db_prefix"] == "warehouse"
    assert tags["operation"] == "connect"


def test_db_prefix_in_error_tags():
    """The db_prefix is included in error counter tags."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics, db_prefix="alerts")
    db.connect()

    inner.execute = lambda q, p=None: (_ for _ in ()).throw(RuntimeError("fail"))

    with pytest.raises(RuntimeError):
        db.execute("SELECT 1")

    assert len(metrics.counter_calls) == 1
    name, value, tags = metrics.counter_calls[0]
    assert name == "db_query_errors_total"
    assert tags["db_prefix"] == "alerts"
    assert tags["operation"] == "execute"


def test_db_prefix_defaults_to_default():
    """When db_prefix is not specified, it defaults to 'default'."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    _, _, tags = metrics.histogram_calls[0]
    assert tags["db_prefix"] == "default"


def test_caller_tag_excludes_observable_module():
    """The caller tag does not resolve to the observable_database.py wrapper itself."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    _, _, tags = metrics.histogram_calls[0]
    caller = tags["caller"]
    assert caller != "unknown"
    # Must not point to the wrapper module's own source
    assert "de_platform/services/database/observable_database.py" not in caller


@pytest.mark.parametrize("method,args", [
    ("execute", ("INSERT INTO t VALUES (1)",)),
    ("fetch_one", ("SELECT * FROM t",)),
    ("fetch_all", ("SELECT * FROM t",)),
    ("insert_one", ("t", {"id": "1"})),
    ("bulk_insert", ("t", [{"id": "1"}])),
])
def test_all_sync_methods_record_errors(method, args):
    """Every sync data method emits an error counter on failure."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    setattr(inner, method, lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("fail")))

    with pytest.raises(RuntimeError):
        getattr(db, method)(*args)

    assert metrics.counters.get("db_query_errors_total", 0) >= 1


def test_factory_wraps_with_observable_when_metrics_provided():
    """DatabaseFactory.get() returns an ObservableDatabase when metrics are provided."""
    from de_platform.services.database.factory import DatabaseFactory, DbConfig

    configs = {"warehouse": DbConfig(impl_cls=MemoryDatabase)}
    metrics = MemoryMetrics()
    factory = DatabaseFactory(configs, metrics=metrics)

    db = factory.get("warehouse")
    assert isinstance(db, ObservableDatabase)

    # Verify the prefix is set correctly
    db.connect()
    _, _, tags = metrics.histogram_calls[0]
    assert tags["db_prefix"] == "warehouse"


def test_factory_returns_raw_when_no_metrics():
    """DatabaseFactory.get() returns a raw instance when metrics are None."""
    from de_platform.services.database.factory import DatabaseFactory, DbConfig

    configs = {"default": DbConfig(impl_cls=MemoryDatabase)}
    factory = DatabaseFactory(configs)  # no metrics

    db = factory.get("default")
    assert isinstance(db, MemoryDatabase)
    assert not isinstance(db, ObservableDatabase)
