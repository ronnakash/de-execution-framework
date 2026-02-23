# Phase 11: Database Observability

Enhance the existing `ObservableDatabase` wrapper with error counting and ensure it is properly wired into the DI system with database-instance-level granularity.

---

## Current State

`de_platform/services/database/observable_database.py` already exists and wraps all `DatabaseInterface` methods with timing histograms. It records a `db_query_duration_seconds` histogram with `{operation, caller}` tags on every successful call. The `_caller_tag()` helper walks the stack to find the first frame outside the database package, producing tags like `de_platform/modules/normalizer/main.py:47`.

### What works today

- All sync methods (`connect`, `disconnect`, `execute`, `fetch_one`, `fetch_all`, `insert_one`, `bulk_insert`) record duration histograms after successful calls.
- All async methods (`connect_async`, `disconnect_async`, `execute_async`, `fetch_one_async`, `fetch_all_async`, `insert_one_async`, `bulk_insert_async`) record duration histograms after successful calls.
- The CLI runner (`de_platform/cli/runner.py`, `_build_container()` step 9) wraps the singleton `DatabaseInterface` in `ObservableDatabase` using the resolved `MetricsInterface`.
- Basic unit tests exist at `de_platform/services/database/tests/test_observable_database.py` covering sync ops, async ops, and a basic caller-tag smoke test.

### What is missing

1. **Error counter**: No `db_query_errors_total` counter is emitted on exceptions. If a query throws, the exception propagates without recording any metric. This means database errors are invisible in dashboards — you only see successful query latencies.

2. **`db_prefix` tag**: The `_record()` method does not include the database prefix (e.g., `warehouse`, `alerts`, `events`, `default`) in metric tags. When multiple named database instances are used (via `--db warehouse=postgres --db alerts=postgres`), all queries appear under the same metric series with no way to distinguish which database instance they target.

3. **`DatabaseFactory` does not auto-wrap**: The factory's `get()` method returns raw database instances. Only the singleton `DatabaseInterface` registered in the DI container gets wrapped (step 9 of `_build_container`). If a module uses `DatabaseFactory.get("events")` directly, that instance is unwrapped — no timing histograms, no error counters.

4. **`MemoryMetrics` discards tags**: The current `MemoryMetrics` implementation stores histogram values in `dict[str, list[float]]` and counter values in `dict[str, float]`, both keyed only by metric name. Tags are accepted as parameters but silently discarded. This makes it impossible to write unit tests that assert on specific tag values (e.g., verifying that `db_prefix="warehouse"` appears in the metric). The existing test file has a comment acknowledging this limitation.

5. **Incomplete test coverage**: The existing tests verify that histograms are recorded on success, but do not test error paths, tag correctness, or the `db_prefix` parameter (which does not yet exist). There are no tests for the factory wrapping behavior.

---

## Implementation Details

### Step 1: Enhance `MemoryMetrics` to store tags

**File:** `de_platform/services/metrics/memory_metrics.py`

Add parallel data structures that retain tag information alongside the existing tag-free structures (to avoid breaking any existing test assertions that rely on the current shape).

```python
class MemoryMetrics(MetricsInterface):
    def __init__(self) -> None:
        # Existing (tag-free) structures — keep for backward compatibility
        self.counters: dict[str, float] = {}
        self.gauges: dict[str, float] = {}
        self.histograms: dict[str, list[float]] = {}

        # New: tag-aware structures for detailed assertions
        self.counter_calls: list[tuple[str, float, dict[str, str] | None]] = []
        self.histogram_calls: list[tuple[str, float, dict[str, str] | None]] = []

    def counter(self, name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        self.counters[name] = self.counters.get(name, 0) + value
        self.counter_calls.append((name, value, tags))

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        self.gauges[name] = value

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        if name not in self.histograms:
            self.histograms[name] = []
        self.histograms[name].append(value)
        self.histogram_calls.append((name, value, tags))
```

**Why add new lists instead of changing the existing dicts?** Many existing unit tests across the codebase assert against `metrics.counters["events_received_total"]` or `metrics.histograms["db_query_duration_seconds"]`. Changing the shape of those structures would require updating dozens of tests. The new `counter_calls` and `histogram_calls` lists are additive — existing tests continue to work unchanged, and new tests can inspect tags.

**Verification:** Run `make test` to confirm no existing tests break.

### Step 2: Add `db_prefix` parameter and error counter to `ObservableDatabase`

**File:** `de_platform/services/database/observable_database.py`

#### 2a. Constructor change

Add a `db_prefix` parameter with a default of `"default"`:

```python
_ERROR_METRIC = "db_query_errors_total"

class ObservableDatabase(DatabaseInterface):
    def __init__(
        self, inner: DatabaseInterface, metrics: MetricsInterface, db_prefix: str = "default"
    ) -> None:
        self._inner = inner
        self._metrics = metrics
        self._db_prefix = db_prefix
```

The default value of `"default"` means existing call sites that do not pass `db_prefix` (including the step-9 wrapping in `_build_container`) continue to work without changes.

#### 2b. Update `_record()` to include `db_prefix`

```python
def _record(self, operation: str, elapsed: float, caller: str) -> None:
    self._metrics.histogram(
        _METRIC,
        elapsed,
        tags={"operation": operation, "caller": caller, "db_prefix": self._db_prefix},
    )
```

#### 2c. Add `_record_error()` helper

```python
def _record_error(self, operation: str, caller: str) -> None:
    self._metrics.counter(
        _ERROR_METRIC,
        tags={"operation": operation, "caller": caller, "db_prefix": self._db_prefix},
    )
```

#### 2d. Wrap each method with try/except

Every method that currently follows the pattern:

```python
def execute(self, query: str, params: list[Any] | None = None) -> int:
    caller = _caller_tag()
    t0 = time.perf_counter()
    result = self._inner.execute(query, params)
    self._record("execute", time.perf_counter() - t0, caller)
    return result
```

becomes:

```python
def execute(self, query: str, params: list[Any] | None = None) -> int:
    caller = _caller_tag()
    t0 = time.perf_counter()
    try:
        result = self._inner.execute(query, params)
        self._record("execute", time.perf_counter() - t0, caller)
        return result
    except Exception:
        self._record_error("execute", caller)
        raise
```

Apply this pattern to all 12 data methods (6 sync + 6 async):
- `execute` / `execute_async`
- `fetch_one` / `fetch_one_async`
- `fetch_all` / `fetch_all_async`
- `insert_one` / `insert_one_async`
- `bulk_insert` / `bulk_insert_async`
- `connect` / `connect_async`

**Note on `disconnect`/`disconnect_async`:** These should also get the try/except treatment. A failed disconnect is worth counting.

**Note on `is_connected` and `health_check`:** These are pass-through delegations that currently do not record timing. They should remain unchanged — they are lightweight status checks, not query operations, and adding error counting to `health_check` could create noisy metrics from expected health probe failures.

### Step 3: Wire `db_prefix` through `DatabaseFactory`

**File:** `de_platform/services/database/factory.py`

The factory knows the name of each database instance (e.g., `"warehouse"`, `"alerts"`, `"default"`) and is the natural place to attach the prefix. Update it to accept an optional `MetricsInterface` and auto-wrap instances in `ObservableDatabase`.

#### 3a. Constructor change

```python
class DatabaseFactory:
    def __init__(
        self, configs: dict[str, DbConfig], metrics: MetricsInterface | None = None
    ) -> None:
        self._configs = configs
        self._instances: dict[str, DatabaseInterface] = {}
        self._metrics = metrics
```

The `metrics` parameter is optional and defaults to `None`. When `None`, the factory creates raw instances without wrapping — preserving backward compatibility for unit tests that construct `DatabaseFactory` without metrics.

#### 3b. Auto-wrap in `get()`

```python
def get(self, name: str = "default") -> DatabaseInterface:
    if name in self._instances:
        return self._instances[name]
    config = self._configs.get(name)
    if config is None:
        available = ", ".join(self._configs.keys())
        raise KeyError(f"No database configured with name '{name}' (available: {available})")
    instance = config.create()
    if self._metrics is not None:
        from de_platform.services.database.observable_database import ObservableDatabase
        instance = ObservableDatabase(instance, self._metrics, db_prefix=name)
    self._instances[name] = instance
    return instance
```

The lazy import of `ObservableDatabase` inside the method avoids circular imports (the factory module does not import the observable module at the top level).

#### 3c. Import for type hint

Add at the top of the file:

```python
from de_platform.services.metrics.interface import MetricsInterface
```

### Step 4: Pass `MetricsInterface` to `DatabaseFactory` in the CLI runner

**File:** `de_platform/cli/runner.py`

In `_build_container()`, step 6 creates the `DatabaseFactory`. Currently:

```python
factory = DatabaseFactory(db_configs)
```

After step 8 (which ensures `MetricsInterface` is always registered), the factory cannot retroactively receive metrics because it was already created. Two options:

**Option A (recommended): Reorder steps.** Move the MetricsInterface default registration (step 8) before the DatabaseFactory creation (step 6). Then pass metrics to the factory:

```python
# Move current step 8 to before step 6:
from de_platform.services.metrics.interface import MetricsInterface
if not container.has(MetricsInterface):
    from de_platform.services.metrics.noop_metrics import NoopMetrics
    container.register_instance(MetricsInterface, NoopMetrics())

# Then in step 6:
metrics = container._registry[MetricsInterface]
factory = DatabaseFactory(db_configs, metrics=metrics)
```

**Option B: Pass metrics lazily.** Add a `set_metrics()` method to the factory and call it after step 8. This is more complex and less clean.

**Go with Option A.** The step reordering is safe because `MetricsInterface` registration has no dependencies on database or other services — it just ensures a `NoopMetrics` default exists.

**Remove step 9 wrapping (or make it conditional):** Since the factory now auto-wraps via `ObservableDatabase`, the explicit wrapping of the singleton `DatabaseInterface` in step 9 would result in double-wrapping. Two approaches:

- **Remove step 9 entirely.** The factory already wraps, so `factory.get("default")` returns an `ObservableDatabase`. When that instance is registered as the singleton `DatabaseInterface`, it is already wrapped.
- **Keep step 9 with an `isinstance` guard.** This is what the current code already does (`if not isinstance(raw_db, ObservableDatabase)`), so with the factory wrapping, the guard will skip redundant wrapping. This is the safer approach — leave step 9 as-is.

**Recommended:** Leave step 9 unchanged. The `isinstance` check already prevents double-wrapping, and keeping it serves as a safety net for cases where `DatabaseFactory` might be constructed without metrics (e.g., in tests).

### Step 5: Unit tests

**File:** `de_platform/services/database/tests/test_observable_database.py`

Replace the existing test file with comprehensive coverage. The existing three tests (`test_wraps_sync_operations`, `test_wraps_async_operations`, `test_caller_tag_contains_this_file`) should be preserved and enhanced. Add new tests:

#### 5a. Test error counter on sync exception

```python
def test_error_counter_on_sync_exception():
    """When the inner DB raises, an error counter is emitted and the exception propagates."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    # Patch inner to raise on execute
    original = inner.execute
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
```

#### 5b. Test error counter on async exception

```python
async def test_error_counter_on_async_exception():
    """Async methods also emit error counters on failure."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    await db.connect_async()

    original = inner.fetch_one
    def failing_fetch(query, params=None):
        raise RuntimeError("timeout")
    inner.fetch_one = failing_fetch

    with pytest.raises(RuntimeError, match="timeout"):
        await db.fetch_one_async("SELECT 1")

    assert "db_query_errors_total" in metrics.counters
    assert metrics.counters["db_query_errors_total"] == 1
```

#### 5c. Test `db_prefix` appears in tags

```python
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
```

#### 5d. Test `db_prefix` defaults to `"default"`

```python
def test_db_prefix_defaults_to_default():
    """When db_prefix is not specified, it defaults to 'default'."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    _, _, tags = metrics.histogram_calls[0]
    assert tags["db_prefix"] == "default"
```

#### 5e. Test caller tag points outside database package

```python
def test_caller_tag_points_to_test_file():
    """The caller tag resolves to this test file, not observable_database.py."""
    inner = MemoryDatabase()
    metrics = MemoryMetrics()
    db = ObservableDatabase(inner, metrics)
    db.connect()

    _, _, tags = metrics.histogram_calls[0]
    assert "test_observable_database.py" in tags["caller"]
    assert "observable_database.py" not in tags["caller"]
```

#### 5f. Test all sync method error paths

```python
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
```

#### 5g. Test factory auto-wrapping

**File:** `de_platform/services/database/tests/test_observable_database.py` (or a separate `test_factory.py` if preferred)

```python
def test_factory_wraps_with_observable_when_metrics_provided():
    """DatabaseFactory.get() returns an ObservableDatabase when metrics are provided."""
    from de_platform.services.database.factory import DatabaseFactory, DbConfig
    from de_platform.services.database.memory_database import MemoryDatabase

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
    from de_platform.services.database.memory_database import MemoryDatabase

    configs = {"default": DbConfig(impl_cls=MemoryDatabase)}
    factory = DatabaseFactory(configs)  # no metrics

    db = factory.get("default")
    assert isinstance(db, MemoryDatabase)
    assert not isinstance(db, ObservableDatabase)
```

---

## Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `de_platform/services/metrics/memory_metrics.py` | Modified | Add `counter_calls` and `histogram_calls` lists that retain tag information |
| `de_platform/services/database/observable_database.py` | Modified | Add `db_prefix` constructor param, `_record_error()` method, try/except on all data methods |
| `de_platform/services/database/factory.py` | Modified | Accept optional `MetricsInterface`, auto-wrap instances in `ObservableDatabase` |
| `de_platform/cli/runner.py` | Modified | Reorder `MetricsInterface` default registration before `DatabaseFactory` creation; pass metrics to factory |
| `de_platform/services/database/tests/test_observable_database.py` | Modified | Expand with error counter tests, tag assertion tests, factory wrapping tests |

No new files are created (the test file already exists).

---

## Verification

1. `make test` — all unit tests pass, including the expanded observable database tests
2. `make lint` — no ruff errors
3. `make test-integration` — integration tests still pass (ObservableDatabase wrapping is transparent to module code)

---

## Risks and Considerations

- **`_caller_tag()` performance:** `inspect.stack(0)` is called on every database operation. This is already the case today and has not been flagged as a bottleneck, but adding the error path does not make it worse — `_caller_tag()` is called once per method invocation regardless of success or failure.

- **Double-wrapping prevention:** The `isinstance` guard in `_build_container` step 9 already prevents double-wrapping. With the factory also wrapping, the guard ensures the singleton path is safe. No factory-created instance will be wrapped twice because the factory stores the wrapped instance in `_instances` and returns it on subsequent `get()` calls.

- **Tag cardinality:** Adding `db_prefix` introduces a low-cardinality tag (typically 1-3 distinct values per deployment). This is safe for Prometheus label cardinality. The `caller` tag has higher cardinality (one value per call site) which is already present today.

- **Backward compatibility of `MemoryMetrics`:** The new `counter_calls` and `histogram_calls` attributes are additive. Existing tests that only read `counters`, `gauges`, and `histograms` dicts are unaffected.

- **Backward compatibility of `ObservableDatabase.__init__`:** The `db_prefix` parameter has a default value (`"default"`), so all existing construction sites (including the step-9 wrapping in `_build_container`) continue to work without modification.

- **Backward compatibility of `DatabaseFactory.__init__`:** The `metrics` parameter has a default value (`None`), so all existing construction sites (including test harnesses that build `DatabaseFactory(configs)`) continue to work without modification.
