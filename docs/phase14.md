# Phase 14: Alert Manager Improvements

**Goal:** Eliminate full-table scans, make the case aggregation window configurable per-tenant, and improve case merging.

## Context

The current `alert_manager/main.py` has several performance issues that become critical as alert volume grows:

1. **`_find_matching_case()`** performs three full table scans per incoming alert: `SELECT * FROM cases`, `SELECT * FROM case_alerts`, and `SELECT * FROM alerts`. All filtering is done in Python after fetching every row.

2. **REST API handlers** (`_list_alerts`, `_list_cases`, `_get_case`, `_get_alert`, `_update_case_status`, `_get_alert_case`, `_cases_summary`) all use the same pattern: `SELECT * FROM <table>` followed by Python-side filtering.

3. **`_load_dedup_set()`** fetches ALL alerts at startup to rebuild the in-memory dedup set. As the alerts table grows, startup time degrades linearly.

4. **Aggregation window is hardcoded** at 60 minutes (`_DEFAULT_AGGREGATION_MINUTES = 60`). There is no way for different tenants to use different windows. The `ClientConfigCache` already supports a `case_aggregation_minutes` field in `get_window_config()`, but the alert manager does not use it.

### Current Code Snapshot

The alert manager module constructor (`de_platform/modules/alert_manager/main.py`, line 62-78) takes these dependencies:

```python
def __init__(
    self,
    config: ModuleConfig,
    logger: LoggerFactory,
    mq: MessageQueueInterface,
    db: DatabaseInterface,
    lifecycle: LifecycleManager,
    metrics: MetricsInterface,
    secrets: SecretsInterface,
) -> None:
```

It does **not** take `CacheInterface`, so it cannot use `ClientConfigCache`. The E2E harness launches the alert manager with `--db alerts=postgres --mq kafka` but no `--cache redis` flag (see `tests/helpers/harness.py`, line 609).

### MemoryDatabase Limitation

`MemoryDatabase._query()` only supports `SELECT * FROM <table>` with an optional single-column `WHERE col = $1` clause. Multi-column WHERE clauses, JOINs, subqueries, and `SELECT <specific_columns>` all silently return empty results. This means that any SQL optimization must be done in a way that is compatible with both:

- **PostgresDatabase** (production) -- supports full SQL with parameterized queries
- **MemoryDatabase** (unit tests) -- only supports `SELECT * FROM <table> [WHERE col = $1]`

The strategy is to use targeted single-column WHERE queries where possible (which both backends support), and fall back to Python-side filtering only where multi-column filtering is required.

---

## Implementation Plan

### Step 1: Add Database Indexes

Add compound indexes to the `alerts`, `cases`, and `case_alerts` tables. The existing migration `001_create_cases.up.sql` already creates basic single-column indexes (`idx_cases_tenant_id`, `idx_cases_status`), and `005_create_alerts_table.up.sql` creates `idx_alerts_tenant` and `idx_alerts_severity`. The new migration adds compound indexes for the query patterns used by case aggregation and REST API filtering.

**New file:** `de_platform/migrations/alert_manager/002_add_indexes.up.sql`

```sql
-- Compound index for the most common case aggregation lookup:
-- open cases for a specific tenant
CREATE INDEX IF NOT EXISTS idx_cases_tenant_status ON cases(tenant_id, status);

-- Index on last_alert_at for aggregation window cutoff filtering
CREATE INDEX IF NOT EXISTS idx_cases_last_alert_at ON cases(last_alert_at);

-- Index for cross-algorithm grouping: find alerts by event_id within a tenant
CREATE INDEX IF NOT EXISTS idx_alerts_tenant_event ON alerts(tenant_id, event_id);

-- Compound index for REST API filtering: alerts by tenant + algorithm
CREATE INDEX IF NOT EXISTS idx_alerts_tenant_algorithm ON alerts(tenant_id, algorithm);

-- Index for case_alerts lookups by alert_id (reverse lookup: alert -> case)
CREATE INDEX IF NOT EXISTS idx_case_alerts_alert_id ON case_alerts(alert_id);

-- Index for case_alerts lookups by case_id (forward lookup: case -> alerts)
-- Note: case_alerts has PRIMARY KEY (case_id, alert_id), which already covers
-- lookups by case_id. This index is not needed but included for clarity.
```

**New file:** `de_platform/migrations/alert_manager/002_add_indexes.down.sql`

```sql
DROP INDEX IF EXISTS idx_cases_tenant_status;
DROP INDEX IF EXISTS idx_cases_last_alert_at;
DROP INDEX IF EXISTS idx_alerts_tenant_event;
DROP INDEX IF EXISTS idx_alerts_tenant_algorithm;
DROP INDEX IF EXISTS idx_case_alerts_alert_id;
```

**Note:** The `idx_cases_tenant_id` and `idx_cases_status` indexes from `001_create_cases.up.sql` can be kept -- the new compound index `idx_cases_tenant_status` covers the combined case but Postgres can still use the single-column indexes for other query patterns. No existing indexes are dropped.

### Step 2: Replace Full-Table Scans in `_find_matching_case()`

**File:** `de_platform/modules/alert_manager/main.py`

The current implementation (lines 178-220) performs three `SELECT * FROM ...` queries and filters in Python. Replace with targeted queries that use single-column WHERE clauses (compatible with MemoryDatabase).

**Current code:**

```python
async def _find_matching_case(
    self, tenant_id: str, algorithm: str, event_id: str,
) -> dict | None:
    all_cases = await self.db.fetch_all_async("SELECT * FROM cases")
    open_cases = [
        c for c in all_cases
        if c.get("tenant_id") == tenant_id and c.get("status") == "open"
    ]
    # ... fetches ALL case_alerts and ALL alerts ...
```

**New approach:**

```python
async def _find_matching_case(
    self, tenant_id: str, algorithm: str, event_id: str,
) -> dict | None:
    # Step 1: Find alerts with the same event_id for this tenant
    # MemoryDatabase supports single-column WHERE, so query by event_id
    # and filter by tenant_id in Python
    matching_alerts = await self.db.fetch_all_async(
        "SELECT * FROM alerts WHERE event_id = $1", [event_id]
    )
    matching_alert_ids = {
        a["alert_id"] for a in matching_alerts
        if a.get("tenant_id") == tenant_id
    }

    if matching_alert_ids:
        # Step 2: Check if any of these alerts are linked to an open case
        for alert_id in matching_alert_ids:
            case_links = await self.db.fetch_all_async(
                "SELECT * FROM case_alerts WHERE alert_id = $1", [alert_id]
            )
            for link in case_links:
                case_id = link["case_id"]
                case = await self.db.fetch_one_async(
                    "SELECT * FROM cases WHERE case_id = $1", [case_id]
                )
                if (case
                    and case.get("tenant_id") == tenant_id
                    and case.get("status") == "open"):
                    return case

    # Rule 1: same algorithm, within aggregation window
    # Fetch open cases for this tenant -- use single-column WHERE
    # on tenant_id, then filter by status in Python
    tenant_cases = await self.db.fetch_all_async(
        "SELECT * FROM cases WHERE tenant_id = $1", [tenant_id]
    )
    open_cases = [c for c in tenant_cases if c.get("status") == "open"]

    cutoff = datetime.utcnow() - timedelta(
        minutes=self._get_aggregation_minutes(tenant_id),
    )

    for case in open_cases:
        case_algos = case.get("algorithms", [])
        if isinstance(case_algos, str):
            case_algos = [case_algos]
        if algorithm in case_algos:
            last_alert = case.get("last_alert_at")
            if last_alert:
                if isinstance(last_alert, str):
                    last_alert = datetime.fromisoformat(last_alert)
                if last_alert.replace(tzinfo=None) >= cutoff:
                    return case

    return None
```

**Key improvements:**
- Rule 2 (cross-algorithm grouping) goes from 3 full-table scans to targeted lookups by `event_id` and `alert_id`
- Rule 1 (same algorithm within window) goes from scanning all cases to scanning only cases for the target tenant
- All queries use `WHERE col = $1` which MemoryDatabase supports
- In Postgres, the new indexes from Step 1 make these queries efficient

**Trade-off:** Rule 2 now issues one `SELECT * FROM case_alerts WHERE alert_id = $1` per matching alert. In the worst case (many alerts sharing the same event_id), this could be N+1 queries. This is acceptable because: (a) in practice, each event_id has at most a handful of alerts (one per algorithm), and (b) each query is indexed and returns at most one row.

### Step 3: Optimize REST API Query Methods

**File:** `de_platform/modules/alert_manager/main.py`

Replace full-table scans in REST handlers with targeted queries. Where multi-column filtering is needed, use the most selective single-column WHERE clause and filter the rest in Python.

#### `_list_alerts` (line 320-338)

**Current:** `SELECT * FROM alerts` + Python filter by tenant_id, severity, algorithm.

**New:**

```python
async def _list_alerts(self, request: web.Request) -> web.Response:
    # ...
    tenant_id = self._resolve_tenant_id(request)
    severity = request.rel_url.query.get("severity")
    algorithm = request.rel_url.query.get("algorithm")
    limit = int(request.rel_url.query.get("limit", 50))
    offset = int(request.rel_url.query.get("offset", 0))

    if tenant_id:
        rows = await self.db.fetch_all_async(
            "SELECT * FROM alerts WHERE tenant_id = $1", [tenant_id]
        )
    else:
        rows = await self.db.fetch_all_async("SELECT * FROM alerts")

    if severity:
        rows = [r for r in rows if r.get("severity") == severity]
    if algorithm:
        rows = [r for r in rows if r.get("algorithm") == algorithm]
    result = rows[offset: offset + limit]
    return web.json_response(dumps=_dumps, data=result)
```

The `tenant_id` filter is the most common and most selective. By pushing it to the database level, we reduce the number of rows transferred and processed in Python. The `severity` and `algorithm` filters remain in Python because MemoryDatabase only supports single-column WHERE.

#### `_get_alert` (line 340-349)

**Current:** `SELECT * FROM alerts` + loop to find by alert_id.

**New:**

```python
async def _get_alert(self, request: web.Request) -> web.Response:
    alert_id = request.match_info["alert_id"]
    row = await self.db.fetch_one_async(
        "SELECT * FROM alerts WHERE alert_id = $1", [alert_id]
    )
    if not row:
        raise web.HTTPNotFound(...)
    return web.json_response(dumps=_dumps, data=row)
```

This is a direct primary key lookup -- the most impactful optimization.

#### `_get_alert_case` (line 351-363)

**Current:** `SELECT * FROM case_alerts` + loop, then `SELECT * FROM cases` + loop.

**New:**

```python
async def _get_alert_case(self, request: web.Request) -> web.Response:
    alert_id = request.match_info["alert_id"]
    case_alert = await self.db.fetch_one_async(
        "SELECT * FROM case_alerts WHERE alert_id = $1", [alert_id]
    )
    if not case_alert:
        raise web.HTTPNotFound(...)
    case = await self.db.fetch_one_async(
        "SELECT * FROM cases WHERE case_id = $1", [case_alert["case_id"]]
    )
    if not case:
        raise web.HTTPNotFound(...)
    return web.json_response(dumps=_dumps, data=case)
```

Two point lookups instead of two full table scans.

#### `_list_cases` (line 365-383)

**Current:** `SELECT * FROM cases` + Python filter.

**New:** Same pattern as `_list_alerts` -- push `tenant_id` WHERE clause to the database, filter `status` and `severity` in Python.

#### `_get_case` (line 385-407)

**Current:** `SELECT * FROM cases` + loop, then `SELECT * FROM case_alerts` + filter, then `SELECT * FROM alerts` + filter.

**New:**

```python
async def _get_case(self, request: web.Request) -> web.Response:
    case_id = request.match_info["case_id"]
    case = await self.db.fetch_one_async(
        "SELECT * FROM cases WHERE case_id = $1", [case_id]
    )
    if not case:
        raise web.HTTPNotFound(...)

    case_alert_rows = await self.db.fetch_all_async(
        "SELECT * FROM case_alerts WHERE case_id = $1", [case_id]
    )
    alert_ids = {ca["alert_id"] for ca in case_alert_rows}

    # Fetch each alert by ID (typically 1-10 alerts per case)
    alerts = []
    for aid in alert_ids:
        alert = await self.db.fetch_one_async(
            "SELECT * FROM alerts WHERE alert_id = $1", [aid]
        )
        if alert:
            alerts.append(alert)

    result = dict(case)
    result["alerts"] = alerts
    return web.json_response(dumps=_dumps, data=result)
```

Point lookup for the case, indexed lookup for case_alerts, then N point lookups for individual alerts. N is bounded by the number of alerts per case (typically 1-10).

#### `_update_case_status` (line 409-440)

**Current:** `SELECT * FROM cases` + loop.

**New:** Use `fetch_one_async("SELECT * FROM cases WHERE case_id = $1", [case_id])`.

#### `_cases_summary` (line 442-458)

**Current:** `SELECT * FROM cases` + Python aggregation.

**New:** Push `tenant_id` filter to database:

```python
if tenant_id:
    cases = await self.db.fetch_all_async(
        "SELECT * FROM cases WHERE tenant_id = $1", [tenant_id]
    )
else:
    cases = await self.db.fetch_all_async("SELECT * FROM cases")
```

The aggregation (counting by status/severity) stays in Python since it requires full-column grouping. For a future optimization, this could use SQL `GROUP BY` with Postgres, but that would not be MemoryDatabase-compatible.

### Step 4: Optimize `_load_dedup_set()`

**File:** `de_platform/modules/alert_manager/main.py`

The current implementation (lines 144-149) fetches all alert rows and extracts `(algorithm, event_id)` tuples. This transfers full row data when only two columns are needed.

**MemoryDatabase limitation:** `SELECT algorithm, event_id FROM alerts` returns empty results because MemoryDatabase only handles `SELECT *`. So we must keep `SELECT *` for compatibility.

**Optimization approach:** No SQL change is possible due to MemoryDatabase. However, we can add a progress log and document the limitation:

```python
async def _load_dedup_set(self) -> None:
    self._known_alerts = set()
    rows = await self.db.fetch_all_async("SELECT * FROM alerts")
    for row in rows:
        self._known_alerts.add((row.get("algorithm", ""), row.get("event_id", "")))
    self.log.info("Dedup set loaded", count=len(self._known_alerts))
```

**Future improvement (out of scope):** If the alerts table grows very large, consider:
- Adding a `SELECT algorithm, event_id FROM alerts` path that only activates when the database is PostgresDatabase (runtime type check)
- Using a Bloom filter instead of an exact set
- Paginating the initial load

### Step 5: Add `CacheInterface` Dependency for Per-Tenant Configuration

**File:** `de_platform/modules/alert_manager/main.py`

Add `CacheInterface` as an **optional** dependency. The module should work correctly without it (defaulting to the hardcoded 60-minute window), but when cache is available, it reads per-tenant configuration via `ClientConfigCache`.

**Updated constructor:**

```python
from de_platform.services.cache.interface import CacheInterface

class AlertManagerModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        db: DatabaseInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        secrets: SecretsInterface,
        cache: CacheInterface | None = None,  # NEW: optional dependency
    ) -> None:
        # ... existing assignments ...
        self._cache = cache
        self._config_cache: ClientConfigCache | None = None
```

**Note on DI:** Because `cache` defaults to `None`, the DI container will not fail if `CacheInterface` is not registered (i.e., `--cache redis` is not passed). The module degrades gracefully.

**Updated `initialize()`:**

```python
async def initialize(self) -> None:
    self.log = self.logger.create()
    self.port = self.config.get("port", 8007)
    await self.db.connect_async()
    self.lifecycle.on_shutdown(self._stop_server)
    self.lifecycle.on_shutdown(self.db.disconnect_async)

    # Initialize client config cache if cache service is available
    if self._cache is not None:
        from de_platform.pipeline.client_config_cache import ClientConfigCache
        self._config_cache = ClientConfigCache(self._cache)
        self._config_cache.start()
        self.lifecycle.on_shutdown(lambda: self._config_cache.stop())

    await self._load_dedup_set()
    self.log.info("Alert Manager initialized", port=self.port)
```

**New helper method:**

```python
def _get_aggregation_minutes(self, tenant_id: str) -> int:
    """Return the case aggregation window in minutes for the given tenant.

    Reads from ClientConfigCache if available (populated by the client_config
    service). Falls back to _DEFAULT_AGGREGATION_MINUTES (60).
    """
    if self._config_cache is not None:
        window_config = self._config_cache.get_window_config(tenant_id)
        return window_config.get(
            "case_aggregation_minutes", _DEFAULT_AGGREGATION_MINUTES
        )
    return _DEFAULT_AGGREGATION_MINUTES
```

**Usage in `_find_matching_case()`:** Replace the hardcoded `_DEFAULT_AGGREGATION_MINUTES` (line 214) with `self._get_aggregation_minutes(tenant_id)`. This is already shown in the Step 2 code.

### Step 6: Update E2E Harness and Module Spec

**File:** `tests/helpers/harness.py`

Update the `alert_manager` entry in `module_specs` (line 609) to include `--cache redis`:

```python
# Before:
("alert_manager", ["--db", "alerts=postgres", "--mq", "kafka"],
 ["--port", str(self.alert_manager_port)]),

# After:
("alert_manager", ["--db", "alerts=postgres", "--mq", "kafka", "--cache", "redis"],
 ["--port", str(self.alert_manager_port)]),
```

This ensures the alert manager gets a `CacheInterface` (Redis) in E2E tests and production, enabling per-tenant configuration.

**File:** `de_platform/modules/alert_manager/module.json`

No change needed. The `module.json` only describes module-specific args (like `port`). Infrastructure flags (`--db`, `--mq`, `--cache`) are global CLI flags handled by the runner, not module args.

### Step 7: Verify Severity Escalation

The existing `_add_alert_to_case()` method (lines 249-278) already implements severity escalation correctly:

```python
new_severity = max(
    case.get("severity", "medium"), severity,
    key=lambda s: _SEVERITY_ORDER.get(s, 0),
)
```

And there is already a passing test for this (`test_case_severity_escalates`, line 158-170 in `test_main.py`). No code changes needed here, but we should add an additional test for the edge case where severity stays the same (Step 8).

### Step 8: Update and Add Unit Tests

**File:** `de_platform/modules/alert_manager/tests/test_main.py`

The existing test file already has good coverage for case aggregation and severity escalation. The following new tests should be added:

#### Test 1: Configurable aggregation window via cache

```python
@pytest.mark.asyncio
async def test_configurable_aggregation_window() -> None:
    """Verify that per-tenant aggregation window is read from cache."""
    from de_platform.services.cache.memory_cache import MemoryCache

    mq = MemoryQueue()
    db = MemoryDatabase()
    cache = MemoryCache()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({})

    # Set a short aggregation window for tenant t1
    cache.set("client_config:t1", {
        "mode": "realtime",
        "case_aggregation_minutes": 1,  # 1 minute
    })

    module = AlertManagerModule(
        config=config, logger=logger, mq=mq, db=db,
        lifecycle=lifecycle, metrics=NoopMetrics(),
        secrets=EnvSecrets(), cache=cache,
    )
    await module.initialize()

    assert module._get_aggregation_minutes("t1") == 1
    assert module._get_aggregation_minutes("unknown_tenant") == 60  # default
```

#### Test 2: Default aggregation window without cache

```python
@pytest.mark.asyncio
async def test_default_aggregation_window_without_cache() -> None:
    """Verify that the module works without CacheInterface (cache=None)."""
    module, _, _ = await _setup_module()  # existing helper passes no cache
    assert module._get_aggregation_minutes("any_tenant") == 60
```

#### Test 3: Same algorithm outside window creates new case

```python
@pytest.mark.asyncio
async def test_same_algo_outside_window_creates_new_case() -> None:
    """Alerts from the same algorithm outside the aggregation window
    should create separate cases."""
    from de_platform.services.cache.memory_cache import MemoryCache

    mq = MemoryQueue()
    db = MemoryDatabase()
    cache = MemoryCache()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")

    # Set a 0-minute aggregation window -- every alert is outside the window
    cache.set("client_config:t1", {
        "mode": "realtime",
        "case_aggregation_minutes": 0,
    })

    module = AlertManagerModule(
        config=ModuleConfig({}), logger=logger, mq=mq, db=db,
        lifecycle=lifecycle, metrics=NoopMetrics(),
        secrets=EnvSecrets(), cache=cache,
    )
    await module.initialize()

    a1 = _make_alert(event_id="o1", algorithm="large_notional")
    a2 = _make_alert(event_id="o2", algorithm="large_notional")
    mq.publish(ALERTS, a1)
    await module._consume_and_process()
    mq.publish(ALERTS, a2)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 2  # separate cases because window=0
```

#### Test 4: Severity does not downgrade

```python
@pytest.mark.asyncio
async def test_case_severity_does_not_downgrade() -> None:
    """A lower-severity alert should not downgrade the case severity."""
    module, mq, db = await _setup_module()
    a1 = _make_alert(event_id="o1", algorithm="large_notional", severity="critical")
    a2 = _make_alert(event_id="o2", algorithm="large_notional", severity="low")
    mq.publish(ALERTS, a1)
    await module._consume_and_process()
    mq.publish(ALERTS, a2)
    await module._consume_and_process()

    cases = db.fetch_all("SELECT * FROM cases")
    assert len(cases) == 1
    assert cases[0]["severity"] == "critical"  # stays at max
```

#### Update `_setup_module` helper

The existing `_setup_module()` helper does not pass `cache`, which is fine because `cache` defaults to `None`. No change needed for backward compatibility. Tests that need cache create the module directly with the `cache=` parameter.

---

## Files Changed

| File | Action | Description |
|------|--------|-------------|
| `de_platform/migrations/alert_manager/002_add_indexes.up.sql` | NEW | Compound indexes for common query patterns |
| `de_platform/migrations/alert_manager/002_add_indexes.down.sql` | NEW | Rollback for the new indexes |
| `de_platform/modules/alert_manager/main.py` | MODIFY | Add `CacheInterface` dependency, `_get_aggregation_minutes()` helper, replace full-table scans with targeted WHERE queries in `_find_matching_case()` and all REST handlers |
| `tests/helpers/harness.py` | MODIFY | Add `--cache redis` to alert_manager module spec in `SharedPipeline._start_subprocesses()` |
| `de_platform/modules/alert_manager/tests/test_main.py` | MODIFY | Add tests for configurable window, window=0 edge case, severity non-downgrade |

## Verification

1. **Unit tests:** `make test` -- all existing and new unit tests pass. The existing `_setup_module()` helper passes `cache=None` implicitly, so all existing tests are unaffected.

2. **Lint:** `make lint` -- no ruff errors.

3. **Migration:** `make migrate cmd=up args="alert_manager --db postgres"` -- indexes created on the `alerts`, `cases`, and `case_alerts` tables.

4. **Integration test (manual):**
   - Start infrastructure: `make infra-up`
   - Run the alert manager: `make run module=alert_manager args="--db alerts=postgres --mq kafka --cache redis"`
   - Set a custom aggregation window for a tenant via the client_config service
   - Send alerts and verify they group according to the tenant-specific window

5. **E2E tests:** `make test-e2e` -- the updated harness passes `--cache redis` to the alert manager subprocess, so per-tenant config is available.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| MemoryDatabase does not support multi-column WHERE | All new queries use single-column `WHERE col = $1` which MemoryDatabase handles correctly |
| N+1 queries in `_find_matching_case()` Rule 2 | Bounded by alerts-per-event (typically 1-3, one per algorithm). Indexed lookups are fast. |
| Adding `cache` parameter breaks DI resolution | Parameter defaults to `None`, so DI skips it when `CacheInterface` is not registered |
| Existing tests break from constructor change | `cache` has a default value of `None`, so existing test code that does not pass `cache=` is unaffected |
| Index creation on large tables causes lock contention | `CREATE INDEX IF NOT EXISTS` is used. For production tables with millions of rows, consider `CREATE INDEX CONCURRENTLY` (requires running outside a transaction). The migration runner would need to support this. |
