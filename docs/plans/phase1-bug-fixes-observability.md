# Phase 1: Bug Fixes & E2E Observability

## Overview

Fix the three highest-priority bugs discovered in the e2e test run (2026-02-24, 74/75 passed) and restore the E2E report observability that is currently blind to Postgres alerts, ClickHouse row accuracy, Kafka state, and metrics.

## Workstream 1: Alert Manager Race Condition

### Problem
`test_multiple_alerts_aggregate_into_case` fails intermittently. Two alerts arrive ~8ms apart, both call `_aggregate_into_case()`, both find no existing case via `_find_matching_case()`, both create a new case — producing 2 cases with `alert_count: 1` instead of 1 case with `alert_count: 2`.

Root cause: `_aggregate_into_case()` does SELECT-then-INSERT without a database transaction. The `PostgresDatabase.transaction()` context manager exists but is never used in alert_manager.

Secondary issue: `_add_alert_to_case()` uses DELETE + INSERT to update a case — non-atomic window where the case row disappears.

### Solution: Transaction + SELECT FOR UPDATE

Wrap the entire aggregate flow in `PostgresDatabase.transaction()` and use `SELECT ... FOR UPDATE` to lock the case row.

#### Changes to `de_platform/modules/alert_manager/main.py`

**1. `_aggregate_into_case()` — wrap in transaction**

```python
async def _aggregate_into_case(self, alert: dict) -> None:
    tenant_id = alert.get("tenant_id", "")
    algorithm = alert.get("algorithm", "")
    event_id = alert.get("event_id", "")
    alert_id = alert.get("alert_id", "")
    severity = alert.get("severity", "medium")
    created_at = alert.get("created_at", "")

    # Use transaction + row locking to prevent race conditions
    if hasattr(self.db, 'transaction'):
        async with self.db.transaction():
            await self._aggregate_into_case_inner(
                tenant_id, algorithm, event_id, alert_id, severity, created_at,
            )
    else:
        # MemoryDatabase (unit tests) — no transaction support needed
        await self._aggregate_into_case_inner(
            tenant_id, algorithm, event_id, alert_id, severity, created_at,
        )

async def _aggregate_into_case_inner(
    self, tenant_id: str, algorithm: str, event_id: str,
    alert_id: str, severity: str, created_at: Any,
) -> None:
    case = await self._find_matching_case(tenant_id, algorithm, event_id)
    if case:
        await self._add_alert_to_case(case, alert_id, severity, algorithm, created_at)
    else:
        await self._create_case(tenant_id, alert_id, severity, algorithm, created_at)
```

**2. `_find_matching_case()` — add FOR UPDATE locking**

When matching a case via Rule 1 (same algorithm, within aggregation window), use `SELECT ... FOR UPDATE` to lock the row so concurrent consumers see the lock and wait.

```python
# In _find_matching_case, when querying cases for Rule 1:
# Change:
tenant_cases = await self.db.fetch_all_async(
    "SELECT * FROM cases WHERE tenant_id = $1", [tenant_id]
)
# To:
tenant_cases = await self.db.fetch_all_async(
    "SELECT * FROM cases WHERE tenant_id = $1 FOR UPDATE", [tenant_id]
)
```

Note: `FOR UPDATE` is raw SQL passed through to asyncpg — it works with `PostgresDatabase` as-is. `MemoryDatabase` ignores the clause (its parser only looks at `SELECT * FROM <table> WHERE col = $1`).

**3. `_add_alert_to_case()` — replace DELETE+INSERT with UPDATE**

The current DELETE+INSERT pattern is non-atomic. Replace with a proper UPDATE statement.

```python
async def _add_alert_to_case(
    self, case: dict, alert_id: str,
    severity: str, algorithm: str, alert_time: Any,
) -> None:
    case_id = case["case_id"]
    new_count = case.get("alert_count", 0) + 1
    existing_algos = case.get("algorithms", [])
    if isinstance(existing_algos, str):
        existing_algos = [existing_algos]
    algorithms = list(set(existing_algos + [algorithm]))
    new_severity = max(
        case.get("severity", "medium"), severity,
        key=lambda s: _SEVERITY_ORDER.get(s, 0),
    )
    alert_dt = self._to_naive_dt(alert_time) or datetime.utcnow()
    new_title = _generate_title(new_count, algorithms, case["tenant_id"])

    await self.db.execute_async(
        "UPDATE cases SET alert_count = $1, algorithms = $2, severity = $3, "
        "last_alert_at = $4, title = $5, updated_at = $6 "
        "WHERE case_id = $7",
        [new_count, algorithms, new_severity, alert_dt, new_title,
         datetime.utcnow(), case_id],
    )
    await self.db.insert_one_async("case_alerts", {
        "case_id": case_id, "alert_id": alert_id,
    })
```

**Important**: `MemoryDatabase` does not support `UPDATE` — its `execute()` only handles simple `DELETE WHERE`. We need to add UPDATE support to `MemoryDatabase` for this to work in unit tests. Alternatively, keep the DELETE+INSERT path as a fallback for MemoryDatabase (check `hasattr(self.db, 'transaction')`).

**Decision**: Keep DELETE+INSERT for MemoryDatabase (unit tests). The race condition only occurs in concurrent Postgres scenarios, never in MemoryDatabase's synchronous execution. We'll branch on `hasattr(self.db, 'transaction')`:
- With transaction support (Postgres): UPDATE inside transaction
- Without (Memory): existing DELETE+INSERT (single-threaded, no race)

**4. `_update_case_status()` — same DELETE+INSERT → UPDATE fix**

Apply the same UPDATE pattern to `_update_case_status()` which also uses DELETE+INSERT:

```python
await self.db.execute_async(
    "UPDATE cases SET status = $1, updated_at = $2 WHERE case_id = $3",
    [new_status, datetime.utcnow(), case_id],
)
```

#### Unit test changes

Existing unit tests use `MemoryDatabase` and won't exercise the transaction path. No changes needed — the fallback path preserves existing behavior.

Add a focused integration test in `tests/integration/modules/` that:
1. Creates two alerts for the same tenant with the same algorithm
2. Publishes them near-simultaneously to the alerts topic
3. Asserts exactly 1 case with `alert_count: 2`

---

## Workstream 2: Velocity Algorithm Alert Suppression

### Problem

From bugs.md item #2: "I see way too many velocity algo alerts, this is probably not good."

Current behavior: `VelocityAlgo.evaluate_window()` returns 1 alert whenever `len(events) > max_events`. In realtime mode with `window_size=0` (the default when no client config exists), every single event is evaluated individually — the engine calls `_try_evaluate` which with `window_size=0` evaluates just the latest event and clears the buffer. This means velocity can never actually trigger in the default config because each window only has 1 event.

When a client has a non-zero window configured, the window accumulates events and evaluates once the window spans `window_size`. If there are >100 events in a 60s window, one velocity alert fires. But as the window slides, the same batch of events gets re-evaluated in overlapping windows, generating duplicate alerts for the same velocity spike.

### Solution: Deduplicate velocity alerts by suppression key

Add a per-tenant suppression mechanism to `VelocityAlgo` that tracks the last alert time per (tenant_id). Once an alert is generated for a tenant, suppress further velocity alerts for that tenant until the current window passes.

The user specified: velocity alert should be per-client within a tenant (using `client_id`). However, `client_id` doesn't exist on events yet (that's Phase 2). For Phase 1, we implement the suppression infrastructure keyed by `tenant_id` only, and the Phase 2 data model change will switch the key to `(tenant_id, client_id)`.

#### Changes to `de_platform/pipeline/algorithms.py`

```python
class VelocityAlgo(FraudAlgorithm):
    def __init__(
        self,
        max_events: int = 100,
        window_seconds: int = 60,
    ) -> None:
        self.max_events = max_events
        self.window_seconds = window_seconds
        # Track last alert time per suppression key to avoid duplicates
        # Key: tenant_id (Phase 2 will change to (tenant_id, client_id))
        self._last_alert_time: dict[str, datetime] = {}

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        max_events = (thresholds or {}).get("max_events", self.max_events)
        window_seconds = (thresholds or {}).get("window_seconds", self.window_seconds)

        if len(events) <= max_events:
            return []

        # Suppress if we already alerted for this tenant within the window
        suppression_key = tenant_id
        last_alert = self._last_alert_time.get(suppression_key)
        if last_alert is not None:
            cooldown = timedelta(seconds=window_seconds)
            now = datetime.now(timezone.utc)
            if (now - last_alert) < cooldown:
                return []

        trigger_event = events[max_events]
        self._last_alert_time[suppression_key] = datetime.now(timezone.utc)

        return [Alert(
            alert_id=uuid.uuid4().hex,
            tenant_id=tenant_id,
            event_type=trigger_event.get("event_type", ""),
            event_id=trigger_event.get("id", ""),
            message_id=trigger_event.get("message_id", ""),
            algorithm=self.name(),
            severity="medium",
            description=(
                f"Tenant {tenant_id!r} exceeded {max_events} events"
                f" in {window_seconds}s window"
            ),
            details={
                "event_count": len(events),
                "window_seconds": window_seconds,
                "window_start": window_start.isoformat()
                if window_start != datetime.min else "",
                "window_end": window_end.isoformat()
                if window_end != datetime.max else "",
            },
            created_at=_now_iso(),
        )]
```

#### Unit test changes

Update velocity algo unit tests to verify:
1. First window exceeding threshold generates alert
2. Second overlapping window within cooldown does NOT generate alert
3. After cooldown expires, next window does generate alert

---

## Workstream 3: E2E Report Observability

### Problem 1: `postgres.alerts` always -1

`TestDiagnostics._db_row_counts()` calls `self._postgres_db.fetch_all()` (the sync wrapper), which does `asyncio.get_event_loop().run_until_complete()`. This fails inside an async test because the event loop is already running. The bare `except` catches the `RuntimeError` and sets count to -1.

### Solution 1: Make `snapshot()` async

Convert `TestDiagnostics.snapshot()` and `_db_row_counts()` to async methods so they can call `fetch_all_async()` directly.

#### Changes to `tests/helpers/diagnostics.py`

```python
async def snapshot(self) -> PipelineSnapshot:
    """Take a point-in-time snapshot of all pipeline state."""
    snap = PipelineSnapshot(timestamp=time.time())

    if self._memory_queue is not None:
        snap.kafka_topics = self._memory_queue_state()

    snap.db_tables = await self._db_row_counts()
    snap.module_status = self._module_health()

    if self._memory_metrics is not None:
        snap.metrics = self._read_memory_metrics()
    elif self._prometheus_endpoints:
        for name, endpoint in self._prometheus_endpoints.items():
            for k, v in self._scrape_prometheus(endpoint).items():
                snap.metrics[f"{name}.{k}"] = v

    return snap

async def _db_row_counts(self) -> dict[str, int]:
    counts: dict[str, int] = {}
    # ... ClickHouse queries (sync — CH client is sync, this is fine)

    if self._postgres_db is not None:
        try:
            if self._tenant_id:
                rows = await self._postgres_db.fetch_all_async(
                    "SELECT count(*) as cnt FROM alerts WHERE tenant_id = $1",
                    [self._tenant_id],
                )
            else:
                rows = await self._postgres_db.fetch_all_async(
                    "SELECT count(*) as cnt FROM alerts WHERE tenant_id != $1",
                    ["PIPELINE_SENTINEL"],
                )
            counts["postgres.alerts"] = rows[0]["cnt"] if rows else 0
        except Exception:
            counts["postgres.alerts"] = -1
    return counts
```

#### Cascade: update all `snapshot()` callers

`StepLogger.step()` calls `self._diagnostics.snapshot()`. Since `step()` is already an `@asynccontextmanager`, just `await` it:

```python
# In StepLogger.step():
record.snapshot_before = _snapshot_to_dict(
    await self._diagnostics.snapshot()  # was: self._diagnostics.snapshot()
)
```

Check for other callers:
- `tests/helpers/harness.py` — `RealInfraHarness` timeout error paths call `self.diagnostics.snapshot()`. These are inside async methods, so add `await`.
- `tests/helpers/pytest_pipeline_report.py` — `snapshot_to_dict()` receives the snapshot object, doesn't call `snapshot()` itself. No change needed.

For the `MemoryHarness` path (unit tests), `MemoryDatabase.fetch_all()` is sync but also has `fetch_all_async()` (it just wraps the sync call). The async `snapshot()` will work with both.

### Problem 2: ClickHouse row counts don't add up

Step-boundary snapshots miss rows due to MergeTree's eventual consistency. The `_db_row_counts()` queries don't use the `FINAL` keyword.

### Solution 2: Add FINAL to ClickHouse test queries

Add `FINAL` to the ClickHouse count queries in `_db_row_counts()`. This forces ClickHouse to merge parts before returning counts, giving accurate results at the cost of slightly slower queries (acceptable for test diagnostics).

```python
# In _db_row_counts():
if self._clickhouse_db is not None:
    for table in tables[:5]:
        try:
            if self._tenant_id:
                rows = self._clickhouse_db.fetch_all(
                    f"SELECT count(*) as cnt FROM {table} FINAL"
                    " WHERE tenant_id = {p1:String}",
                    [self._tenant_id],
                )
            else:
                rows = self._clickhouse_db.fetch_all(
                    f"SELECT count(*) as cnt FROM {table} FINAL"
                    " WHERE tenant_id != {p1:String}",
                    ["PIPELINE_SENTINEL"],
                )
            counts[f"clickhouse.{table}"] = rows[0]["cnt"] if rows else 0
        except Exception:
            counts[f"clickhouse.{table}"] = -1
```

Note: `FINAL` only applies to test diagnostics queries. Production queries and the pipeline itself are unaffected.

### Problem 3: Kafka deltas always empty in E2E

`TestDiagnostics` only reads Kafka state from `MemoryQueue`. For real Kafka in E2E, it skips with "cannot be tenant-scoped".

### Solution 3: Defer to Phase 5 (Test Infrastructure)

Tenant-scoped Kafka watermarks require either: (a) per-tenant topic naming, or (b) consumer offset tracking per consumer group. Both are architectural changes that belong in the test infrastructure refactoring phase. For now, document the limitation.

### Problem 4: Metrics deltas always empty in E2E

No `prometheus_endpoints` are configured when `TestDiagnostics` is constructed in `RealInfraHarness`.

### Solution 4: Defer to Phase 5 (Test Infrastructure)

The metrics ports are available on `SharedPipeline` but not wired into `TestDiagnostics`. This is a harness wiring issue that fits better in Phase 5 when we refactor the harness architecture.

---

## Files to Modify

| File | Changes |
|------|---------|
| `de_platform/modules/alert_manager/main.py` | Transaction wrapping, FOR UPDATE, UPDATE instead of DELETE+INSERT |
| `de_platform/pipeline/algorithms.py` | Velocity suppression dict, cooldown check |
| `tests/helpers/diagnostics.py` | `snapshot()` and `_db_row_counts()` → async, FINAL keyword |
| `tests/helpers/step_logger.py` | `await` the async `snapshot()` calls |
| `tests/helpers/harness.py` | `await` snapshot() in timeout error paths |

## Files to Add

| File | Purpose |
|------|---------|
| `tests/integration/modules/test_alert_manager_race.py` | Integration test for concurrent case aggregation |

## Acceptance Criteria

1. `test_multiple_alerts_aggregate_into_case` passes reliably (no more race condition)
2. Velocity algo generates at most 1 alert per tenant per cooldown window
3. `postgres.alerts` shows real counts (not -1) in E2E reports
4. ClickHouse delta row counts in E2E reports match expected ingestion counts (FINAL merges)
5. All existing unit tests continue to pass (MemoryDatabase fallback preserved)
6. All existing e2e tests continue to pass

## Execution Order

1. Workstream 3 first (observability) — low risk, unblocks better debugging for the rest
2. Workstream 1 (alert manager race) — the only failing test
3. Workstream 2 (velocity suppression) — behavioral change, needs careful testing
