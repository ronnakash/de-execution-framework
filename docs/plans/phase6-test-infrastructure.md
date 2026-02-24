# Phase 6: Test Infrastructure Refactoring

## Overview

Refactor the test harness architecture, expand integration test coverage, add viewer UI tests, fix step logger issues (irrelevant screenshots, delta relevance), and add batch algo e2e coverage.

**Source:** changes.md #2, #3, #8, #12, #14, #17, #18; bugs.md #3, #5

## 6.1 Harness Refactoring

### Split harness into separate files

**Why:** changes.md #8 — "harness implementations should be split into separate files and the whole thing should be moved to its own folder"

Currently `tests/helpers/harness.py` is ~1025 lines containing `MemoryHarness`, `SharedPipeline`, and `RealInfraHarness` all in one file.

**New structure:**
```
tests/helpers/harness/
    __init__.py              — re-exports for backward compat
    protocol.py              — PipelineHarness protocol
    memory.py                — MemoryHarness
    shared_pipeline.py       — SharedPipeline (subprocess management)
    real_infra.py            — RealInfraHarness (per-test wrapper)
```

### Extract test helper class from harness

**Why:** changes.md #12 — "test harnesses should not have all the methods to interact with databases, kafka, ingestions etc. There should be some other class that contains all of these methods and is constructed with a harness"

**Design:**
```python
class PipelineTestClient:
    """Shared test interaction methods backed by a harness."""

    def __init__(self, harness: PipelineHarness) -> None:
        self._harness = harness

    # Ingestion
    async def ingest_events(self, event_type, events, method="rest"): ...
    async def ingest_file(self, event_type, events): ...

    # Polling / waiting
    async def wait_for_rows(self, table, expected, timeout=30): ...
    async def wait_for_alerts(self, expected, timeout=60): ...
    async def wait_for_cases(self, expected, timeout=60): ...

    # Direct queries
    async def fetch_rows(self, table): ...
    async def fetch_alerts(self): ...
    async def fetch_cases(self): ...

    # Kafka
    def publish(self, topic, message, key=None): ...
    def consume(self, topic): ...
```

This class works identically with both `MemoryHarness` and `RealInfraHarness` — it takes what it needs from the harness (ports, DB connections, MQ instances) and provides a unified API.

## 6.2 Step Logger Improvements

### Fix irrelevant screenshots

**Why:** bugs.md #5, changes.md #18 — "I see screenshots in UI tests when the screenshot is unrelated to the step because it doesn't interact with the ui" / "remove images from e2e test steps that don't interact with the ui"

**Solution:** Add a `ui_step` flag to `StepLogger.step()` and `StepLogger.log()`. Only capture screenshots when `ui_step=True`.

```python
@asynccontextmanager
async def step(self, name: str, description: str = "", ui_step: bool = False):
    # ... existing logic ...
    # Only capture screenshot if this step interacts with the UI
    if ui_step and self._page is not None:
        # capture screenshot
    # ...

def log(self, name: str, description: str = "", ui_step: bool = False):
    if ui_step:
        self._capture_deferred_screenshot()
    # ...
```

Default `ui_step=False` means existing e2e tests (non-UI) automatically stop capturing screenshots. UI tests explicitly pass `ui_step=True` for navigation/interaction steps.

### Fix step delta relevance

**Why:** changes.md #14 — "I see that we have the count changes already present in phase 1, why is it relevant? we should only have it in phase 2"

The issue: snapshot boundaries don't align with step boundaries. An insert from step 1 might not be visible in the before-snapshot of step 1 but appears in step 2's before-snapshot, causing "leaked" deltas.

**Solution:** Add a `settle_time` parameter to `step()` that waits briefly after taking the before-snapshot to let ClickHouse catch up:

```python
@asynccontextmanager
async def step(self, name: str, description: str = "", settle_ms: int = 100, ...):
    record.start_time = time.time()
    if self._diagnostics:
        await asyncio.sleep(settle_ms / 1000)  # let writes settle
        record.snapshot_before = _snapshot_to_dict(await self._diagnostics.snapshot())
    # ...
```

Combined with Phase 1's `FINAL` keyword on ClickHouse queries, this should align deltas with their intended steps.

## 6.3 Integration Test Coverage

**Why:** changes.md #2 — "services that interact with any infra must have integration testing for each of the interactions it has"

**Current coverage:**
- `tests/integration/services/test_postgres_database.py`

**Missing integration tests to add:**

| File | Tests |
|------|-------|
| `tests/integration/services/test_clickhouse_database.py` | Connect, insert, fetch, bulk_insert, FINAL queries |
| `tests/integration/services/test_redis_cache.py` | Get, set, delete, TTL, pub-sub |
| `tests/integration/services/test_kafka_queue.py` | Publish, consume, consumer groups, topic creation |
| `tests/integration/services/test_minio_filesystem.py` | Read, write, list, delete files |

Each test module should:
- Use real Docker infrastructure (`make infra-up`)
- Create isolated test data (unique keys/topics/buckets)
- Test both happy path and error cases (connection loss, timeouts)
- Be marked `@pytest.mark.integration`

## 6.4 Viewer UI Tests

**Why:** bugs.md #3, changes.md #3, #17 — "existing e2e tests mostly look at the admin UI instead of viewer UI, we need coverage for both"

Currently all UI tests log in as `admin@e2e.test`. Need tests that:
1. Create a viewer user for a specific tenant
2. Log in as that viewer
3. Verify the viewer sees only their tenant's data
4. Verify admin-only pages/actions are restricted
5. Clean up the viewer user after tests

**New fixture in `tests/e2e_ui/conftest.py`:**
```python
@pytest.fixture
def viewer_page(browser, base_url, pipeline_data):
    """Create a viewer user, log in, yield page, then delete user."""
    # 1. Create viewer user via auth API
    # 2. Log in via Playwright
    # 3. Yield page
    # 4. Delete viewer user via auth API
```

**New test file:** `tests/e2e_ui/test_viewer.py`
- `test_viewer_sees_own_tenant_alerts` — ingest data for tenant, create viewer for tenant, verify only that tenant's alerts visible
- `test_viewer_cannot_see_other_tenants` — verify no cross-tenant data leakage
- `test_viewer_cannot_access_admin_pages` — client config, user management restricted
- `test_viewer_events_explorer` — verify events scoped to tenant

## 6.5 Grafana Dashboard Verification

**Why:** changes.md #1 — "grafana dashboards have no data. need ui test to see they always work"

**New test file:** `tests/e2e_ui/test_grafana.py`
- Navigate to each Grafana dashboard (6 dashboards)
- Verify at least one panel has data (not all "No data" states)
- Requires Grafana to be running (docker-compose includes it)

## Files to Create

| File | Purpose |
|------|---------|
| `tests/helpers/harness/__init__.py` | Re-export MemoryHarness, SharedPipeline, RealInfraHarness |
| `tests/helpers/harness/protocol.py` | PipelineHarness protocol |
| `tests/helpers/harness/memory.py` | MemoryHarness |
| `tests/helpers/harness/shared_pipeline.py` | SharedPipeline |
| `tests/helpers/harness/real_infra.py` | RealInfraHarness |
| `tests/helpers/pipeline_test_client.py` | PipelineTestClient (shared interaction class) |
| `tests/integration/services/test_clickhouse_database.py` | ClickHouse integration tests |
| `tests/integration/services/test_redis_cache.py` | Redis integration tests |
| `tests/integration/services/test_kafka_queue.py` | Kafka integration tests |
| `tests/integration/services/test_minio_filesystem.py` | MinIO integration tests |
| `tests/e2e_ui/test_viewer.py` | Viewer role UI tests |
| `tests/e2e_ui/test_grafana.py` | Grafana dashboard verification |

## Files to Modify

| File | Changes |
|------|---------|
| `tests/helpers/harness.py` | Replace with harness/ package (keep as re-export shim for backward compat) |
| `tests/helpers/step_logger.py` | Add `ui_step` flag, `settle_ms` parameter |
| `tests/e2e_ui/conftest.py` | Add `viewer_page` fixture |

## Acceptance Criteria

1. Harness code split into separate files in `tests/helpers/harness/` package
2. `PipelineTestClient` encapsulates all DB/Kafka/ingestion interactions
3. All existing tests pass with the refactored harness (backward compat imports)
4. Screenshots only captured for `ui_step=True` steps
5. Step deltas align with their intended steps (no leaked counts)
6. Integration tests exist for ClickHouse, Redis, Kafka, MinIO
7. Viewer UI tests create a viewer user, verify tenant scoping, clean up
8. Grafana dashboard tests verify panels have data
