# Implementation Plan

Comprehensive plan for all changes listed in `docs/changes.md`.

---

## Phase 1: Foundation & Code Quality ✅ COMPLETE

**Goal:** Fix structural issues and harden the codebase before adding features. No new services.

### 1.1 Consolidate Module and AsyncModule (#21)

**Problem:** `Module` (sync) and `AsyncModule` (async) in `de_platform/modules/base.py` are near-identical ABCs. Every new module must choose between them, and the runner has special-case detection for async.

**Changes:**

- Merge into a single `Module` base class where all lifecycle methods (`initialize`, `validate`, `execute`, `teardown`, `run`) are async
- The `run()` method will always be a coroutine; the CLI runner already calls `asyncio.run()` for `AsyncModule` — make it the default path
- Sync modules that exist today (`hello_world`, `batch_etl`, `run_migrations`, `currency_loader`, `file_processor`) simply become `async def execute()` with synchronous code inside (no `await` needed, Python allows this)
- Update `de_platform/cli/runner.py` to always `asyncio.run(module.run())` — remove the sync/async dispatch branch

**Files:**

| File | Change |
|------|--------|
| `de_platform/modules/base.py` | Delete `Module`, rename `AsyncModule` → `Module` |
| `de_platform/cli/runner.py` | Always `asyncio.run()`, remove `inspect.iscoroutine` check |
| `de_platform/modules/hello_world/main.py` | `class HelloWorldModule(Module)`, `async def execute()` |
| `de_platform/modules/batch_etl/main.py` | Same pattern |
| `de_platform/modules/run_migrations/main.py` | Same pattern |
| `de_platform/modules/currency_loader/main.py` | Same pattern |
| `de_platform/modules/file_processor/main.py` | Same pattern |
| All other modules already extending `AsyncModule` | Change to `Module` |
| All tests importing `Module`/`AsyncModule` | Update imports |

### 1.2 Service Crash Resilience (#22)

**Problem:** An unhandled exception in a service's `execute()` loop (e.g., algos inserting a string where Postgres expects datetime) crashes the entire process.

**Changes:**

- Add a `safe_execute` wrapper in the `Module` base class (or as a decorator) that catches exceptions in the main processing loop, logs them, and continues
- Each service module's `execute()` loop should wrap its per-message processing in try/except, log the error with full context (tenant_id, event_id, event_type), and continue to the next message
- Add a configurable error threshold — if a service hits N consecutive errors, it should shut down gracefully rather than silently swallowing a systemic failure

**Modules to update:**

| Module | Per-message handler to wrap |
|--------|---------------------------|
| `normalizer` | `_poll_and_process()` |
| `persistence` | `_flush_buffer()` and individual inserts |
| `algos` | `_evaluate()` |
| `data_api` | `_consume_alerts()` |
| `stream_ingest` | message consumption loop |
| `kafka_starter` | message consumption loop |

**Pattern:**
```python
async def execute(self) -> int:
    consecutive_errors = 0
    while not self.lifecycle.is_shutting_down:
        try:
            self._poll_and_process(...)
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            self.log.error("Processing error", error=str(exc), consecutive=consecutive_errors)
            if consecutive_errors >= self.config.get("max_consecutive_errors", 50):
                self.log.critical("Too many consecutive errors, shutting down")
                break
        await asyncio.sleep(0.01)
    return 0
```

### 1.3 Integration Tests for Infrastructure Services (#24)

**Problem:** Services that interact with Postgres, ClickHouse, Redis, MinIO, Kafka have no dedicated integration tests verifying CRUD operations with real data.

**Changes:**

Create `tests/integration/` directory with test files for each infrastructure service:

| Test file | What it tests |
|-----------|--------------|
| `test_postgres.py` | `PostgresDatabase`: connect, insert_one, bulk_insert, fetch_all, execute (CREATE/TRUNCATE), disconnect. Test with real schema (orders, alerts, currency_rates). |
| `test_clickhouse.py` | `ClickHouseDatabase`: connect, insert_one, bulk_insert, fetch_all, execute (DDL). Test JSON serialization for String columns. |
| `test_redis.py` | `RedisCache`: connect, get/set (string, int, float), set with TTL, delete, flush, disconnect. |
| `test_kafka.py` | `KafkaQueue`: connect, publish, consume_one, subscribe, disconnect. Test with real topic creation/consumption. |
| `test_minio.py` | `MinioFileSystem`: upload (put_object), download (get_object), list, delete. Test with real bucket. |

- All tests marked with `@pytest.mark.real_infra` and use the existing `infra` fixture from `tests/e2e/conftest.py`
- Move shared infra fixtures from `tests/e2e/conftest.py` to `tests/conftest.py` so both `tests/integration/` and `tests/e2e/` can use them
- Add `make test-integration` target (already exists but currently only does Postgres)

### 1.4 Test Execution Improvements (#20, #23)

**#20 — First failure shouldn't stop remaining tests:**
- Add `--tb=short` and remove any `-x` / `--exitfirst` flags from pytest invocations
- If `-x` is desired for local dev speed, make it opt-in: `make test-e2e-quick` (with `-x`) vs `make test-e2e` (runs all)

**#23 — Kafka topic deletion between tests:**
- Already resolved in the current codebase: `_cleanup_between_tests` in `tests/e2e/conftest.py` deletes and recreates all Kafka topics between tests
- Document the reasoning: topics are deleted/recreated to purge stale messages; simple offset management doesn't work because consumer group rebalancing + `auto.offset.reset` timing is unreliable across test boundaries
- This is the correct approach given the test isolation requirements

---

## Phase 2: Observability & Test Debugging

**Goal:** Full observability stack (metrics, structured logging, dashboards) plus a test debugging framework that makes E2E test failures immediately diagnosable. Every future phase benefits from this — new features get observable, debuggable tests for free.

**Motivation:** During Phase 1 E2E debugging, root-causing failures required creating ad-hoc debug scripts and manually tracing data through Kafka topics and databases. The KafkaQueue auto-connect bug took multiple rounds of narrowing down because there was zero visibility into message flow. This phase eliminates that class of debugging pain.

### 2.1 Module Metrics Emission

**Problem:** `MetricsInterface` exists with three implementations (Memory, Prometheus, Noop) but zero modules actually emit metrics. No visibility into pipeline throughput, error rates, or processing latency.

**Changes:**

Add `MetricsInterface` as an optional constructor dependency to all pipeline modules. When metrics is not registered in the DI container (e.g. `--metrics` flag not passed), modules skip emission. When registered, every significant operation emits a metric.

**Standard metric names:**

| Metric | Type | Labels | Emitted by |
|--------|------|--------|------------|
| `events_received_total` | counter | `service`, `topic`, `event_type`, `tenant_id` | normalizer, algos, persistence |
| `events_processed_total` | counter | `service`, `event_type`, `tenant_id`, `status` | normalizer (status=ok/error/duplicate) |
| `events_errors_total` | counter | `service`, `error_type`, `tenant_id` | normalizer, persistence |
| `duplicates_detected_total` | counter | `service`, `dedup_type`, `tenant_id` | normalizer (dedup_type=internal/external) |
| `processing_duration_seconds` | histogram | `service`, `operation` | normalizer (_poll_and_process), persistence (_flush), algos (_evaluate) |
| `rows_flushed_total` | counter | `service`, `table` | persistence |
| `buffer_size` | gauge | `service`, `table` | persistence |
| `alerts_generated_total` | counter | `algorithm`, `severity`, `tenant_id` | algos |
| `http_requests_total` | counter | `service`, `endpoint`, `method`, `status_code` | data_api, rest_starter |
| `http_request_duration_seconds` | histogram | `service`, `endpoint`, `method` | data_api, rest_starter |
| `events_ingested_total` | counter | `service`, `event_type`, `method` | rest_starter, kafka_starter, file_processor |
| `kafka_messages_published_total` | counter | `service`, `topic` | normalizer, algos (when publishing to downstream topics) |

**Pattern for optional metrics injection:**

```python
class NormalizerModule(Module):
    def __init__(
        self,
        mq: MessageQueueInterface,
        cache: CacheInterface,
        log: LoggingInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface | None = None,  # optional
    ):
        self.metrics = metrics or _NullMetrics()

class _NullMetrics:
    """No-op fallback when MetricsInterface is not registered."""
    def counter(self, *a, **kw): pass
    def gauge(self, *a, **kw): pass
    def histogram(self, *a, **kw): pass
```

Alternatively, register `NoopMetrics` as the default when `--metrics` is not passed, so modules always get a non-None `MetricsInterface`. This is cleaner — the container always has a metrics instance.

**Decision:** Register `NoopMetrics` as default in `_build_container()` when no `--metrics` flag is provided. This way modules declare `metrics: MetricsInterface` as a required dependency (no Optional, no null object pattern in modules). The runner handles the fallback.

**Files to modify:**

| File | Change |
|------|--------|
| `de_platform/cli/runner.py` | Register `NoopMetrics` as default `MetricsInterface` when no `--metrics` flag |
| `de_platform/modules/normalizer/main.py` | Add `metrics: MetricsInterface` param, emit metrics in `_poll_and_process()` |
| `de_platform/modules/persistence/main.py` | Add `metrics`, emit on flush/insert |
| `de_platform/modules/algos/main.py` | Add `metrics`, emit on evaluate/alert |
| `de_platform/modules/data_api/main.py` | Add `metrics`, emit on HTTP request/response |
| `de_platform/modules/rest_starter/main.py` | Add `metrics`, emit on ingestion |
| `de_platform/modules/kafka_starter/main.py` | Add `metrics`, emit on ingestion |
| `de_platform/modules/file_processor/main.py` | Add `metrics`, emit on file processing |
| All module unit tests | Update constructors to pass `MemoryMetrics()` or `NoopMetrics()` |

### 2.2 Structured Logging with Loki

**Current state:** `PrettyLogger` writes to stdout. `MemoryLogger` stores in-memory for tests. No centralized log aggregation.

**Changes:**

1. **New logger implementation:** `de_platform/services/logger/loki_logger.py`
   - Implements `LoggingInterface`
   - Sends structured JSON logs to Grafana Loki's HTTP push API (`/loki/api/v1/push`)
   - Labels: `{service="normalizer", tenant_id="acme", environment="production"}`
   - Log lines include full structured context: event_id, message_id, tenant_id, event_type, processing_stage
   - Batches log entries and flushes periodically (every 1s or 100 entries) to avoid HTTP overhead

2. **Register in registry:**
   ```python
   "log": {
       "pretty": "de_platform.services.logger.pretty_logger.PrettyLogger",
       "memory": "de_platform.services.logger.memory_logger.MemoryLogger",
       "loki": "de_platform.services.logger.loki_logger.LokiLogger",
   }
   ```

3. **Enrich all log calls** across modules with contextual fields:
   - normalizer: `tenant_id`, `event_type`, `event_id`, `message_id`, `primary_key`, `dedup_status`
   - algos: `tenant_id`, `algorithm`, `severity`, `event_id`
   - persistence: `tenant_id`, `table`, `batch_size`
   - data_api: `endpoint`, `tenant_id`, `status_code`

4. **Docker-compose:** Add Loki service

```yaml
# Add to .devcontainer/docker-compose.yml
loki:
    image: grafana/loki:3.0.0
    ports:
        - "3100:3100"
    command: -config.file=/etc/loki/local-config.yaml
```

5. **Grafana datasource:** Add Loki alongside Prometheus in provisioning

**Files:**

| File | Change |
|------|--------|
| `de_platform/services/logger/loki_logger.py` | New — Loki HTTP push logger |
| `de_platform/services/registry.py` | Register `loki` logger |
| `de_platform/modules/*/main.py` | Enrich log calls with context fields |
| `.devcontainer/docker-compose.yml` | Add Loki service |
| `.devcontainer/grafana/provisioning/datasources/loki.yml` | New — Loki datasource for Grafana |

### 2.3 Grafana Dashboards

**Dashboard per service** provisioned as JSON files in `.devcontainer/grafana/dashboards/`:

| Dashboard | Panels |
|-----------|--------|
| `pipeline_overview.json` | End-to-end: ingestion rate → normalization rate → persistence rate → alert rate. Error rate across all stages. Top-level health view. |
| `normalizer.json` | Events/sec (received vs processed), error rate, dedup rate (internal vs external), avg processing time, breakdown by tenant |
| `algos.json` | Alerts/min by algorithm, by severity, by tenant. Processing latency. Events evaluated/sec. |
| `persistence.json` | Rows inserted/sec to ClickHouse, batch sizes, flush latency, buffer sizes |
| `data_api.json` | HTTP request rate, latency percentiles (p50/p95/p99), error rate by endpoint |
| `test_runs.json` | Test-specific: per-test metrics snapshots, pipeline stage throughput during test execution, failure annotations |

**Metrics source:** Prometheus (already in docker-compose, scraping `app:9091`). All modules now emit metrics via `MetricsInterface` (from 2.1).

**Log source:** Loki (from 2.2). Each dashboard includes a "Logs" panel with relevant LogQL queries.

**Grafana provisioning** — dashboard JSON files are mounted into Grafana container:

```yaml
# .devcontainer/grafana/provisioning/dashboards/dashboards.yml
apiVersion: 1
providers:
  - name: 'default'
    folder: 'DE Platform'
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

**Internal log viewer:** Grafana's built-in Explore view with Loki datasource. Example query: `{service="normalizer", tenant_id="acme"} |= "error"`. No custom UI needed.

**Files:**

| File | Change |
|------|--------|
| `.devcontainer/grafana/dashboards/pipeline_overview.json` | New |
| `.devcontainer/grafana/dashboards/normalizer.json` | New |
| `.devcontainer/grafana/dashboards/algos.json` | New |
| `.devcontainer/grafana/dashboards/persistence.json` | New |
| `.devcontainer/grafana/dashboards/data_api.json` | New |
| `.devcontainer/grafana/dashboards/test_runs.json` | New |
| `.devcontainer/grafana/provisioning/dashboards/dashboards.yml` | Verify/update |

### 2.4 Test Metrics Infrastructure

**Problem:** When E2E tests fail with a timeout, there's zero visibility into where data got stuck. Was the message published to Kafka? Did the normalizer consume it? Did persistence flush it? You end up writing ad-hoc debug scripts.

**Solution:** A `TestDiagnostics` class that provides a unified view of pipeline state, working with both ContainerHarness (in-process) and SubprocessHarness (OS processes).

#### 2.4.1 TestDiagnostics class

**New file:** `tests/helpers/diagnostics.py`

```python
@dataclass
class PipelineSnapshot:
    """Point-in-time snapshot of pipeline state."""
    timestamp: float
    kafka_topics: dict[str, TopicState]      # topic → {high_watermark, low_watermark, messages_available}
    db_tables: dict[str, int]                 # table → row count
    module_status: dict[str, ModuleStatus]    # module_name → {alive, exit_code, pid}
    metrics: dict[str, float] | None          # metric_name → value (from MemoryMetrics or Prometheus scrape)

@dataclass
class TopicState:
    high_watermark: int
    low_watermark: int
    messages_available: int  # high - low

@dataclass
class ModuleStatus:
    alive: bool
    exit_code: int | None
    pid: int | None

class TestDiagnostics:
    """Collects pipeline state for test debugging."""

    def __init__(
        self,
        kafka_bootstrap: str,
        clickhouse_db: ClickHouseDatabase | None = None,
        postgres_db: PostgresDatabase | None = None,
        metrics: MemoryMetrics | None = None,
        prometheus_endpoints: dict[str, str] | None = None,  # module_name → http://host:port/metrics
        processes: dict[str, subprocess.Popen] | None = None,
        tasks: dict[str, asyncio.Task] | None = None,
    ): ...

    def snapshot(self) -> PipelineSnapshot:
        """Take a point-in-time snapshot of all pipeline state."""
        ...

    def kafka_watermarks(self) -> dict[str, TopicState]:
        """Query Kafka topic watermarks via AdminClient."""
        ...

    def db_row_counts(self) -> dict[str, int]:
        """Query row counts from ClickHouse and Postgres tables."""
        ...

    def module_health(self) -> dict[str, ModuleStatus]:
        """Check if each module process/task is alive."""
        ...

    def scrape_prometheus(self, endpoint: str) -> dict[str, float]:
        """Scrape a Prometheus /metrics endpoint and parse into dict."""
        ...

    def format_snapshot(self, snap: PipelineSnapshot) -> str:
        """Format snapshot as human-readable text for terminal output."""
        ...
```

#### 2.4.2 ContainerHarness integration

- Inject a shared `MemoryMetrics` instance into all modules via the DI container
- Store reference in `ContainerHarness.diagnostics: TestDiagnostics`
- `diagnostics.snapshot()` reads MemoryMetrics counters directly (in-process, instant)
- Also queries Kafka watermarks and DB row counts for ground truth

#### 2.4.3 SubprocessHarness integration

- Each subprocess module launched with `--metrics prometheus`
- Each gets a unique Prometheus port (allocated via `_free_port()`)
- `SubprocessHarness.diagnostics: TestDiagnostics` stores the port mapping
- `diagnostics.snapshot()` scrapes each module's `/metrics` endpoint + queries Kafka/DB directly
- On module crash, capture and include stderr output

**Files:**

| File | Change |
|------|--------|
| `tests/helpers/diagnostics.py` | New — TestDiagnostics, PipelineSnapshot, helpers |
| `tests/helpers/harness.py` (ContainerHarness) | Initialize TestDiagnostics with shared MemoryMetrics, pass to modules |
| `tests/helpers/harness.py` (SubprocessHarness) | Add `--metrics prometheus` to module launch, allocate Prometheus ports, initialize TestDiagnostics |

### 2.5 Enhanced Timeout Diagnostics

**Problem:** `TimeoutError: wait_for_rows: condition not met within timeout` tells you nothing about why.

**Changes:**

Modify all polling/waiting functions in both harnesses to capture and include a `PipelineSnapshot` on timeout:

1. **`wait_for_rows()`**: On timeout, capture snapshot and include in error message:
   ```
   TimeoutError: wait_for_rows('orders', expected=100) timed out after 120s

   Pipeline state at timeout:
     Kafka topics:
       trade_normalization: 100 messages (high=100, low=0) — messages published but not consumed
       orders_persistence:    0 messages (high=0, low=0)
     DB tables:
       orders:       0 rows
       executions:   0 rows
       duplicates:   0 rows
       norm_errors:  0 rows
     Module status:
       normalizer:   DEAD (exit code 1)
       persistence:  alive (pid 12345)
       algos:        alive (pid 12346)
     Metrics (normalizer):
       events_received_total:   0
       events_processed_total:  0
   ```

2. **`wait_for_alert()`**: Same pattern — snapshot on timeout.

3. **Health check waiters** (`_wait_for_http()`): On timeout, check if the process is still alive and include stderr if it crashed:
   ```
   TimeoutError: Health check failed for normalizer at http://127.0.0.1:8080/health/startup after 45s
   Process status: DEAD (exit code 1)
   stderr: ModuleNotFoundError: No module named 'confluent_kafka'
   ```

4. **`_check_tasks_alive()` in ContainerHarness**: When a task has failed, include the exception traceback.

**Implementation:** Wrap the existing `poll_until()` function (or its callers) to catch `TimeoutError`, enrich with diagnostics, and re-raise.

**Files:**

| File | Change |
|------|--------|
| `tests/helpers/harness.py` | Enrich all timeout paths with `diagnostics.snapshot()` context |
| `tests/helpers/diagnostics.py` | `format_snapshot()` produces the human-readable output |

### 2.6 Test Report Generation

**Problem:** After a 35-minute E2E test run, you want a summary of what happened — which tests passed, which failed, what the pipeline state looked like, and where to dig deeper.

**Solution:** A pytest plugin that collects diagnostics on every test and generates reports in three formats.

#### 2.6.1 pytest plugin

**New file:** `tests/helpers/pytest_pipeline_report.py`

Registered as a conftest plugin (or via `pyproject.toml` `[tool.pytest.ini_options] plugins`).

**Hooks:**
- `pytest_runtest_setup`: Record start timestamp, take "before" snapshot if harness is available
- `pytest_runtest_teardown`: Record end timestamp, take "after" snapshot, compute deltas
- `pytest_runtest_makereport`: Attach snapshot data to the test report item
- `pytest_sessionfinish`: Generate JSON, HTML, and terminal reports

**Per-test data collected:**

```python
@dataclass
class TestRunData:
    test_name: str
    status: str  # "passed", "failed", "error", "skipped"
    duration_seconds: float
    start_snapshot: PipelineSnapshot | None
    end_snapshot: PipelineSnapshot | None
    delta: PipelineSnapshotDelta | None  # messages produced, rows inserted, etc.
    failure_message: str | None
    failure_traceback: str | None

@dataclass
class PipelineSnapshotDelta:
    kafka_messages_produced: dict[str, int]  # topic → count
    db_rows_inserted: dict[str, int]         # table → count
    metrics_deltas: dict[str, float]         # metric → change
```

#### 2.6.2 JSON report

Written to `reports/pipeline-report-{timestamp}.json` after each test session.

```json
{
  "session": {
    "start_time": "2026-02-21T10:00:00",
    "duration_seconds": 2096.6,
    "total": 99,
    "passed": 98,
    "failed": 1,
    "skipped": 0
  },
  "tests": [
    {
      "name": "test_valid_events[rest-order]",
      "status": "passed",
      "duration_seconds": 12.3,
      "delta": {
        "kafka_messages_produced": {"trade_normalization": 100, "orders_persistence": 100},
        "db_rows_inserted": {"orders": 100},
        "metrics_deltas": {"events_processed_total": 100}
      }
    },
    {
      "name": "test_velocity_algorithm",
      "status": "failed",
      "duration_seconds": 60.0,
      "failure_message": "TimeoutError: wait_for_alert: condition not met within timeout",
      "end_snapshot": {
        "kafka_topics": {"trades_algos": {"high_watermark": 101, "low_watermark": 101}},
        "db_tables": {"alerts": 0},
        "metrics": {"events_received_total": 101, "alerts_generated_total": 0}
      }
    }
  ]
}
```

#### 2.6.3 HTML report

Written to `reports/pipeline-report-{timestamp}.html`. Self-contained (inline CSS/JS).

**Sections:**
- **Summary bar**: total/passed/failed/duration with color-coded status
- **Test table**: sortable by name, status, duration. Click to expand details.
- **Per-test details** (expandable): snapshot deltas, pipeline stage flow visualization (simple bar chart showing message counts per topic), failure traceback
- **Pipeline overview**: aggregate metrics across all tests — total events processed, total alerts, error rates

**Implementation:** Use a simple Jinja2 template (or raw string templating to avoid adding a dependency). Include minimal inline CSS for styling and vanilla JS for sorting/expanding.

#### 2.6.4 Grafana test annotations

Push test results as Grafana annotations so they appear on dashboards:

- **On test start:** POST annotation with `tags: ["test-start", test_name]`
- **On test end:** POST annotation with `tags: ["test-end", test_name, status]`
- **On session end:** POST annotation with summary

This lets you correlate test execution with Prometheus metrics and Loki logs on Grafana dashboards. When a test fails, you can see exactly which metrics spiked or dropped during that test's execution window.

**API:** Grafana HTTP API (`POST /api/annotations`) — no auth needed for local dev Grafana (admin/admin).

#### 2.6.5 Terminal summary

Printed to stdout after every test session (always, not just on failure):

```
Pipeline E2E Test Report
========================
Duration: 34m 56s | Tests: 99 | Passed: 98 | Failed: 1

Failed tests:
  test_velocity_algorithm (60.0s)
    TimeoutError: wait_for_alert: condition not met within timeout
    Kafka: trades_algos=101 msgs consumed | alerts=0 msgs produced
    DB: alerts=0 rows
    Metrics: events_received=101, alerts_generated=0

Reports written:
  JSON: reports/pipeline-report-20260221-100000.json
  HTML: reports/pipeline-report-20260221-100000.html
```

**Files:**

| File | Change |
|------|--------|
| `tests/helpers/pytest_pipeline_report.py` | New — pytest plugin for report generation |
| `tests/helpers/report_template.html` | New — Jinja2/string template for HTML report |
| `tests/helpers/diagnostics.py` | Add `PipelineSnapshotDelta`, delta computation |
| `tests/e2e/conftest.py` | Register the plugin, wire up harness diagnostics |
| `pyproject.toml` | Add `reports/` to gitignore, register pytest plugin |
| `.gitignore` | Add `reports/` directory |

### 2.7 Summary of all files for Phase 2

**New files:**

| File | Purpose |
|------|---------|
| `de_platform/services/logger/loki_logger.py` | Loki HTTP push logger |
| `.devcontainer/grafana/provisioning/datasources/loki.yml` | Loki datasource for Grafana |
| `.devcontainer/grafana/dashboards/pipeline_overview.json` | Pipeline overview dashboard |
| `.devcontainer/grafana/dashboards/normalizer.json` | Normalizer dashboard |
| `.devcontainer/grafana/dashboards/algos.json` | Algos dashboard |
| `.devcontainer/grafana/dashboards/persistence.json` | Persistence dashboard |
| `.devcontainer/grafana/dashboards/data_api.json` | Data API dashboard |
| `.devcontainer/grafana/dashboards/test_runs.json` | Test runs dashboard |
| `tests/helpers/diagnostics.py` | TestDiagnostics, PipelineSnapshot, state queries |
| `tests/helpers/pytest_pipeline_report.py` | pytest plugin for report generation |
| `tests/helpers/report_template.html` | HTML report template |

**Modified files:**

| File | Change |
|------|--------|
| `de_platform/cli/runner.py` | Register `NoopMetrics` as default MetricsInterface |
| `de_platform/services/registry.py` | Register `loki` logger |
| `de_platform/modules/normalizer/main.py` | Add metrics + enriched logging |
| `de_platform/modules/persistence/main.py` | Add metrics + enriched logging |
| `de_platform/modules/algos/main.py` | Add metrics + enriched logging |
| `de_platform/modules/data_api/main.py` | Add metrics + enriched logging |
| `de_platform/modules/rest_starter/main.py` | Add metrics + enriched logging |
| `de_platform/modules/kafka_starter/main.py` | Add metrics + enriched logging |
| `de_platform/modules/file_processor/main.py` | Add metrics + enriched logging |
| `tests/helpers/harness.py` | Integrate TestDiagnostics, enrich timeouts |
| `tests/e2e/conftest.py` | Register pytest plugin, wire diagnostics |
| `.devcontainer/docker-compose.yml` | Add Loki service |
| `.gitignore` | Add `reports/` |
| `pyproject.toml` | Register pytest plugin |
| All module unit tests | Update constructors for MetricsInterface |

---

## Phase 3: Client Configuration Service

**Goal:** Centralized configuration for tenants — realtime/batch mode, algo thresholds, available algos, algo run times. This is the foundation for phases 5-7.

### 3.1 Client Configuration Service (#7)

**New module:** `de_platform/modules/client_config/`

**Postgres database:** New migration namespace `client_config` with its own tables.

**Schema:**

```sql
-- de_platform/migrations/client_config/001_create_clients.up.sql
CREATE TABLE IF NOT EXISTS clients (
    tenant_id       TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    mode            TEXT NOT NULL DEFAULT 'batch',  -- 'realtime' | 'batch'
    algo_run_hour   INTEGER,                        -- hour of day for batch algo runs (0-23)
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- de_platform/migrations/client_config/002_create_algo_config.up.sql
CREATE TABLE IF NOT EXISTS client_algo_config (
    tenant_id       TEXT NOT NULL REFERENCES clients(tenant_id),
    algorithm       TEXT NOT NULL,                  -- algo name (e.g., 'large_notional')
    enabled         BOOLEAN NOT NULL DEFAULT true,
    thresholds      JSONB NOT NULL DEFAULT '{}',    -- algo-specific thresholds
    PRIMARY KEY (tenant_id, algorithm)
);
```

**REST API endpoints:**

```
GET    /api/v1/clients                         — list all clients
GET    /api/v1/clients/{tenant_id}             — get client config
POST   /api/v1/clients                         — create client
PUT    /api/v1/clients/{tenant_id}             — update client config
DELETE /api/v1/clients/{tenant_id}             — delete client

GET    /api/v1/clients/{tenant_id}/algos       — get algo config for client
PUT    /api/v1/clients/{tenant_id}/algos/{algo} — update algo thresholds/enabled
```

**Module structure:**

```
de_platform/modules/client_config/
    __init__.py
    main.py         # ClientConfigModule(Module) — aiohttp server
    module.json     # type: "service"
    tests/
        __init__.py
        test_main.py
```

**Port:** 8003 (configurable via `--port`)

### 3.2 Client Configuration Fields (#3, #4, #5, #17, #18)

All configuration fields are stored in the `clients` and `client_algo_config` tables:

- **#5 Realtime/Batch mode:** `clients.mode` column (`'realtime'` | `'batch'`)
- **#17 Algo run time:** `clients.algo_run_hour` column (integer 0-23)
- **#18 Available algos:** Rows in `client_algo_config` with `enabled` boolean
- **#4 Adjustable thresholds:** `client_algo_config.thresholds` JSONB column. Each algo defines what thresholds it supports:
  - `large_notional`: `{"threshold_usd": 1000000}`
  - `velocity`: `{"max_events": 100, "window_seconds": 60}`
  - `suspicious_counterparty`: `{"suspicious_ids": ["id1", "id2"]}`

### 3.3 Config Caching with Redis Pub-Sub (#6)

**Problem:** Services (normalizer, algos) need client config at runtime but can't query Postgres on every event.

**Strategy:** Fetch on startup + periodic refresh + Redis pub-sub for immediate updates.

**Changes:**

1. **New shared module:** `de_platform/pipeline/client_config_cache.py`
   - `ClientConfigCache` class that wraps `CacheInterface`
   - On startup: fetches all client configs from the client_config service REST API, stores in Redis with keys like `client_config:{tenant_id}` and `algo_config:{tenant_id}:{algo}`
   - Periodic refresh: every 60 seconds (configurable), re-fetch all configs
   - Redis pub-sub: subscribe to `client_config_updates` channel. When the client_config service updates a client, it publishes the tenant_id to this channel. Subscribers fetch the updated config

2. **Client config service changes:** On every PUT/POST/DELETE, publish the affected `tenant_id` to `client_config_updates` Redis channel

3. **Consumer services:** Normalizer and algos modules instantiate `ClientConfigCache` during `initialize()` and use it to check:
   - Normalizer: should we forward events to algos topics? (only if `mode == 'realtime'`)
   - Algos: which algos are enabled for this tenant? What thresholds?

**Files:**

| File | Change |
|------|--------|
| `de_platform/pipeline/client_config_cache.py` | New — cache layer with pub-sub |
| `de_platform/modules/client_config/main.py` | Publish to Redis on config changes |
| `de_platform/modules/normalizer/main.py` | Check `mode` before publishing to algos topics |
| `de_platform/modules/algos/main.py` | Load per-tenant algo config (enabled, thresholds) |
| `de_platform/pipeline/algorithms.py` | Accept thresholds dict in constructor, override defaults |

---

## Phase 4: Authentication System

**Goal:** JWT-based auth protecting all API endpoints. Multi-user, multi-tenant — users belong to a tenant and can only access their tenant's data.

### 4.1 Auth Service (#2)

**New module:** `de_platform/modules/auth/`

**Postgres database:** New migration namespace `auth`.

**Schema:**

```sql
-- de_platform/migrations/auth/001_create_users.up.sql
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    user_id         TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(tenant_id),
    email           TEXT UNIQUE NOT NULL,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'viewer',  -- 'admin' | 'editor' | 'viewer'
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_id        TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(user_id),
    expires_at      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**REST API endpoints:**

```
POST /api/v1/auth/login           — { email, password } → { access_token, refresh_token }
POST /api/v1/auth/refresh         — { refresh_token } → { access_token }
POST /api/v1/auth/logout          — revoke refresh token
GET  /api/v1/auth/me              — current user info (requires auth)
POST /api/v1/auth/users           — create user (admin only)
GET  /api/v1/auth/users           — list users for tenant (admin only)
```

**JWT structure:**
```json
{
  "sub": "user_id",
  "tenant_id": "acme",
  "role": "editor",
  "exp": 1800,
  "iat": "..."
}
```

**Dependencies to add:** `pyjwt>=2.8`, `bcrypt>=4.0` (add to `[project.dependencies]` since auth is core)

**Port:** 8004 (configurable)

### 4.2 Auth Middleware

**New shared module:** `de_platform/pipeline/auth_middleware.py`

- aiohttp middleware function that extracts `Authorization: Bearer <token>` header
- Validates JWT signature and expiration
- Injects `tenant_id` and `user_id` into `request["user"]`
- Skips auth for whitelisted paths (`/api/v1/auth/login`, `/api/v1/auth/refresh`, `/health`)

**Apply to:**
- `data_api` module — all `/api/v1/*` endpoints get tenant-scoped filtering
- `client_config` module — admin-only access
- Future UI static files — `/ui/*` paths (no auth, JWT checked client-side)

### 4.3 Tenant-Scoped Data Access

- All API query endpoints (`data_api`, `client_config`) extract `tenant_id` from the JWT rather than accepting it as a query parameter
- Admin users can optionally pass `?tenant_id=X` to access other tenants
- Database queries filtered by `tenant_id` from the token

---

## Phase 5: Algorithms Rewrite

**Goal:** Algos run on configurable-size sliding windows of time-ordered data. Two execution modes: realtime (streaming) and batch (historical).

### 5.1 Algorithm Interface Redesign (#8)

**Current state:** Each algo's `evaluate()` receives a single event and returns an Alert or None. This works for simple threshold checks but doesn't support windowed analysis.

**New interface:**

```python
# de_platform/pipeline/algorithms.py

class FraudAlgorithm(ABC):
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[Alert]:
        """Evaluate a batch of time-ordered events within a window.

        Returns zero or more Alerts.
        """
        ...

    def evaluate(self, event: dict[str, Any]) -> Alert | None:
        """Legacy single-event evaluation. Default delegates to evaluate_window."""
        alerts = self.evaluate_window(
            [event], event.get("tenant_id", ""),
            datetime.min, datetime.max,
        )
        return alerts[0] if alerts else None
```

- All existing algos (`LargeNotionalAlgo`, `VelocityAlgo`, `SuspiciousCounterpartyAlgo`) rewritten to implement `evaluate_window()` operating on a list of events
- The single-event `evaluate()` is kept as a convenience that delegates to `evaluate_window([event])`
- Algos accept thresholds from `client_algo_config` rather than hardcoded defaults

**Sliding window configuration (per-client):**
- `window_size_minutes`: size of each evaluation window (default: 5)
- `window_slide_minutes`: how much the window advances (default: 1)
- Stored in `client_algo_config.thresholds` JSONB

### 5.2 Realtime Algo Execution (#9 — streaming path)

**Module:** `de_platform/modules/algos/main.py` (existing, modified)

**In-memory buffer per tenant:**

```python
# Per-tenant event buffer for windowed analysis
self.buffers: dict[str, list[dict]] = defaultdict(list)
```

- On each consumed event, append to `self.buffers[tenant_id]`
- Sort buffer by `transact_time`
- When buffer for a tenant has events spanning >= `window_size_minutes`, evaluate the window
- Slide the window: remove events older than `window_end - window_size_minutes`
- Use `ClientConfigCache` to get the tenant's enabled algos and thresholds

**Flow:**
```
Kafka (trades_algos/transactions_algos)
  → buffer per tenant
  → window full? → evaluate_window() for each enabled algo
  → publish alerts
```

### 5.3 Batch Algo Execution (#9 — historical path)

**New module:** `de_platform/modules/batch_algos/`

```
de_platform/modules/batch_algos/
    __init__.py
    main.py         # BatchAlgosModule(Module)
    module.json     # type: "job"
    tests/
        __init__.py
        test_main.py
```

**Execution:** Takes a tenant_id, start_date, end_date as args. Queries ClickHouse for all events in that range, then runs the sliding window algorithm across the entire dataset.

**CLI:**
```bash
python -m de_platform run batch_algos \
    --db events=clickhouse --db alerts=postgres --cache redis \
    --tenant-id acme --start-date 2026-01-01 --end-date 2026-01-31
```

**Algorithm:**
1. Query ClickHouse: `SELECT * FROM orders WHERE tenant_id = $1 AND transact_time BETWEEN $2 AND $3 ORDER BY transact_time`
2. Same for executions and transactions
3. Iterate with sliding window (configurable size, default 5 min) across the time range
4. For each window position, call `evaluate_window()` on all enabled algos for this tenant
5. Collect all alerts, deduplicate by (algorithm, event_id) to avoid re-alerting
6. Insert new alerts into Postgres and publish to Kafka

**Performance:** For a full day of data, expect ~288 windows (5-min windows, 1-min slide over 24h). Each window fetches data from the already-loaded dataset in memory — no repeated DB queries.

### 5.4 Update Existing Algo Tests

- Unit tests: test `evaluate_window()` with multi-event batches
- Test that per-client thresholds override defaults
- Test that disabled algos are skipped
- Test sliding window progression (events enter and leave the window correctly)

---

## Phase 6: Data Audit Service

**Goal:** Track incoming data volumes, processed counts, errors, and duplicates per tenant. Queryable by the UI.

### 6.1 Data Audit Service (#13)

**New module:** `de_platform/modules/data_audit/`

**Postgres database:** New migration namespace `data_audit`.

**Schema:**

```sql
-- de_platform/migrations/data_audit/001_create_audit_tables.up.sql

-- Per-file audit (for file uploads)
CREATE TABLE IF NOT EXISTS file_audit (
    id              SERIAL PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    source          TEXT NOT NULL,           -- 'file_upload'
    event_type      TEXT NOT NULL,           -- 'order', 'execution', 'transaction'
    total_count     INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'processing',  -- 'processing' | 'completed' | 'failed'
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Per-day audit (for REST and Kafka)
CREATE TABLE IF NOT EXISTS daily_audit (
    id              SERIAL PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    date            DATE NOT NULL,
    source          TEXT NOT NULL,           -- 'rest' | 'kafka'
    event_type      TEXT NOT NULL,
    total_count     INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, date, source, event_type)
);
```

**How counts are calculated:**

The data audit service consumes from multiple Kafka topics to track counts:

- **Incoming:** Count messages on `trade_normalization` and `tx_normalization` topics
- **Processed:** Count messages on persistence topics (`orders_persistence`, `executions_persistence`, `transactions_persistence`)
- **Errors:** Count messages on `normalization_errors` topic
- **Duplicates:** Count messages on `duplicates` topic

The service subscribes to all these topics with its own consumer group and increments counters in its Postgres database. This is a separate path from the actual processing — it just counts.

**REST API endpoints:**

```
GET /api/v1/audit/daily?tenant_id=X&date=2026-01-15&source=rest
GET /api/v1/audit/daily?tenant_id=X&start_date=2026-01-01&end_date=2026-01-31
GET /api/v1/audit/files?tenant_id=X
GET /api/v1/audit/files/{file_id}
GET /api/v1/audit/summary?tenant_id=X   — totals across all sources/dates
```

**Port:** 8005 (configurable)

### 6.2 Pipeline Integration

- **`file_processor` module:** On file processing start, create a `file_audit` row. On completion, update counts and status.
- **`rest_starter` / `kafka_starter`:** Publish a lightweight "ingestion event" to a new `ingestion_audit` topic with metadata (tenant_id, source, event_type, count). Data audit service consumes this.
- Alternative: data audit service directly counts from existing topics (simpler, no pipeline changes needed). Prefer this approach.

---

## Phase 7: Task Scheduler

**Goal:** Centralized task scheduling and tracking. Schedule recurring jobs (currency fetch, batch algos), track run history, support custom argument generation per task.

### 7.1 Task Scheduler Module (#16)

**New module:** `de_platform/modules/task_scheduler/`

**Dependencies to add:** `apscheduler>=3.10` (add to `[project.dependencies]`)

**Postgres database:** New migration namespace `task_scheduler`.

**Schema:**

```sql
-- de_platform/migrations/task_scheduler/001_create_task_tables.up.sql
CREATE TABLE IF NOT EXISTS task_definitions (
    task_id         TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    module_name     TEXT NOT NULL,               -- de_platform module to run
    default_args    JSONB NOT NULL DEFAULT '{}', -- default CLI args
    schedule_cron   TEXT,                         -- cron expression (NULL = manual only)
    enabled         BOOLEAN NOT NULL DEFAULT true,
    arg_generator   TEXT,                         -- dotted path to custom arg generator function
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id          TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES task_definitions(task_id),
    status          TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'running' | 'completed' | 'failed'
    args            JSONB NOT NULL DEFAULT '{}',      -- actual args for this run
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    exit_code       INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**How it works:**

1. On startup, load all `task_definitions` from Postgres
2. For each task with a `schedule_cron`, register it with APScheduler
3. When a task triggers (scheduled or manual):
   a. If `arg_generator` is set, import and call it to get dynamic args
   b. Create a `task_runs` row with status `pending`
   c. Execute the module as a subprocess: `python -m de_platform run <module_name> <args>`
   d. Update `task_runs` with status, exit_code, timestamps

**Custom arg generators:**

For batch algos scheduling, we need dynamic date ranges:

```python
# de_platform/pipeline/task_arg_generators.py

def batch_algos_args(task_def: dict, last_run: dict | None) -> dict:
    """Generate args for the next batch algo run.

    Increments the date range from where the last run left off.
    """
    if last_run and last_run.get("args"):
        last_end = last_run["args"]["end_date"]
        start = last_end  # pick up where we left off
    else:
        start = "2026-01-01"  # first run
    end = (datetime.fromisoformat(start) + timedelta(hours=1)).isoformat()[:10]
    return {"start_date": start, "end_date": end, "tenant_id": task_def["default_args"]["tenant_id"]}
```

**REST API endpoints:**

```
GET    /api/v1/tasks                     — list task definitions
GET    /api/v1/tasks/{task_id}           — get task definition
POST   /api/v1/tasks                     — create task definition
PUT    /api/v1/tasks/{task_id}           — update task definition
DELETE /api/v1/tasks/{task_id}           — delete task definition
POST   /api/v1/tasks/{task_id}/run       — trigger manual run
GET    /api/v1/tasks/{task_id}/runs      — list runs for a task
GET    /api/v1/runs/{run_id}             — get run details
```

**Port:** 8006 (configurable)

### 7.2 Pre-configured Tasks

Seed the `task_definitions` table with:

| task_id | module_name | schedule_cron | Notes |
|---------|-------------|---------------|-------|
| `currency_fetch` | `currency_loader` | `0 * * * *` (hourly) | Fetch latest currency rates |
| `batch_algos_{tenant}` | `batch_algos` | Configured per client via `algo_run_hour` | Dynamic — one task per batch-mode client |
| `file_processor` | `file_processor` | Manual trigger | Already tracked as a task run |

---

## Phase 8: React UI

**Goal:** React + TypeScript frontend served by data_api, communicating with all backend services through a unified API gateway.

### 8.1 UI Application (#1, #3)

**Tech stack:** React 18+ with TypeScript, Vite build tool, TanStack Query for data fetching, React Router for navigation, Tailwind CSS for styling.

**Directory structure:**

```
ui/
    package.json
    tsconfig.json
    vite.config.ts
    src/
        main.tsx
        App.tsx
        api/               # API client with JWT auth
            client.ts
            auth.ts
        pages/
            LoginPage.tsx
            AlertsPage.tsx
            EventsPage.tsx
            DataAuditPage.tsx
            ClientConfigPage.tsx
            TasksPage.tsx
        components/
            Layout.tsx
            DataTable.tsx
            Filters.tsx
            Pagination.tsx
```

**Pages:**

| Page | Data source | Description |
|------|-------------|-------------|
| Login | Auth service | Email/password login, stores JWT in localStorage |
| Alerts Dashboard | Data API | Table of alerts with severity, algorithm, tenant filters. Click for details. |
| Events Explorer | Data API | Search orders/executions/transactions by date, tenant. Paginated table. |
| Data Audit | Data Audit service | Per-day and per-file counts: incoming, processed, errors, duplicates. |
| Client Configuration | Client Config service | CRUD for clients: mode, algo thresholds, run time, enabled algos. Admin only. |
| Task Manager | Task Scheduler | View task definitions, trigger runs, see run history with status. |

**Build integration:**

- `make build-ui` runs `cd ui && npm run build` → outputs to `de_platform/modules/data_api/static/`
- data_api already serves `/ui/` from the `static/` directory
- During development: `cd ui && npm run dev` with proxy to `localhost:8002` for API calls

**API client:**

```typescript
// ui/src/api/client.ts
const BASE_URL = '/api/v1';

async function fetchWithAuth(path: string, options?: RequestInit) {
    const token = localStorage.getItem('access_token');
    const res = await fetch(`${BASE_URL}${path}`, {
        ...options,
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
            ...options?.headers,
        },
    });
    if (res.status === 401) {
        // attempt token refresh, redirect to login if failed
    }
    return res;
}
```

### 8.2 API Gateway / Proxy (#1)

**Problem:** The UI runs from data_api (port 8002) but needs to reach auth (8004), client_config (8003), data_audit (8005), and task_scheduler (8006).

**Solution:** data_api module acts as the API gateway. Add reverse proxy routes:

```python
# In data_api's _create_app():
app.router.add_route("*", "/api/v1/auth/{path:.*}", self._proxy_auth)
app.router.add_route("*", "/api/v1/clients/{path:.*}", self._proxy_client_config)
app.router.add_route("*", "/api/v1/audit/{path:.*}", self._proxy_data_audit)
app.router.add_route("*", "/api/v1/tasks/{path:.*}", self._proxy_task_scheduler)
app.router.add_route("*", "/api/v1/runs/{path:.*}", self._proxy_task_scheduler)
```

Each proxy function forwards the request (with headers) to the target service using `aiohttp.ClientSession`. This keeps a single entry point for the UI.

**Alternative:** Use nginx or Traefik as a reverse proxy in docker-compose. But since data_api already serves the UI, having it proxy API calls is simpler for development.

### 8.3 Playwright E2E Tests (#12)

**Dependencies to add:** `playwright` to `[project.optional-dependencies.dev]`

**Test directory:** `tests/e2e_ui/`

**Test scenarios:**

| Test | What it verifies |
|------|-----------------|
| `test_login.py` | Login page → enter credentials → redirected to dashboard |
| `test_alerts.py` | Insert alerts via API → alerts page shows them → filter by severity |
| `test_events.py` | Insert events via pipeline → events page shows them → filter by date/type |
| `test_data_audit.py` | Run pipeline → data audit page shows correct counts |
| `test_client_config.py` | Create/edit/delete client config through the UI |

**Fixture:**
```python
@pytest.fixture(scope="session")
def browser():
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch()
        yield browser
        browser.close()
```

---

## Phase 9: Test Reorganization & Parallelism

**Goal:** Separate algo tests from data tests, enable parallel test execution.

### 9.1 Reorganize E2E Tests (#11)

**Current state:** `tests/e2e/test_pipeline.py` mixes data flow tests (valid events, invalid events, duplicates) with algo tests (large_notional, velocity, suspicious_counterparty).

**New structure:**

```
tests/
    e2e/
        conftest.py                   # shared infra fixtures (unchanged)
        test_data_pipeline.py         # scenarios: valid_events, invalid_events, duplicate_events,
                                      #   internal_dedup, multi_error_consolidation,
                                      #   duplicate_contains_original_event
        test_algo_pipeline.py         # scenarios: large_notional, velocity,
                                      #   suspicious_counterparty, alert_via_method
        test_real_infra_data.py       # real-infra versions of data tests
        test_real_infra_algos.py      # real-infra versions of algo tests
    e2e_ui/
        conftest.py
        test_login.py
        test_alerts.py
        ...
    integration/
        test_postgres.py
        test_clickhouse.py
        test_redis.py
        test_kafka.py
        test_minio.py
    helpers/
        harness.py
        events.py
        scenarios.py                  # shared scenario functions (unchanged)
        diagnostics.py                # from Phase 2
        pytest_pipeline_report.py     # from Phase 2
```

### 9.2 Parallel E2E Tests (#19)

**Problem:** Tests run sequentially. Running concurrently requires either per-test client isolation or per-test execution venue isolation.

**Approach:** Per-test tenant isolation.

**Changes:**

1. Each test creates a unique `tenant_id` (e.g., `test-{uuid}`)
2. All events ingested with that unique tenant_id
3. All assertions query by that tenant_id
4. No cross-test data contamination even when running in parallel

**Implementation:**

- Add `pytest-xdist` to dev dependencies
- Add `make test-e2e-parallel` target: `pytest tests/e2e/ -n auto`
- Update `tests/helpers/harness.py`: `PipelineHarness.__init__` takes an optional `tenant_id` parameter (defaults to `f"test-{uuid4().hex[:8]}"`)
- Update `tests/helpers/events.py`: all factory functions accept `tenant_id` parameter
- Update `tests/helpers/scenarios.py`: all scenarios use `harness.tenant_id` instead of hardcoded `"acme"`
- Cleanup fixture: instead of truncating all tables, each test only cleans up its own tenant's data (or we skip cleanup entirely since unique tenant_ids prevent collision)

**Caveat:** Real-infra tests share Kafka topics. With unique tenant_ids, messages from different tests co-exist on the same topic. Each test's harness filters consumed messages by tenant_id. This is slightly more complex but avoids the topic-deletion overhead.

### 9.3 E2E Tests for New Features (#10)

Add e2e test scenarios for each new service:

| Test file | Scenarios |
|-----------|-----------|
| `test_client_config.py` | CRUD client, update thresholds, verify cache invalidation |
| `test_auth.py` | Login, access with token, tenant isolation, token refresh, expired token |
| `test_data_audit.py` | Ingest data → verify counts in audit service |
| `test_batch_algos.py` | Insert historical data → run batch algos → verify alerts |
| `test_task_scheduler.py` | Create task, trigger run, verify run history |

---

## Execution Order & Dependencies

```
Phase 1: Foundation ✅ COMPLETE
    ├── 1.1 Consolidate Module/AsyncModule
    ├── 1.2 Crash resilience
    ├── 1.3 Integration tests
    └── 1.4 Test execution fixes

Phase 2: Observability & Test Debugging (depends on: Phase 1)
    ├── 2.1 Module metrics emission
    ├── 2.2 Structured logging with Loki
    ├── 2.3 Grafana dashboards
    ├── 2.4 Test metrics infrastructure
    ├── 2.5 Enhanced timeout diagnostics
    └── 2.6 Test report generation

Phase 3: Client Config (depends on: Phase 1)
    ├── 3.1 Client config service
    ├── 3.2 Config fields
    └── 3.3 Redis pub-sub caching

Phase 4: Auth (depends on: Phase 1)
    ├── 4.1 Auth service
    ├── 4.2 Auth middleware
    └── 4.3 Tenant-scoped access

Phase 5: Algos Rewrite (depends on: Phase 3)
    ├── 5.1 Algorithm interface redesign
    ├── 5.2 Realtime execution (buffer)
    ├── 5.3 Batch execution (ClickHouse)
    └── 5.4 Algo tests

Phase 6: Data Audit (depends on: Phase 1)
    ├── 6.1 Data audit service
    └── 6.2 Pipeline integration

Phase 7: Task Scheduler (depends on: Phase 5 for batch_algos)
    ├── 7.1 Task scheduler module
    └── 7.2 Pre-configured tasks

Phase 8: React UI (depends on: Phase 3, 4, 6, 7)
    ├── 8.1 UI application
    ├── 8.2 API gateway
    └── 8.3 Playwright tests

Phase 9: Test Reorg (depends on: Phase 8 for UI tests, otherwise Phase 2)
    ├── 9.1 Reorganize tests
    ├── 9.2 Parallel tests
    └── 9.3 New feature tests
```

**Recommended implementation sequence:**

1. **Phase 1** — Foundation ✅ COMPLETE
2. **Phase 2** — Observability & Test Debugging (unblocks debuggable E2E for all future phases)
3. **Phase 3** — Client config (needed by algos rewrite and UI)
4. **Phase 4** — Auth (can parallelize with Phase 3)
5. **Phase 6** — Data audit (independent, can parallelize with 3/4)
6. **Phase 5** — Algos rewrite (needs Phase 3 for config)
7. **Phase 7** — Task scheduler (needs Phase 5 for batch algos)
8. **Phase 8** — UI (needs 3, 4, 6, 7 — do this last or build incrementally as services are ready)
9. **Phase 9** — Test reorg (ongoing, finalize after all features)

---

## New Dependencies Summary

| Package | Group | Purpose |
|---------|-------|---------|
| `pyjwt>=2.8` | core | JWT token encoding/decoding |
| `bcrypt>=4.0` | core | Password hashing |
| `apscheduler>=3.10` | core | Task scheduling (cron) |
| `playwright` | dev | Browser-based UI e2e tests |
| `pytest-xdist` | dev | Parallel test execution |

## New Docker-Compose Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `loki` | `grafana/loki:3.0.0` | 3100 | Log aggregation |

## New Database Namespaces

| Namespace | Tables | Used by |
|-----------|--------|---------|
| `client_config` | `clients`, `client_algo_config` | Client config service |
| `auth` | `tenants`, `users`, `refresh_tokens` | Auth service |
| `data_audit` | `file_audit`, `daily_audit` | Data audit service |
| `task_scheduler` | `task_definitions`, `task_runs` | Task scheduler |

## New Modules

| Module | Type | Port | Purpose |
|--------|------|------|---------|
| `client_config` | service | 8003 | Client configuration CRUD + Redis pub-sub |
| `auth` | service | 8004 | JWT authentication + user management |
| `data_audit` | service | 8005 | Data volume tracking + audit queries |
| `task_scheduler` | service | 8006 | APScheduler + task run history |
| `batch_algos` | job | — | Historical algo execution on ClickHouse data |
