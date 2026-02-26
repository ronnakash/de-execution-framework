# Phase 15: Test Report & Live Dashboard

**Goal:** Enhanced HTML report with per-test drill-down, step logger for recording named pipeline steps within tests, and a Grafana dashboard for live test monitoring.

---

## Context

The current `tests/helpers/pytest_pipeline_report.py` generates basic HTML reports with:

- Summary cards (total / passed / failed / errors / duration)
- A flat table with test name, status, duration, and failure message
- End snapshots with metrics (but no per-step detail)

What is missing:

- Clickable test rows with step-by-step execution detail
- Kafka topic watermark deltas per step
- DB row count deltas per step
- Descriptive explanations of what each step does
- Search and filter bar for navigating large test suites
- Step-by-step recording infrastructure within tests

---

## Implementation Details

### Step 1: Test Step Logger

**New file:** `tests/helpers/step_logger.py`

An async context manager that records named steps within a test. Each step captures a before/after `PipelineSnapshot` from `TestDiagnostics` and computes deltas for Kafka topics, DB row counts, and metrics.

#### Data model

```python
@dataclass
class StepRecord:
    """A single recorded step."""
    name: str
    description: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0
    snapshot_before: dict | None = None
    snapshot_after: dict | None = None
    delta: dict | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "duration_seconds": round(self.duration_seconds, 3),
            "delta": self.delta,
            "error": self.error,
        }
```

#### StepLogger class

```python
class StepLogger:
    """Records named steps within a test for report drill-down."""

    def __init__(self, diagnostics: TestDiagnostics | None = None) -> None:
        self._diagnostics = diagnostics
        self._steps: list[StepRecord] = []

    @asynccontextmanager
    async def step(self, name: str, description: str = ""):
        """Context manager to record a named step."""
        record = StepRecord(name=name, description=description)
        record.start_time = time.time()

        # Take before snapshot
        if self._diagnostics:
            try:
                record.snapshot_before = _snapshot_to_dict(self._diagnostics.snapshot())
            except Exception:
                pass

        try:
            yield record
        except Exception as e:
            record.error = str(e)
            raise
        finally:
            record.end_time = time.time()
            record.duration_seconds = record.end_time - record.start_time

            # Take after snapshot
            if self._diagnostics:
                try:
                    snap_after = self._diagnostics.snapshot()
                    record.snapshot_after = _snapshot_to_dict(snap_after)
                except Exception:
                    pass

            # Compute delta between before and after
            if record.snapshot_before and record.snapshot_after:
                record.delta = _compute_step_delta(
                    record.snapshot_before, record.snapshot_after
                )

            self._steps.append(record)

    @property
    def steps(self) -> list[StepRecord]:
        return list(self._steps)

    def to_dicts(self) -> list[dict]:
        return [s.to_dict() for s in self._steps]
```

#### Delta computation helper

`_compute_step_delta(before, after)` computes a dict with three keys:

- `kafka` -- per-topic message count increases (from `kafka_topics.*.high` field)
- `db` -- per-table row count changes (from `db_tables` field, can be negative for deletes)
- `metrics` -- per-metric value changes (from `metrics` field)

Only non-zero deltas are included. The function uses the same snapshot dict format already produced by `snapshot_to_dict()` in `pytest_pipeline_report.py`.

#### Snapshot conversion

`_snapshot_to_dict()` delegates to the existing `snapshot_to_dict()` function in `pytest_pipeline_report.py`, which converts `PipelineSnapshot` to a JSON-serializable dict with keys `timestamp`, `kafka_topics`, `db_tables`, `metrics`.

#### Usage in tests

```python
async with step_logger.step("Ingest 100 orders via REST") as s:
    await harness.ingest("rest", "order", events)

async with step_logger.step("Wait for persistence") as s:
    rows = await harness.wait_for_rows("orders", 100)
```

---

### Step 2: Add `step()` to PipelineHarness

**File:** `tests/helpers/harness.py`

#### PipelineHarness Protocol

Add `step()` to the protocol so all harness implementations expose it:

```python
class PipelineHarness(Protocol):
    tenant_id: str
    step_logger: StepLogger  # NEW

    # ... existing methods ...

    def step(self, name: str, description: str = "") -> AsyncContextManager: ...
```

#### MemoryHarness changes

In `__init__`, after `self.diagnostics` is set (inside `_ensure_modules()`), create the step logger:

```python
# In _ensure_modules(), after self.diagnostics is assigned:
self.step_logger = StepLogger(diagnostics=self.diagnostics)
```

Add the `step()` method:

```python
def step(self, name: str, description: str = ""):
    return self.step_logger.step(name, description)
```

Note: `StepLogger` must be imported at the top of `harness.py`:

```python
from tests.helpers.step_logger import StepLogger
```

The `step_logger` attribute should be initialized to a no-diagnostics `StepLogger()` in `__init__` (before modules are ensured), then re-created with diagnostics once `_ensure_modules()` runs. This avoids attribute errors if `step()` is called before `_ensure_modules()`.

#### RealInfraHarness changes

In `__init__`, initialize a placeholder step logger:

```python
self.step_logger = StepLogger()
```

In `__aenter__`, after `self.diagnostics` is set, re-create it with diagnostics:

```python
self.step_logger = StepLogger(diagnostics=self.diagnostics)
```

Add the same `step()` method:

```python
def step(self, name: str, description: str = ""):
    return self.step_logger.step(name, description)
```

---

### Step 3: Enhanced HTML Report

**File:** `tests/helpers/pytest_pipeline_report.py`

This is the largest change. The existing `_generate_html()` function and `PipelineReportPlugin` class are both modified.

#### 3a. Add `steps` field to `TestRunData`

```python
@dataclass
class TestRunData:
    test_name: str
    status: str = "unknown"
    duration_seconds: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0
    start_snapshot: dict | None = None
    end_snapshot: dict | None = None
    delta: dict | None = None
    failure_message: str | None = None
    failure_traceback: str | None = None
    steps: list[dict] = field(default_factory=list)  # NEW
```

#### 3b. Collect steps in `pytest_runtest_teardown`

In the `pytest_runtest_teardown` hook, after collecting the end snapshot, attempt to retrieve step data from the test's harness. The harness fixture stores itself on the pytest item via `item.stash` or a well-known attribute. The plugin looks for a `step_logger` attribute on the item's function-scoped fixtures:

```python
# In pytest_runtest_teardown, after existing snapshot collection:

# Collect step data from harness (if available)
try:
    # Check for step_logger on fixtures attached to the item
    for fixture_value in item.funcargs.values():
        if hasattr(fixture_value, 'step_logger'):
            self._current.steps = fixture_value.step_logger.to_dicts()
            break
except Exception:
    pass
```

This works because pytest makes fixture values available via `item.funcargs` during teardown. Both `MemoryHarness` and `RealInfraHarness` will have a `step_logger` attribute after step 2.

#### 3c. Include steps in JSON serialization

Update the dict comprehension in `pytest_sessionfinish` and `_write_worker_partial` to include steps:

```python
{
    "name": t.test_name,
    "status": t.status,
    "duration_seconds": round(t.duration_seconds, 2),
    **({"failure_message": t.failure_message} if t.failure_message else {}),
    **({"failure_traceback": t.failure_traceback} if t.failure_traceback else {}),
    **({"end_snapshot": t.end_snapshot} if t.end_snapshot else {}),
    **({"steps": t.steps} if t.steps else {}),
}
```

Note: `failure_traceback` is not currently included in the serialized output. Add it here so the enhanced HTML report can display full tracebacks.

#### 3d. Enhanced `_generate_html()` function

Replace the existing `_generate_html()` with a version that includes:

**Search/filter bar** at the top of the page:
- A text input that filters test rows by name as the user types (JavaScript `oninput` handler)
- Status filter buttons: All, Passed, Failed, Skipped (JavaScript `onclick` handlers)
- Active button state tracking

**Clickable test rows** that expand to show step details:
- Each test `<tr>` has a `data-index` attribute and an `onclick="toggleDetail(N)"` handler
- A hidden detail `<tr>` below each test row with `id="detail-N"` and `style="display:none"`
- JavaScript `toggleDetail(N)` function toggles display between `none` and `table-row`
- A CSS chevron indicator (right-pointing when collapsed, down-pointing when expanded)

**Step-by-step detail panel** inside each expandable row:
- For each step: name, duration, description, and delta tables
- Kafka delta table: topic name, message count increase ("+N msgs")
- DB delta table: table name, row count change ("+N rows" or "-N rows")
- Metrics delta table: metric name, value change
- If a step has an error, display it in a red-bordered box
- If no steps are recorded (test did not use `step()`), show just the end snapshot and failure message

**Pre-populated step descriptions** for common step names. The HTML generator applies default descriptions when a step's `description` field is empty, using keyword matching on the step name:
- Name contains "ingest" --> "Events are published to Kafka normalization topics"
- Name contains "persistence" --> "Normalizer enriches events, persistence module flushes to ClickHouse"
- Name contains "alert" --> "Algos module evaluates events and publishes alerts to Kafka"
- Name contains "dedup" --> "Deduplicator checks for duplicate events via Redis cache"
- Name contains "api" --> "Query the Data API HTTP endpoint"

**Failure detail section:**
- Full traceback displayed in a `<pre>` block inside the expandable section
- Pipeline snapshot at failure time shown below the traceback
- Styled with a left red border and light red background

**Updated HTML structure:**

```html
<!-- Search/filter bar -->
<div class="toolbar">
  <input type="text" id="search" placeholder="Filter tests by name..."
         oninput="filterTests()">
  <div class="status-filters">
    <button class="filter-btn active" onclick="filterStatus('all')">All (N)</button>
    <button class="filter-btn" onclick="filterStatus('passed')">Passed (N)</button>
    <button class="filter-btn" onclick="filterStatus('failed')">Failed (N)</button>
    <button class="filter-btn" onclick="filterStatus('skipped')">Skipped (N)</button>
  </div>
</div>

<!-- Test table -->
<table>
  <thead>
    <tr><th>Test</th><th>Status</th><th>Duration</th><th>Steps</th></tr>
  </thead>
  <tbody>
    <!-- For each test: -->
    <tr class="test-row passed" data-status="passed" onclick="toggleDetail(0)">
      <td><span class="chevron">&#9654;</span> test_name</td>
      <td><span class="badge passed">passed</span></td>
      <td>1.2s</td>
      <td>3 steps</td>
    </tr>
    <tr class="test-detail" id="detail-0" style="display:none">
      <td colspan="4">
        <div class="detail-content">
          <!-- Steps -->
          <div class="steps">
            <div class="step">
              <div class="step-header">
                <strong>Step 1: Ingest 10 orders via REST</strong>
                <span class="step-duration">0.3s</span>
              </div>
              <p class="step-description">
                Events are published to Kafka normalization topics
              </p>
              <table class="delta-table">
                <tr><th>Kafka Topic</th><th>Messages</th></tr>
                <tr><td>trade_normalization</td><td>+10</td></tr>
              </table>
              <table class="delta-table">
                <tr><th>DB Table</th><th>Rows</th></tr>
                <tr><td>clickhouse.orders</td><td>+10</td></tr>
              </table>
            </div>
            <!-- More steps... -->
          </div>

          <!-- Failure traceback (if failed) -->
          <div class="failure-detail">
            <h4>Failure Traceback</h4>
            <pre>...</pre>
          </div>

          <!-- End snapshot -->
          <div class="snapshot">
            <h4>End Snapshot</h4>
            <pre>Metrics: ...</pre>
          </div>
        </div>
      </td>
    </tr>
  </tbody>
</table>
```

**CSS additions** (add to the existing `<style>` block):

```css
/* Toolbar */
.toolbar { display: flex; gap: 1rem; margin-bottom: 1.5rem; align-items: center; flex-wrap: wrap; }
.toolbar input { padding: 0.5rem 1rem; border: 1px solid #d1d5db; border-radius: 6px;
                 font-size: 0.95rem; width: 300px; }
.status-filters { display: flex; gap: 0.5rem; }
.filter-btn { padding: 0.4rem 1rem; border: 1px solid #d1d5db; border-radius: 6px;
              background: white; cursor: pointer; font-size: 0.85rem; }
.filter-btn.active { background: #1f2937; color: white; border-color: #1f2937; }

/* Clickable rows */
.test-row { cursor: pointer; }
.test-row:hover { background: #f9fafb; }
.chevron { display: inline-block; transition: transform 0.2s; font-size: 0.7rem;
           margin-right: 0.5rem; }
.chevron.open { transform: rotate(90deg); }

/* Status badges */
.badge { padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
.badge.passed { background: #dcfce7; color: #166534; }
.badge.failed { background: #fee2e2; color: #991b1b; }
.badge.error { background: #fef3c7; color: #92400e; }
.badge.skipped { background: #e5e7eb; color: #374151; }

/* Detail panel */
.detail-content { padding: 1rem; }
.step { margin-bottom: 1rem; padding: 0.75rem; background: #f9fafb; border-radius: 6px;
        border-left: 3px solid #3b82f6; }
.step-header { display: flex; justify-content: space-between; align-items: center; }
.step-duration { color: #6b7280; font-size: 0.85rem; }
.step-description { color: #6b7280; font-size: 0.9rem; margin: 0.25rem 0 0.5rem 0; }

/* Delta tables */
.delta-table { width: auto; margin: 0.5rem 0; font-size: 0.85rem; }
.delta-table th { background: #e5e7eb; color: #374151; padding: 0.3rem 0.75rem; }
.delta-table td { padding: 0.3rem 0.75rem; }

/* Failure detail */
.failure-detail { margin-top: 1rem; padding: 0.75rem; border-left: 3px solid #ef4444;
                  background: #fef2f2; border-radius: 0 6px 6px 0; }
.failure-detail pre { white-space: pre-wrap; word-break: break-word; font-size: 0.8rem; }
```

**JavaScript** (add inside a `<script>` tag before `</body>`):

```javascript
function toggleDetail(index) {
    const detail = document.getElementById('detail-' + index);
    const row = detail.previousElementSibling;
    const chevron = row.querySelector('.chevron');
    if (detail.style.display === 'none') {
        detail.style.display = 'table-row';
        chevron.classList.add('open');
    } else {
        detail.style.display = 'none';
        chevron.classList.remove('open');
    }
}

function filterTests() {
    const query = document.getElementById('search').value.toLowerCase();
    const rows = document.querySelectorAll('.test-row');
    rows.forEach((row, i) => {
        const name = row.querySelector('td').textContent.toLowerCase();
        const match = name.includes(query);
        row.style.display = match ? '' : 'none';
        document.getElementById('detail-' + i).style.display = 'none';
    });
}

let currentFilter = 'all';
function filterStatus(status) {
    currentFilter = status;
    // Update active button
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.toggle('active', btn.textContent.toLowerCase().startsWith(status));
    });
    // Filter rows
    const rows = document.querySelectorAll('.test-row');
    rows.forEach((row, i) => {
        const rowStatus = row.dataset.status;
        const match = status === 'all' || rowStatus === status;
        row.style.display = match ? '' : 'none';
        document.getElementById('detail-' + i).style.display = 'none';
    });
}
```

#### 3e. Python helper for generating step HTML

Add a helper function `_render_steps_html(steps: list[dict]) -> str` that iterates over the step dicts and produces the HTML for the detail panel. This function:

1. Numbers each step sequentially ("Step 1:", "Step 2:", etc.)
2. Applies default descriptions based on keyword matching if description is empty
3. Renders delta tables only when there are non-empty deltas
4. Renders error boxes only for steps with errors

#### 3f. Python helper for default step descriptions

```python
_DEFAULT_DESCRIPTIONS: list[tuple[str, str]] = [
    ("ingest", "Events are published to Kafka normalization topics"),
    ("persistence", "Normalizer enriches events, persistence module flushes to ClickHouse"),
    ("alert", "Algos module evaluates events and publishes alerts to Kafka"),
    ("dedup", "Deduplicator checks for duplicate events via Redis cache"),
    ("api", "Query the Data API HTTP endpoint"),
    ("normaliz", "Normalizer enriches and routes events to downstream topics"),
]


def _default_description(step_name: str) -> str:
    """Return a default description based on keywords in the step name."""
    lower = step_name.lower()
    for keyword, desc in _DEFAULT_DESCRIPTIONS:
        if keyword in lower:
            return desc
    return ""
```

---

### Step 4: Grafana Live Dashboard

**New file:** `grafana/dashboards/test_live.json`

A new Grafana dashboard for monitoring test runs in real time. This dashboard complements the existing `test_runs.json` dashboard, which focuses on throughput and alert generation. The new dashboard focuses on correlating test session activity with infrastructure state.

#### Dashboard metadata

```json
{
  "uid": "test-live",
  "title": "Test Live Monitor",
  "tags": ["de-platform", "testing", "live"],
  "timezone": "utc",
  "time": { "from": "now-15m", "to": "now" },
  "refresh": "5s",
  "schemaVersion": 39,
  "version": 1,
  "editable": true
}
```

Key settings: `"refresh": "5s"` for live monitoring, short default time window of 15 minutes.

#### Template variables

```json
"templating": {
  "list": [
    {
      "name": "module",
      "type": "query",
      "query": "label_values(events_received_total, service)",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "multi": true,
      "includeAll": true,
      "current": { "text": "All", "value": "$__all" }
    }
  ]
}
```

The `$module` variable allows filtering all panels by module name. A `$test_session` variable is not practical since Prometheus does not label metrics with session IDs, but Grafana annotations (already pushed by `_push_grafana_annotations`) provide session context visually.

#### Annotations

```json
"annotations": {
  "list": [
    {
      "name": "Test Sessions",
      "datasource": { "type": "grafana", "uid": "-- Grafana --" },
      "enable": true,
      "iconColor": "green",
      "type": "tags",
      "tags": ["test-session"],
      "limit": 100
    }
  ]
}
```

This pulls in the annotations pushed by `_push_grafana_annotations()` in `pytest_pipeline_report.py`, which already tags them with `"test-session"`. Each test session appears as a time-range annotation bar on all panels, color-coded by pass/fail tags.

#### Panel 1: Active Tests Timeline (row 0, full width)

- **Type:** `timeseries` with annotations overlay
- **gridPos:** `{ "h": 4, "w": 24, "x": 0, "y": 0 }`
- **Purpose:** Visual timeline showing when test sessions are active
- **Query:** `events_received_total` (just to have a baseline; the annotations provide the test session markers)
- **Note:** This panel mainly serves as an annotation display. The graph shows overall event throughput as context.

#### Panel 2: Per-Module Event Throughput (row 1, left half)

- **Type:** `timeseries`
- **gridPos:** `{ "h": 8, "w": 12, "x": 0, "y": 4 }`
- **Queries:**
  - `rate(events_received_total{service=~"$module"}[5s])` -- legendFormat: `{{service}} received`
  - `rate(events_processed_total{service=~"$module"}[5s])` -- legendFormat: `{{service}} processed`
- **Config:** stacked area chart, `fillOpacity: 20`, unit `ops`
- **Purpose:** Shows bursts of activity during test ingestion. The 5-second rate window gives near-real-time visibility.

#### Panel 3: Error Rate (row 1, right half)

- **Type:** `timeseries`
- **gridPos:** `{ "h": 8, "w": 12, "x": 12, "y": 4 }`
- **Queries:**
  - `rate(events_errors_total{service=~"$module"}[5s])` -- legendFormat: `{{service}} errors`
  - `rate(alerts_generated_total[5s])` -- legendFormat: `alerts generated`
- **Config:** line chart, error line in red via override, alerts in orange
- **Purpose:** Correlate error spikes with test session annotations to identify which tests cause errors.

#### Panel 4: Kafka Consumer Lag (row 2, left half)

- **Type:** `timeseries`
- **gridPos:** `{ "h": 8, "w": 12, "x": 0, "y": 12 }`
- **Queries:**
  - `kafka_consumer_group_lag{group=~"e2e-.*"}` (if kafka-exporter is running)
  - Fallback: `events_received_total - events_processed_total` as an approximation
- **Config:** line chart, unit `short`
- **Purpose:** Shows whether modules are keeping up with ingestion during test runs. High lag indicates a bottleneck.

#### Panel 5: DB Insert Rate (row 2, right half)

- **Type:** `timeseries`
- **gridPos:** `{ "h": 8, "w": 12, "x": 12, "y": 12 }`
- **Queries:**
  - `rate(rows_flushed_total[5s])` -- legendFormat: `{{table}}`
- **Config:** bar chart style (`drawStyle: "bars"`), unit `ops`
- **Purpose:** Shows bursts of ClickHouse inserts during test ingestion. Each bar represents a flush from the persistence module.

#### Panel 6: Duplicate Detection (row 3, left half)

- **Type:** `stat`
- **gridPos:** `{ "h": 4, "w": 12, "x": 0, "y": 20 }`
- **Query:** `duplicates_detected_total`
- **Config:** single stat, color mode `value`, green < 10, yellow < 100, red >= 100
- **Purpose:** Quick overview of how many duplicates have been detected across the session.

#### Panel 7: Events Ingested Total (row 3, right half)

- **Type:** `stat`
- **gridPos:** `{ "h": 4, "w": 12, "x": 12, "y": 20 }`
- **Query:** `events_ingested_total`
- **Config:** single stat, color mode `background`, always blue
- **Purpose:** Total events ingested across all test runs in the time window.

#### Dashboard JSON conventions

Follow the same conventions as the existing dashboards in `grafana/dashboards/`:
- `datasource: { "type": "prometheus", "uid": "prometheus" }` for all Prometheus panels
- Schema version 39
- Use `fieldConfig.defaults.custom` for chart styling
- Use `options.legend` and `options.tooltip` for display options

---

## Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `tests/helpers/step_logger.py` | NEW | `StepLogger` class, `StepRecord` dataclass, `_compute_step_delta()`, `_snapshot_to_dict()` |
| `tests/helpers/harness.py` | MODIFIED | Add `step_logger` attribute and `step()` method to `PipelineHarness` protocol, `MemoryHarness`, and `RealInfraHarness` |
| `tests/helpers/pytest_pipeline_report.py` | MODIFIED | Add `steps` field to `TestRunData`, collect steps in teardown hook, include `failure_traceback` in serialized output, replace `_generate_html()` with enhanced version (clickable rows, search/filter, step detail panels) |
| `grafana/dashboards/test_live.json` | NEW | Grafana dashboard with 7 panels for live test monitoring |

---

## Verification

### 1. Unit tests pass

```bash
make test
```

All existing unit tests must continue to pass. The new `step_logger.py` module has no side effects on existing tests -- harnesses gain a `step_logger` attribute but tests that do not call `step()` simply have an empty step list.

### 2. Step logger works in isolation

Write a small unit test in `tests/unit/test_step_logger.py`:

```python
async def test_step_logger_records_steps():
    logger = StepLogger()
    async with logger.step("step 1", "description") as s:
        pass
    async with logger.step("step 2") as s:
        pass
    assert len(logger.steps) == 2
    assert logger.steps[0].name == "step 1"
    assert logger.steps[0].description == "description"
    assert logger.steps[0].duration_seconds >= 0

async def test_step_logger_records_errors():
    logger = StepLogger()
    with pytest.raises(ValueError):
        async with logger.step("failing step") as s:
            raise ValueError("boom")
    assert len(logger.steps) == 1
    assert logger.steps[0].error == "boom"
```

### 3. HTML report visual verification

Run E2E tests with steps instrumented:

```bash
make test-e2e
```

Then open the generated HTML report in `reports/pipeline-report-*.html` and verify:

- Search bar filters tests by name as you type
- Status filter buttons (All / Passed / Failed / Skipped) correctly show/hide rows
- Clicking a test row expands it to show the detail panel
- Clicking again collapses it
- Step deltas show Kafka topic message counts, DB row changes, and metric changes
- Failed tests show the full traceback in the expanded section
- Steps without explicit descriptions show auto-generated descriptions based on name keywords

### 4. Grafana dashboard loads (if infrastructure is running)

```bash
make infra-up
```

Open Grafana at `http://localhost:3000`, navigate to dashboards, and verify:

- "Test Live Monitor" dashboard appears in the dashboard list
- All 7 panels render without errors (may show "No data" if no tests are running)
- The `$module` dropdown populates with module names when metrics are available
- Test session annotations appear as colored bars when tests are run
- Auto-refresh at 5s interval updates panels in near-real-time

### 5. xdist compatibility

The step data is serialized to JSON in worker partials and aggregated in the controller process. Verify by running:

```bash
pytest tests/e2e/ -n 2 --dist loadscope
```

The generated report should include steps from all workers.
