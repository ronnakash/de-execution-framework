"""Pytest plugin for pipeline test reporting.

Collects per-test diagnostics (snapshots, deltas, timing) and generates
JSON + HTML + terminal reports after each test session.

Supports pytest-xdist: workers write partial results to temp files,
the controller aggregates them into a single report.

Registered automatically via conftest.py or pyproject.toml.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.diagnostics import PipelineSnapshot, TestDiagnostics

# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class PipelineSnapshotDelta:
    """Change between two pipeline snapshots."""

    kafka_messages_produced: dict[str, int] = field(default_factory=dict)
    db_rows_inserted: dict[str, int] = field(default_factory=dict)
    metrics_deltas: dict[str, float] = field(default_factory=dict)


@dataclass
class TestRunData:
    """Data collected for a single test run."""

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
    steps: list[dict] = field(default_factory=list)


# ── Delta computation ────────────────────────────────────────────────────────


def compute_delta(
    before: PipelineSnapshot | None, after: PipelineSnapshot | None
) -> PipelineSnapshotDelta | None:
    """Compute the difference between two snapshots."""
    if before is None or after is None:
        return None

    delta = PipelineSnapshotDelta()

    # Kafka / MQ messages
    for topic in set(before.kafka_topics) | set(after.kafka_topics):
        before_count = before.kafka_topics.get(topic)
        after_count = after.kafka_topics.get(topic)
        b = before_count.high_watermark if before_count else 0
        a = after_count.high_watermark if after_count else 0
        if a - b > 0:
            delta.kafka_messages_produced[topic] = a - b

    # DB rows
    for table in set(before.db_tables) | set(after.db_tables):
        b = before.db_tables.get(table, 0)
        a = after.db_tables.get(table, 0)
        if a - b > 0:
            delta.db_rows_inserted[table] = a - b

    # Metrics
    for name in set(before.metrics) | set(after.metrics):
        b = before.metrics.get(name, 0)
        a = after.metrics.get(name, 0)
        if a - b != 0:
            delta.metrics_deltas[name] = a - b

    return delta


def snapshot_to_dict(snap: PipelineSnapshot | None) -> dict | None:
    """Convert a snapshot to a JSON-serializable dict."""
    if snap is None:
        return None
    return {
        "timestamp": snap.timestamp,
        "kafka_topics": {
            t: {"high": s.high_watermark, "low": s.low_watermark, "available": s.messages_available}
            for t, s in snap.kafka_topics.items()
        },
        "db_tables": snap.db_tables,
        "metrics": snap.metrics,
    }


# ── xdist helpers ────────────────────────────────────────────────────────────


def _is_xdist_worker() -> bool:
    """True if running as a pytest-xdist worker process."""
    return os.environ.get("PYTEST_XDIST_WORKER") is not None


def _is_xdist_controller(session: pytest.Session) -> bool:
    """True if running as the pytest-xdist controller process."""
    return hasattr(session.config, "workerinput") is False and _xdist_partials_dir() is not None


def _xdist_partials_dir() -> Path | None:
    """Return the shared temp dir for xdist partial results, or None."""
    d = os.environ.get("PIPELINE_REPORT_PARTIALS_DIR")
    if d:
        return Path(d)
    return None


# ── Plugin implementation ────────────────────────────────────────────────────


class PipelineReportPlugin:
    """Pytest plugin that collects pipeline diagnostics and generates reports."""

    def __init__(self) -> None:
        self._tests: list[TestRunData] = []
        self._current: TestRunData | None = None
        self._session_start: float = 0.0
        self._diagnostics: TestDiagnostics | None = None
        self._partials_dir: str | None = None

    def set_diagnostics(self, diag: TestDiagnostics) -> None:
        """Set the diagnostics instance (called by harness setup)."""
        self._diagnostics = diag

    @pytest.hookimpl(tryfirst=True)
    def pytest_sessionstart(self, session: pytest.Session) -> None:
        self._session_start = time.time()

        # If we're the main process (not a worker), create a shared temp dir
        # for xdist workers to write partial results to.
        if not _is_xdist_worker():
            tmpdir = tempfile.mkdtemp(prefix="pipeline-report-")
            self._partials_dir = tmpdir
            os.environ["PIPELINE_REPORT_PARTIALS_DIR"] = tmpdir

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_setup(self, item: pytest.Item) -> None:
        self._current = TestRunData(test_name=item.nodeid)
        self._current.start_time = time.time()
        if self._diagnostics:
            try:
                self._current.start_snapshot = snapshot_to_dict(
                    self._diagnostics.snapshot()
                )
            except Exception:
                pass

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_makereport(self, item: pytest.Item, call: Any) -> None:
        if self._current is None:
            return
        if call.when == "setup" and call.excinfo is not None:
            # Setup failure (e.g., skip via importorskip or pytest.skip)
            if call.excinfo.errisinstance(pytest.skip.Exception):
                self._current.status = "skipped"
            else:
                self._current.status = "error"
                self._current.failure_message = str(call.excinfo.value)
                self._current.failure_traceback = str(call.excinfo.getrepr())
        elif call.when == "call":
            if call.excinfo is not None:
                if call.excinfo.errisinstance(pytest.skip.Exception):
                    self._current.status = "skipped"
                else:
                    self._current.status = "failed"
                    self._current.failure_message = str(call.excinfo.value)
                    self._current.failure_traceback = str(call.excinfo.getrepr())
            else:
                self._current.status = "passed"

    @pytest.hookimpl(trylast=True)
    def pytest_runtest_teardown(self, item: pytest.Item) -> None:
        if self._current is None:
            return
        self._current.end_time = time.time()
        self._current.duration_seconds = (
            self._current.end_time - self._current.start_time
        )

        if self._diagnostics:
            try:
                end_snap = self._diagnostics.snapshot()
                self._current.end_snapshot = snapshot_to_dict(end_snap)
            except Exception:
                pass

        # Collect step data from harness (if available)
        try:
            for fixture_value in item.funcargs.values():
                if hasattr(fixture_value, 'step_logger'):
                    self._current.steps = fixture_value.step_logger.to_dicts()
                    break
        except Exception:
            pass

        self._tests.append(self._current)
        self._current = None

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        if _is_xdist_worker():
            # Worker: write partial results to shared temp dir
            self._write_worker_partial()
            return

        # Main process or controller: aggregate and generate report
        duration = time.time() - self._session_start

        # Combine local test data with any xdist worker partials
        all_tests: list[dict] = [
            {
                "name": t.test_name,
                "status": t.status,
                "duration_seconds": round(t.duration_seconds, 2),
                **({"failure_message": t.failure_message} if t.failure_message else {}),
                **({"failure_traceback": t.failure_traceback} if t.failure_traceback else {}),
                **({"end_snapshot": t.end_snapshot} if t.end_snapshot else {}),
                **({"steps": t.steps} if t.steps else {}),
            }
            for t in self._tests
        ]
        partials_dir = self._partials_dir or os.environ.get("PIPELINE_REPORT_PARTIALS_DIR")
        if partials_dir:
            all_tests.extend(self._read_worker_partials(Path(partials_dir)))

        if not all_tests:
            return

        passed = sum(1 for t in all_tests if t["status"] == "passed")
        failed = sum(1 for t in all_tests if t["status"] == "failed")
        errors = sum(1 for t in all_tests if t["status"] == "error")
        skipped = sum(1 for t in all_tests if t["status"] == "skipped")
        total = len(all_tests)

        report_data = {
            "session": {
                "start_time": datetime.fromtimestamp(
                    self._session_start, tz=timezone.utc
                ).isoformat(),
                "duration_seconds": round(duration, 1),
                "total": total,
                "passed": passed,
                "failed": failed,
                "errors": errors,
                "skipped": skipped,
            },
            "tests": all_tests,
        }

        # Write reports
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

        # JSON report
        json_path = reports_dir / f"pipeline-report-{ts}.json"
        json_path.write_text(json.dumps(report_data, indent=2, default=str))

        # HTML report
        html_path = reports_dir / f"pipeline-report-{ts}.html"
        html_path.write_text(_generate_html(report_data))

        # Terminal summary
        _print_terminal_summary(report_data, json_path, html_path)

        # Grafana annotations (best-effort)
        _push_grafana_annotations(all_tests, self._session_start, duration)

        # Clean up partials dir
        if partials_dir:
            import shutil
            try:
                shutil.rmtree(partials_dir, ignore_errors=True)
            except Exception:
                pass

    def _write_worker_partial(self) -> None:
        """Write this worker's test results to a JSON file in the shared dir."""
        partials_dir = _xdist_partials_dir()
        if partials_dir is None:
            return

        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "unknown")
        tests_data = [
            {
                "name": t.test_name,
                "status": t.status,
                "duration_seconds": round(t.duration_seconds, 2),
                **({"failure_message": t.failure_message} if t.failure_message else {}),
                **({"failure_traceback": t.failure_traceback} if t.failure_traceback else {}),
                **({"end_snapshot": t.end_snapshot} if t.end_snapshot else {}),
                **({"steps": t.steps} if t.steps else {}),
            }
            for t in self._tests
        ]

        partials_dir.mkdir(parents=True, exist_ok=True)
        partial_path = partials_dir / f"worker-{worker_id}.json"
        partial_path.write_text(json.dumps(tests_data, default=str))

    @staticmethod
    def _read_worker_partials(partials_dir: Path) -> list[dict]:
        """Read and merge all worker partial result files."""
        all_tests: list[dict] = []
        if not partials_dir.exists():
            return all_tests

        for partial_file in sorted(partials_dir.glob("worker-*.json")):
            try:
                worker_tests = json.loads(partial_file.read_text())
                all_tests.extend(worker_tests)
            except Exception:
                pass

        return all_tests


# ── Terminal summary ─────────────────────────────────────────────────────────


def _print_terminal_summary(
    data: dict, json_path: Path, html_path: Path
) -> None:
    """Print a concise terminal summary."""
    s = data["session"]
    mins = int(s["duration_seconds"]) // 60
    secs = int(s["duration_seconds"]) % 60

    errors = s.get("errors", 0)
    lines = [
        "",
        "Pipeline Test Report",
        "=" * 40,
        f"Duration: {mins}m {secs}s | Tests: {s['total']} | "
        f"Passed: {s['passed']} | Failed: {s['failed']} | "
        f"Errors: {errors} | Skipped: {s['skipped']}",
    ]

    failed_tests = [t for t in data["tests"] if t["status"] in ("failed", "error")]
    if failed_tests:
        lines.append("")
        lines.append("Failed tests:")
        for t in failed_tests:
            lines.append(f"  {t['name']} ({t['duration_seconds']}s)")
            if t.get("failure_message"):
                msg = t["failure_message"][:200]
                lines.append(f"    {msg}")

    lines.extend([
        "",
        "Reports written:",
        f"  JSON: {json_path}",
        f"  HTML: {html_path}",
        "",
    ])

    print("\n".join(lines))


# ── HTML report ──────────────────────────────────────────────────────────────


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


def _render_steps_html(steps: list[dict]) -> str:
    """Render step detail HTML for a test's expandable section."""
    if not steps:
        return ""

    parts: list[str] = ['<div class="steps">']
    for i, step in enumerate(steps, 1):
        name = _html_escape(step.get("name", ""))
        duration = step.get("duration_seconds", 0)
        description = step.get("description", "") or _default_description(name)
        error = step.get("error")
        delta = step.get("delta") or {}

        parts.append('<div class="step">')
        parts.append(
            f'<div class="step-header">'
            f'<strong>Step {i}: {name}</strong>'
            f'<span class="step-duration">{duration:.3f}s</span>'
            f'</div>'
        )
        if description:
            parts.append(f'<p class="step-description">{_html_escape(description)}</p>')

        # Kafka delta table
        kafka = delta.get("kafka", {})
        if kafka:
            parts.append('<table class="delta-table">')
            parts.append("<tr><th>Kafka Topic</th><th>Messages</th></tr>")
            for topic, count in sorted(kafka.items()):
                sign = "+" if count > 0 else ""
                parts.append(f"<tr><td>{_html_escape(topic)}</td><td>{sign}{count} msgs</td></tr>")
            parts.append("</table>")

        # DB delta table
        db = delta.get("db", {})
        if db:
            parts.append('<table class="delta-table">')
            parts.append("<tr><th>DB Table</th><th>Rows</th></tr>")
            for table, count in sorted(db.items()):
                sign = "+" if count > 0 else ""
                parts.append(f"<tr><td>{_html_escape(table)}</td><td>{sign}{count} rows</td></tr>")
            parts.append("</table>")

        # Metrics delta table
        metrics = delta.get("metrics", {})
        if metrics:
            parts.append('<table class="delta-table">')
            parts.append("<tr><th>Metric</th><th>Change</th></tr>")
            for metric, val in sorted(metrics.items()):
                sign = "+" if val > 0 else ""
                parts.append(f"<tr><td>{_html_escape(metric)}</td><td>{sign}{val}</td></tr>")
            parts.append("</table>")

        # Error box
        if error:
            parts.append(
                f'<div class="failure-detail" style="margin-top:0.5rem">'
                f'<pre>{_html_escape(error)}</pre></div>'
            )

        parts.append("</div>")  # .step

    parts.append("</div>")  # .steps
    return "\n".join(parts)


def _generate_html(data: dict) -> str:
    """Generate a self-contained HTML report with search, filters, and drill-down."""
    s = data["session"]
    mins = int(s["duration_seconds"]) // 60
    secs = int(s["duration_seconds"]) % 60

    passed = s["passed"]
    failed = s["failed"]
    errors = s.get("errors", 0)
    skipped = s.get("skipped", 0)
    total = s["total"]

    test_rows: list[str] = []
    for i, t in enumerate(data["tests"]):
        status = t["status"]
        name = _html_escape(t["name"])
        duration = t["duration_seconds"]
        steps = t.get("steps", [])
        step_count = len(steps)
        step_label = f"{step_count} steps" if step_count else ""

        # Main test row
        test_rows.append(
            f'<tr class="test-row {status}" data-status="{status}" onclick="toggleDetail({i})">'
            f'<td><span class="chevron" id="chev-{i}">&#9654;</span> {name}</td>'
            f'<td><span class="badge {status}">{status}</span></td>'
            f'<td>{duration}s</td>'
            f'<td>{step_label}</td>'
            f'</tr>'
        )

        # Detail row (hidden by default)
        detail_parts: list[str] = ['<div class="detail-content">']

        # Steps
        if steps:
            detail_parts.append(_render_steps_html(steps))

        # Failure traceback
        if t.get("failure_traceback"):
            detail_parts.append(
                '<div class="failure-detail">'
                '<h4>Failure Traceback</h4>'
                f'<pre>{_html_escape(t["failure_traceback"])}</pre>'
                '</div>'
            )
        elif t.get("failure_message"):
            detail_parts.append(
                '<div class="failure-detail">'
                '<h4>Failure</h4>'
                f'<pre>{_html_escape(t["failure_message"])}</pre>'
                '</div>'
            )

        # End snapshot
        if t.get("end_snapshot"):
            snap = t["end_snapshot"]
            snap_lines: list[str] = []
            if snap.get("metrics"):
                snap_lines.append("Metrics:")
                for k, v in sorted(snap["metrics"].items()):
                    snap_lines.append(f"  {k}: {v}")
            if snap.get("db_tables"):
                snap_lines.append("DB Tables:")
                for k, v in sorted(snap["db_tables"].items()):
                    snap_lines.append(f"  {k}: {v} rows")
            if snap_lines:
                detail_parts.append(
                    '<div class="snapshot">'
                    '<h4>End Snapshot</h4>'
                    f'<pre>{_html_escape(chr(10).join(snap_lines))}</pre>'
                    '</div>'
                )

        detail_parts.append('</div>')

        test_rows.append(
            f'<tr class="test-detail" id="detail-{i}" style="display:none">'
            f'<td colspan="4">{"".join(detail_parts)}</td>'
            f'</tr>'
        )

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Pipeline Test Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 2rem; background: #f5f5f5; }}
h1 {{ color: #1f2937; margin-bottom: 1.5rem; }}

/* Summary cards */
.summary {{ display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }}
.summary .card {{ padding: 1rem 2rem; border-radius: 8px; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.summary .total {{ font-size: 2rem; font-weight: bold; }}
.summary .label {{ color: #666; font-size: 0.9rem; }}
.passed-count {{ color: #22c55e; }}
.failed-count {{ color: #ef4444; }}

/* Toolbar */
.toolbar {{ display: flex; gap: 1rem; margin-bottom: 1.5rem; align-items: center; flex-wrap: wrap; }}
.toolbar input {{ padding: 0.5rem 1rem; border: 1px solid #d1d5db; border-radius: 6px;
                 font-size: 0.95rem; width: 300px; }}
.status-filters {{ display: flex; gap: 0.5rem; }}
.filter-btn {{ padding: 0.4rem 1rem; border: 1px solid #d1d5db; border-radius: 6px;
              background: white; cursor: pointer; font-size: 0.85rem; }}
.filter-btn.active {{ background: #1f2937; color: white; border-color: #1f2937; }}

/* Table */
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
th {{ background: #1f2937; color: white; padding: 0.75rem 1rem; text-align: left; }}
td {{ padding: 0.75rem 1rem; border-bottom: 1px solid #e5e7eb; }}

/* Clickable rows */
.test-row {{ cursor: pointer; }}
.test-row:hover {{ background: #f9fafb; }}
.chevron {{ display: inline-block; transition: transform 0.2s; font-size: 0.7rem; margin-right: 0.5rem; }}
.chevron.open {{ transform: rotate(90deg); }}

/* Status badges */
.badge {{ padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }}
.badge.passed {{ background: #dcfce7; color: #166534; }}
.badge.failed {{ background: #fee2e2; color: #991b1b; }}
.badge.error {{ background: #fef3c7; color: #92400e; }}
.badge.skipped {{ background: #e5e7eb; color: #374151; }}

/* Detail panel */
.detail-content {{ padding: 1rem; }}
.step {{ margin-bottom: 1rem; padding: 0.75rem; background: #f9fafb; border-radius: 6px;
        border-left: 3px solid #3b82f6; }}
.step-header {{ display: flex; justify-content: space-between; align-items: center; }}
.step-duration {{ color: #6b7280; font-size: 0.85rem; }}
.step-description {{ color: #6b7280; font-size: 0.9rem; margin: 0.25rem 0 0.5rem 0; }}

/* Delta tables */
.delta-table {{ width: auto; margin: 0.5rem 0; font-size: 0.85rem; border-collapse: collapse; }}
.delta-table th {{ background: #e5e7eb; color: #374151; padding: 0.3rem 0.75rem; }}
.delta-table td {{ padding: 0.3rem 0.75rem; border-bottom: 1px solid #e5e7eb; }}

/* Failure detail */
.failure-detail {{ margin-top: 1rem; padding: 0.75rem; border-left: 3px solid #ef4444;
                  background: #fef2f2; border-radius: 0 6px 6px 0; }}
.failure-detail pre {{ white-space: pre-wrap; word-break: break-word; font-size: 0.8rem; margin: 0; }}
.failure-detail h4 {{ margin: 0 0 0.5rem 0; color: #991b1b; font-size: 0.9rem; }}

/* Snapshot */
.snapshot {{ margin-top: 1rem; }}
.snapshot h4 {{ margin: 0 0 0.5rem 0; color: #374151; font-size: 0.9rem; }}
.snapshot pre {{ background: #f9fafb; padding: 0.75rem; border-radius: 6px; font-size: 0.8rem;
               overflow-x: auto; margin: 0; }}
</style>
</head>
<body>
<h1>Pipeline Test Report</h1>
<div class="summary">
  <div class="card"><div class="total">{total}</div><div class="label">Total</div></div>
  <div class="card"><div class="total passed-count">{passed}</div><div class="label">Passed</div></div>
  <div class="card"><div class="total failed-count">{failed}</div><div class="label">Failed</div></div>
  <div class="card"><div class="total" style="color: #f59e0b;">{errors}</div><div class="label">Errors</div></div>
  <div class="card"><div class="total">{mins}m {secs}s</div><div class="label">Duration</div></div>
</div>

<div class="toolbar">
  <input type="text" id="search" placeholder="Filter tests by name..." oninput="filterTests()">
  <div class="status-filters">
    <button class="filter-btn active" onclick="filterStatus('all')">All ({total})</button>
    <button class="filter-btn" onclick="filterStatus('passed')">Passed ({passed})</button>
    <button class="filter-btn" onclick="filterStatus('failed')">Failed ({failed + errors})</button>
    <button class="filter-btn" onclick="filterStatus('skipped')">Skipped ({skipped})</button>
  </div>
</div>

<table>
<thead><tr><th>Test</th><th>Status</th><th>Duration</th><th>Steps</th></tr></thead>
<tbody>
{''.join(test_rows)}
</tbody>
</table>

<script>
function toggleDetail(index) {{
    var detail = document.getElementById('detail-' + index);
    var chevron = document.getElementById('chev-' + index);
    if (detail.style.display === 'none') {{
        detail.style.display = 'table-row';
        chevron.classList.add('open');
    }} else {{
        detail.style.display = 'none';
        chevron.classList.remove('open');
    }}
}}

function filterTests() {{
    var query = document.getElementById('search').value.toLowerCase();
    var rows = document.querySelectorAll('.test-row');
    rows.forEach(function(row, i) {{
        var name = row.querySelector('td').textContent.toLowerCase();
        var match = name.includes(query);
        row.style.display = match ? '' : 'none';
        document.getElementById('detail-' + i).style.display = 'none';
        var chevron = document.getElementById('chev-' + i);
        if (chevron) chevron.classList.remove('open');
    }});
}}

var currentFilter = 'all';
function filterStatus(status) {{
    currentFilter = status;
    document.querySelectorAll('.filter-btn').forEach(function(btn) {{
        btn.classList.toggle('active', btn.textContent.toLowerCase().indexOf(status) === 0);
    }});
    var rows = document.querySelectorAll('.test-row');
    rows.forEach(function(row, i) {{
        var rowStatus = row.dataset.status;
        var match = status === 'all' || rowStatus === status
                    || (status === 'failed' && (rowStatus === 'failed' || rowStatus === 'error'));
        row.style.display = match ? '' : 'none';
        document.getElementById('detail-' + i).style.display = 'none';
        var chevron = document.getElementById('chev-' + i);
        if (chevron) chevron.classList.remove('open');
    }});
}}
</script>
</body>
</html>"""


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Grafana annotations ─────────────────────────────────────────────────────


def _push_grafana_annotations(
    tests: list,
    session_start: float,
    session_duration: float,
) -> None:
    """Push test results as Grafana annotations (best-effort)."""
    grafana_url = os.environ.get("GRAFANA_URL", "http://localhost:3000")
    try:
        import urllib.request

        # Session summary annotation
        passed = sum(1 for t in tests if (t.status if hasattr(t, "status") else t.get("status")) == "passed")
        failed = sum(1 for t in tests if (t.status if hasattr(t, "status") else t.get("status")) == "failed")
        summary = {
            "time": int(session_start * 1000),
            "timeEnd": int((session_start + session_duration) * 1000),
            "tags": ["test-session", f"passed:{passed}", f"failed:{failed}"],
            "text": f"Test session: {passed} passed, {failed} failed",
        }
        req = urllib.request.Request(
            f"{grafana_url}/api/annotations",
            data=json.dumps(summary).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Basic YWRtaW46YWRtaW4=",  # admin:admin
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception:
        pass  # Best-effort — don't fail tests if Grafana is unavailable
