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
                **({"end_snapshot": t.end_snapshot} if t.end_snapshot else {}),
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
                **({"end_snapshot": t.end_snapshot} if t.end_snapshot else {}),
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


def _generate_html(data: dict) -> str:
    """Generate a self-contained HTML report."""
    s = data["session"]
    mins = int(s["duration_seconds"]) // 60
    secs = int(s["duration_seconds"]) % 60

    test_rows = []
    for t in data["tests"]:
        status_class = t["status"]
        details = ""
        if t.get("failure_message"):
            details = f'<div class="details">{_html_escape(t["failure_message"])}</div>'
        if t.get("end_snapshot"):
            snap = t["end_snapshot"]
            if snap.get("metrics"):
                metrics_lines = [
                    f"  {k}: {v}" for k, v in sorted(snap["metrics"].items())
                ]
                details += f'<div class="details"><pre>Metrics:\n{"chr(10)".join(metrics_lines)}</pre></div>'

        test_rows.append(
            f'<tr class="{status_class}">'
            f"<td>{_html_escape(t['name'])}</td>"
            f"<td>{t['status']}</td>"
            f"<td>{t['duration_seconds']}s</td>"
            f"<td>{details}</td>"
            f"</tr>"
        )

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Pipeline Test Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 2rem; background: #f5f5f5; }}
.summary {{ display: flex; gap: 1rem; margin-bottom: 2rem; }}
.summary .card {{ padding: 1rem 2rem; border-radius: 8px; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.summary .total {{ font-size: 2rem; font-weight: bold; }}
.summary .label {{ color: #666; font-size: 0.9rem; }}
.passed-count {{ color: #22c55e; }}
.failed-count {{ color: #ef4444; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
th {{ background: #1f2937; color: white; padding: 0.75rem 1rem; text-align: left; }}
td {{ padding: 0.75rem 1rem; border-bottom: 1px solid #e5e7eb; }}
tr.passed td:nth-child(2) {{ color: #22c55e; font-weight: bold; }}
tr.failed td:nth-child(2) {{ color: #ef4444; font-weight: bold; }}
tr.failed {{ background: #fef2f2; }}
tr.error td:nth-child(2) {{ color: #f59e0b; font-weight: bold; }}
tr.error {{ background: #fffbeb; }}
.details {{ margin-top: 0.5rem; font-size: 0.85rem; color: #666; }}
.details pre {{ background: #f9fafb; padding: 0.5rem; border-radius: 4px; overflow-x: auto; }}
</style>
</head>
<body>
<h1>Pipeline Test Report</h1>
<div class="summary">
  <div class="card"><div class="total">{s['total']}</div><div class="label">Total</div></div>
  <div class="card"><div class="total passed-count">{s['passed']}</div><div class="label">Passed</div></div>
  <div class="card"><div class="total failed-count">{s['failed']}</div><div class="label">Failed</div></div>
  <div class="card"><div class="total" style="color: #f59e0b;">{s.get('errors', 0)}</div><div class="label">Errors</div></div>
  <div class="card"><div class="total">{mins}m {secs}s</div><div class="label">Duration</div></div>
</div>
<table>
<thead><tr><th>Test</th><th>Status</th><th>Duration</th><th>Details</th></tr></thead>
<tbody>
{''.join(test_rows)}
</tbody>
</table>
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
