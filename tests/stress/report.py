"""Benchmark report generation for stress tests.

Generates JSON and HTML reports with latency distribution and throughput data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tests.stress.metrics import StressMetrics


@dataclass
class StressTestResult:
    """Result of a single stress test scenario."""

    scenario_name: str
    tenants: list[str]
    events_per_tenant: int
    event_types: list[str]
    metrics: StressMetrics

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario_name,
            "tenants": self.tenants,
            "events_per_tenant": self.events_per_tenant,
            "event_types": self.event_types,
            "metrics": self.metrics.to_dict(),
        }


def save_report(results: list[StressTestResult], output_dir: str = "reports/stress") -> str:
    """Save JSON and HTML reports. Returns path to the HTML report."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "results": [r.to_dict() for r in results],
    }

    json_path = out / f"stress_{timestamp}.json"
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)

    html_path = out / f"stress_{timestamp}.html"
    with open(html_path, "w") as f:
        f.write(_render_html(data))

    return str(html_path)


def _render_html(data: dict) -> str:
    """Render a simple HTML report."""
    rows = ""
    for r in data["results"]:
        m = r["metrics"]
        lat = m["latency"]
        rows += f"""
        <tr>
            <td>{r['scenario']}</td>
            <td>{', '.join(r['tenants'])}</td>
            <td>{r['events_per_tenant']}</td>
            <td>{m['total_events']}</td>
            <td>{m['duration_seconds']}s</td>
            <td>{m['throughput_eps']}</td>
            <td>{m['errors']}</td>
            <td>{m['error_rate']}</td>
            <td>{lat['mean']}s</td>
            <td>{lat['p95']}s</td>
            <td>{lat['p99']}s</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Stress Test Report</title>
    <style>
        body {{ font-family: -apple-system, sans-serif; margin: 2rem; }}
        h1 {{ color: #1a1a1a; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: right; }}
        th {{ background: #f5f5f5; text-align: center; }}
        td:first-child, td:nth-child(2) {{ text-align: left; }}
        tr:hover {{ background: #f9f9f9; }}
        .meta {{ color: #666; font-size: 0.9rem; margin-bottom: 1rem; }}
    </style>
</head>
<body>
    <h1>Stress Test Report</h1>
    <div class="meta">Generated: {data['generated_at']}</div>
    <table>
        <thead>
            <tr>
                <th>Scenario</th>
                <th>Tenants</th>
                <th>Events/Tenant</th>
                <th>Total Events</th>
                <th>Duration</th>
                <th>Throughput (e/s)</th>
                <th>Errors</th>
                <th>Error Rate</th>
                <th>Mean Latency</th>
                <th>p95 Latency</th>
                <th>p99 Latency</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
</body>
</html>"""
