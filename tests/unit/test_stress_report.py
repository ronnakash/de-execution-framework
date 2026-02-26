"""Tests for stress test report generation."""

import json
import tempfile
from pathlib import Path

from tests.stress.metrics import StressMetrics
from tests.stress.report import StressTestResult, save_report


def _make_result(name: str = "test_scenario") -> StressTestResult:
    m = StressMetrics()
    m.start()
    for i in range(10):
        m.record_ingestion(float(i))
    m.record_completion(0.0, 0.5)
    m.record_completion(0.0, 1.0)
    m.stop()
    return StressTestResult(
        scenario_name=name,
        tenants=["t1"],
        events_per_tenant=10,
        event_types=["order"],
        metrics=m,
    )


def test_result_to_dict():
    r = _make_result()
    d = r.to_dict()
    assert d["scenario"] == "test_scenario"
    assert d["tenants"] == ["t1"]
    assert d["events_per_tenant"] == 10
    assert "metrics" in d


def test_save_report_creates_files():
    results = [_make_result("s1"), _make_result("s2")]
    with tempfile.TemporaryDirectory() as tmpdir:
        html_path = save_report(results, output_dir=tmpdir)
        assert Path(html_path).exists()
        assert html_path.endswith(".html")

        # JSON file should also exist
        json_files = list(Path(tmpdir).glob("*.json"))
        assert len(json_files) == 1


def test_report_json_is_valid():
    results = [_make_result()]
    with tempfile.TemporaryDirectory() as tmpdir:
        save_report(results, output_dir=tmpdir)
        json_file = next(Path(tmpdir).glob("*.json"))
        data = json.loads(json_file.read_text())
        assert "generated_at" in data
        assert len(data["results"]) == 1
        assert data["results"][0]["scenario"] == "test_scenario"


def test_report_html_contains_scenario():
    results = [_make_result("my_scenario")]
    with tempfile.TemporaryDirectory() as tmpdir:
        html_path = save_report(results, output_dir=tmpdir)
        html = Path(html_path).read_text()
        assert "my_scenario" in html
        assert "Stress Test Report" in html
