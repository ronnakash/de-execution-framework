"""Tests for stress test metrics collection."""

import time

from tests.stress.metrics import StressMetrics


def test_initial_state():
    m = StressMetrics()
    assert m.total_events == 0
    assert m.error_count == 0
    assert m.error_rate == 0.0


def test_record_ingestion():
    m = StressMetrics()
    m.record_ingestion(time.monotonic())
    m.record_ingestion(time.monotonic())
    assert m.total_events == 2


def test_record_error():
    m = StressMetrics()
    m.record_ingestion(time.monotonic())
    m.record_error()
    assert m.error_count == 1
    assert m.error_rate == 1.0  # 1 error / 1 event


def test_error_rate_with_events():
    m = StressMetrics()
    for _ in range(10):
        m.record_ingestion(time.monotonic())
    for _ in range(2):
        m.record_error()
    assert m.error_rate == 0.2  # 2/10


def test_duration():
    m = StressMetrics()
    m.start()
    time.sleep(0.05)
    m.stop()
    assert m.duration_seconds >= 0.04


def test_throughput():
    m = StressMetrics()
    m.start()
    for _ in range(100):
        m.record_ingestion(time.monotonic())
    m.stop()
    assert m.throughput_events_per_second > 0


def test_latency_stats_empty():
    m = StressMetrics()
    stats = m.latency_stats()
    assert stats["min"] == 0.0
    assert stats["max"] == 0.0
    assert stats["mean"] == 0.0


def test_latency_stats_with_data():
    m = StressMetrics()
    for i in range(100):
        m.record_completion(0.0, float(i) / 100.0)
    stats = m.latency_stats()
    assert stats["min"] == 0.0
    assert stats["max"] > 0.9
    assert stats["p95"] > stats["median"]


def test_to_dict():
    m = StressMetrics()
    m.start()
    m.record_ingestion(time.monotonic())
    m.record_completion(0.0, 0.5)
    m.stop()
    d = m.to_dict()
    assert "total_events" in d
    assert "throughput_eps" in d
    assert "latency" in d
    assert "p95" in d["latency"]
