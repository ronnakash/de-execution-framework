from de_platform.services.metrics.memory_metrics import MemoryMetrics


def test_counter_increments():
    m = MemoryMetrics()
    m.counter("requests")
    m.counter("requests")
    m.counter("requests", value=3)
    assert m.counters["requests"] == 5


def test_gauge_sets_value():
    m = MemoryMetrics()
    m.gauge("cpu", 0.5)
    assert m.gauges["cpu"] == 0.5
    m.gauge("cpu", 0.8)
    assert m.gauges["cpu"] == 0.8


def test_histogram_records_values():
    m = MemoryMetrics()
    m.histogram("latency", 10.0)
    m.histogram("latency", 20.0)
    m.histogram("latency", 15.0)
    assert m.histograms["latency"] == [10.0, 20.0, 15.0]


def test_separate_metric_names():
    m = MemoryMetrics()
    m.counter("a")
    m.counter("b", value=2)
    assert m.counters["a"] == 1
    assert m.counters["b"] == 2
