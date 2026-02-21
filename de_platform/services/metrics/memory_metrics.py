from __future__ import annotations

from de_platform.services.metrics.interface import MetricsInterface


class MemoryMetrics(MetricsInterface):
    """In-memory metrics for test assertions on metric values."""

    def __init__(self) -> None:
        self.counters: dict[str, float] = {}
        self.gauges: dict[str, float] = {}
        self.histograms: dict[str, list[float]] = {}

    def counter(self, name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        self.counters[name] = self.counters.get(name, 0) + value

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        self.gauges[name] = value

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        if name not in self.histograms:
            self.histograms[name] = []
        self.histograms[name].append(value)
