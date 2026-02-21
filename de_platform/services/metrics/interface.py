from __future__ import annotations

from abc import ABC, abstractmethod


class MetricsInterface(ABC):
    """Metrics collection for counters, gauges, and histograms."""

    @abstractmethod
    def counter(self, name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        ...

    @abstractmethod
    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None: ...

    @abstractmethod
    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None: ...
