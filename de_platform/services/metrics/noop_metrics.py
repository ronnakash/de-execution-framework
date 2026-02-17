from de_platform.services.metrics.interface import MetricsInterface


class NoopMetrics(MetricsInterface):
    """Discards all metrics. Used when metrics collection is not needed."""

    def counter(self, name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        pass

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass
