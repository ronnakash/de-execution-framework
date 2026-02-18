"""Prometheus metrics implementation using prometheus_client."""

from __future__ import annotations

from de_platform.services.metrics.interface import MetricsInterface
from de_platform.services.secrets.interface import SecretsInterface


def _label_names(tags: dict[str, str] | None) -> list[str]:
    return sorted(tags.keys()) if tags else []


def _label_values(names: list[str], tags: dict[str, str] | None) -> list[str]:
    if not tags:
        return []
    return [tags[n] for n in names]


class PrometheusMetrics(MetricsInterface):
    """Metrics backend that exposes metrics via Prometheus HTTP endpoint.

    Config (via secrets):
        METRICS_PROMETHEUS_PORT - Port to expose /metrics on (default: 9091).
                                  Set to 0 or empty to disable the HTTP server
                                  (useful when metrics are scraped another way).

    All metric names have illegal characters (dashes, dots) replaced with underscores
    to comply with Prometheus naming conventions.
    """

    def __init__(self, secrets: SecretsInterface) -> None:
        import prometheus_client as prom

        self._prom = prom
        self._counters: dict[str, prom.Counter] = {}
        self._gauges: dict[str, prom.Gauge] = {}
        self._histograms: dict[str, prom.Histogram] = {}

        port_str = secrets.get_or_default("METRICS_PROMETHEUS_PORT", "9091")
        port = int(port_str) if port_str else 0
        if port:
            prom.start_http_server(port)

    @staticmethod
    def _sanitize(name: str) -> str:
        return name.replace("-", "_").replace(".", "_")

    def counter(self, name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        safe = self._sanitize(name)
        label_names = _label_names(tags)
        key = f"{safe}:{','.join(label_names)}"
        if key not in self._counters:
            self._counters[key] = self._prom.Counter(safe, safe, label_names)
        c = self._counters[key]
        if label_names:
            c.labels(*_label_values(label_names, tags)).inc(value)
        else:
            c.inc(value)

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        safe = self._sanitize(name)
        label_names = _label_names(tags)
        key = f"{safe}:{','.join(label_names)}"
        if key not in self._gauges:
            self._gauges[key] = self._prom.Gauge(safe, safe, label_names)
        g = self._gauges[key]
        if label_names:
            g.labels(*_label_values(label_names, tags)).set(value)
        else:
            g.set(value)

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        safe = self._sanitize(name)
        label_names = _label_names(tags)
        key = f"{safe}:{','.join(label_names)}"
        if key not in self._histograms:
            self._histograms[key] = self._prom.Histogram(safe, safe, label_names)
        h = self._histograms[key]
        if label_names:
            h.labels(*_label_values(label_names, tags)).observe(value)
        else:
            h.observe(value)
