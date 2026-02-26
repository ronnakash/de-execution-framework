"""Latency and throughput metrics collection for stress tests."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field


@dataclass
class StressMetrics:
    """Collects and computes stress test metrics."""

    _ingestion_times: list[float] = field(default_factory=list)
    _completion_times: list[float] = field(default_factory=list)
    _latencies: list[float] = field(default_factory=list)
    _errors: int = 0
    _start_time: float = 0.0
    _end_time: float = 0.0
    _total_events: int = 0

    def start(self) -> None:
        """Mark the start of the stress test."""
        self._start_time = time.monotonic()

    def stop(self) -> None:
        """Mark the end of the stress test."""
        self._end_time = time.monotonic()

    def record_ingestion(self, timestamp: float) -> None:
        """Record when an event was ingested."""
        self._ingestion_times.append(timestamp)
        self._total_events += 1

    def record_completion(self, ingested_at: float, completed_at: float) -> None:
        """Record when an event completed pipeline processing."""
        self._completion_times.append(completed_at)
        self._latencies.append(completed_at - ingested_at)

    def record_error(self) -> None:
        """Record a processing error."""
        self._errors += 1

    @property
    def duration_seconds(self) -> float:
        """Total test duration in seconds."""
        if self._end_time == 0:
            return time.monotonic() - self._start_time
        return self._end_time - self._start_time

    @property
    def throughput_events_per_second(self) -> float:
        """Overall throughput in events/second."""
        dur = self.duration_seconds
        if dur <= 0:
            return 0.0
        return self._total_events / dur

    @property
    def error_count(self) -> int:
        return self._errors

    @property
    def error_rate(self) -> float:
        """Error rate as a fraction."""
        if self._total_events == 0:
            return 0.0
        return self._errors / self._total_events

    @property
    def total_events(self) -> int:
        return self._total_events

    def latency_stats(self) -> dict[str, float]:
        """Compute latency statistics.

        Returns dict with min, max, mean, median, p95, p99 in seconds.
        """
        if not self._latencies:
            return {
                "min": 0.0,
                "max": 0.0,
                "mean": 0.0,
                "median": 0.0,
                "p95": 0.0,
                "p99": 0.0,
            }
        sorted_lat = sorted(self._latencies)
        n = len(sorted_lat)
        return {
            "min": sorted_lat[0],
            "max": sorted_lat[-1],
            "mean": statistics.mean(sorted_lat),
            "median": statistics.median(sorted_lat),
            "p95": sorted_lat[int(n * 0.95)] if n > 1 else sorted_lat[0],
            "p99": sorted_lat[int(n * 0.99)] if n > 1 else sorted_lat[0],
        }

    def to_dict(self) -> dict:
        """Export metrics as a plain dictionary."""
        return {
            "total_events": self._total_events,
            "duration_seconds": round(self.duration_seconds, 3),
            "throughput_eps": round(self.throughput_events_per_second, 2),
            "errors": self._errors,
            "error_rate": round(self.error_rate, 4),
            "latency": {k: round(v, 4) for k, v in self.latency_stats().items()},
        }
