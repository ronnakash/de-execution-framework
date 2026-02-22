"""Sliding window engine for fraud detection algorithms.

Manages per-tenant event buffers, tracks window positions, and dispatches
evaluation to registered ``FraudAlgorithm`` instances.  Shared by both the
realtime ``AlgosModule`` (streaming from Kafka) and the batch
``BatchAlgosModule`` (historical data from ClickHouse).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from de_platform.pipeline.algorithms import Alert, FraudAlgorithm


@dataclass
class WindowConfig:
    """Sliding window parameters for a tenant."""

    window_size_minutes: int = 5
    window_slide_minutes: int = 1

    @property
    def window_size(self) -> timedelta:
        return timedelta(minutes=self.window_size_minutes)

    @property
    def window_slide(self) -> timedelta:
        return timedelta(minutes=self.window_slide_minutes)


@dataclass
class TenantWindowState:
    """Per-tenant state maintained by the engine."""

    events: list[dict[str, Any]] = field(default_factory=list)
    last_window_end: datetime | None = None


class SlidingWindowEngine:
    """Manages per-tenant sliding windows and dispatches algorithm evaluation.

    Two entry points:

    * ``ingest(event)`` — add a single event (from Kafka) and evaluate if the
      window is ready.  Used by the realtime ``AlgosModule``.
    * ``run_batch(events, tenant_id, start, end)`` — evaluate sliding windows
      across a pre-loaded batch of events.  Used by ``BatchAlgosModule``.
    """

    def __init__(
        self,
        algorithms: list[FraudAlgorithm],
        get_window_config: Callable[[str], WindowConfig],
        get_algo_config: Callable[[str, str], tuple[bool, dict]],
    ) -> None:
        self.algorithms = algorithms
        self.get_window_config = get_window_config
        self.get_algo_config = get_algo_config
        self._tenants: dict[str, TenantWindowState] = defaultdict(TenantWindowState)

    # ── Realtime path ─────────────────────────────────────────────────────

    def ingest(self, event: dict[str, Any]) -> list[Alert]:
        """Add a single event and evaluate if the window is ready."""
        tenant_id = event.get("tenant_id", "")
        state = self._tenants[tenant_id]

        state.events.append(event)
        state.events.sort(key=lambda e: self._parse_time(e.get("transact_time", "")))

        config = self.get_window_config(tenant_id)
        return self._try_evaluate(tenant_id, state, config)

    # ── Batch path ────────────────────────────────────────────────────────

    def run_batch(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Alert]:
        """Evaluate sliding windows across a pre-loaded batch of events.

        *events* must already be sorted by ``transact_time``.
        """
        config = self.get_window_config(tenant_id)
        all_alerts: list[Alert] = []

        # Edge case: zero-size window evaluates every event individually
        if config.window_size == timedelta(0):
            for event in events:
                t = self._parse_time(event.get("transact_time", ""))
                alerts = self._evaluate_window(tenant_id, [event], t, t)
                all_alerts.extend(alerts)
            return all_alerts

        window_start = start_time
        while window_start + config.window_size <= end_time:
            window_end = window_start + config.window_size

            window_events = [
                e for e in events
                if window_start <= self._parse_time(e.get("transact_time", "")) < window_end
            ]

            if window_events:
                alerts = self._evaluate_window(
                    tenant_id, window_events, window_start, window_end,
                )
                all_alerts.extend(alerts)

            window_start += config.window_slide

        return all_alerts

    # ── Shared evaluation logic ───────────────────────────────────────────

    def _try_evaluate(
        self,
        tenant_id: str,
        state: TenantWindowState,
        config: WindowConfig,
    ) -> list[Alert]:
        """Check if the buffer spans a full window; if so, evaluate and slide."""
        if not state.events:
            return []

        # window_size=0 means evaluate immediately on every event
        if config.window_size == timedelta(0):
            latest = state.events[-1]
            now = self._parse_time(latest.get("transact_time", ""))
            alerts = self._evaluate_window(tenant_id, [latest], now, now)
            state.events.clear()
            return alerts

        earliest = self._parse_time(state.events[0].get("transact_time", ""))
        latest = self._parse_time(state.events[-1].get("transact_time", ""))
        span = latest - earliest

        if span < config.window_size:
            return []

        if state.last_window_end is None:
            window_start = earliest
        else:
            window_start = state.last_window_end
        window_end = window_start + config.window_size

        window_events = [
            e for e in state.events
            if window_start <= self._parse_time(e.get("transact_time", "")) < window_end
        ]

        alerts = self._evaluate_window(tenant_id, window_events, window_start, window_end)

        # Slide forward and prune old events
        state.last_window_end = window_start + config.window_slide
        cutoff = state.last_window_end - config.window_size
        state.events = [
            e for e in state.events
            if self._parse_time(e.get("transact_time", "")) >= cutoff
        ]

        return alerts

    def _evaluate_window(
        self,
        tenant_id: str,
        events: list[dict[str, Any]],
        window_start: datetime,
        window_end: datetime,
    ) -> list[Alert]:
        """Run all enabled algorithms on a window of events."""
        alerts: list[Alert] = []
        for algo in self.algorithms:
            enabled, thresholds = self.get_algo_config(tenant_id, algo.name())
            if not enabled:
                continue
            algo_alerts = algo.evaluate_window(
                events, tenant_id, window_start, window_end,
                thresholds=thresholds or None,
            )
            alerts.extend(algo_alerts)
        return alerts

    @staticmethod
    def _parse_time(value: str | datetime) -> datetime:
        """Parse ISO timestamp string to datetime.  Pass-through for datetime."""
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return datetime.min
