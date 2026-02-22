"""Fraud detection algorithm interface and built-in implementations.

All algorithms implement FraudAlgorithm.  The primary method is
``evaluate_window()`` which receives a time-ordered list of events and
returns zero or more Alerts.  A convenience ``evaluate()`` method is
provided for single-event evaluation (delegates to ``evaluate_window``).

Design principle: all algorithm implementations must be **pure in-memory
computations**.  No external service calls (Redis, DB, HTTP) inside
``evaluate_window()``.  The sliding window engine manages buffers and
config lookups; algos just receive data and return alerts.
"""

from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Alert:
    alert_id: str       # UUID hex
    tenant_id: str
    event_type: str
    event_id: str
    message_id: str
    algorithm: str      # name of the algo that triggered
    severity: str       # "low" | "medium" | "high" | "critical"
    description: str
    details: dict
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "tenant_id": self.tenant_id,
            "event_type": self.event_type,
            "event_id": self.event_id,
            "message_id": self.message_id,
            "algorithm": self.algorithm,
            "severity": self.severity,
            "description": self.description,
            "details": json.dumps(self.details),
            "created_at": self.created_at,
        }


class FraudAlgorithm(ABC):
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this algorithm."""
        ...

    @abstractmethod
    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        """Evaluate a time-ordered batch of events within a window.

        Must be a pure in-memory computation — no external service calls.
        Returns zero or more Alerts.
        """
        ...

    def evaluate(
        self, event: dict[str, Any], thresholds: dict[str, Any] | None = None,
    ) -> Alert | None:
        """Single-event convenience. Delegates to evaluate_window()."""
        alerts = self.evaluate_window(
            [event],
            event.get("tenant_id", ""),
            datetime.min,
            datetime.max,
            thresholds=thresholds,
        )
        return alerts[0] if alerts else None


class LargeNotionalAlgo(FraudAlgorithm):
    """Flag trades whose notional_usd (or amount_usd) exceeds a USD threshold."""

    def __init__(self, threshold_usd: float = 1_000_000) -> None:
        self.threshold_usd = threshold_usd

    def name(self) -> str:
        return "large_notional"

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        threshold_usd = (thresholds or {}).get("threshold_usd", self.threshold_usd)
        alerts: list[Alert] = []
        for event in events:
            notional_usd = event.get("notional_usd") or event.get("amount_usd", 0)
            try:
                if float(notional_usd) > threshold_usd:
                    alerts.append(Alert(
                        alert_id=uuid.uuid4().hex,
                        tenant_id=event.get("tenant_id", "") or tenant_id,
                        event_type=event.get("event_type", ""),
                        event_id=event.get("id", ""),
                        message_id=event.get("message_id", ""),
                        algorithm=self.name(),
                        severity="high",
                        description=(
                            f"Notional exceeds ${threshold_usd:,.0f} USD threshold"
                        ),
                        details={
                            "notional_usd": float(notional_usd),
                            "threshold_usd": threshold_usd,
                        },
                        created_at=_now_iso(),
                    ))
            except (TypeError, ValueError):
                pass
        return alerts


class VelocityAlgo(FraudAlgorithm):
    """Flag when a window contains more than *max_events* events.

    Pure in-memory: counts events in the provided window directly.
    No external service (Redis/cache) dependency.
    """

    def __init__(
        self,
        max_events: int = 100,
        window_seconds: int = 60,
    ) -> None:
        self.max_events = max_events
        self.window_seconds = window_seconds

    def name(self) -> str:
        return "velocity"

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        max_events = (thresholds or {}).get("max_events", self.max_events)
        window_seconds = (thresholds or {}).get("window_seconds", self.window_seconds)

        if len(events) > max_events:
            trigger_event = events[max_events]
            return [Alert(
                alert_id=uuid.uuid4().hex,
                tenant_id=tenant_id,
                event_type=trigger_event.get("event_type", ""),
                event_id=trigger_event.get("id", ""),
                message_id=trigger_event.get("message_id", ""),
                algorithm=self.name(),
                severity="medium",
                description=(
                    f"Tenant {tenant_id!r} exceeded {max_events} events"
                    f" in {window_seconds}s window"
                ),
                details={
                    "event_count": len(events),
                    "window_seconds": window_seconds,
                    "window_start": window_start.isoformat()
                    if window_start != datetime.min else "",
                    "window_end": window_end.isoformat()
                    if window_end != datetime.max else "",
                },
                created_at=_now_iso(),
            )]
        return []


class SuspiciousCounterpartyAlgo(FraudAlgorithm):
    """Flag transactions whose counterparty_id appears in a blocklist."""

    def __init__(self, suspicious_ids: set[str] | None = None) -> None:
        self.suspicious_ids = suspicious_ids or set()

    def name(self) -> str:
        return "suspicious_counterparty"

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        suspicious_ids = self.suspicious_ids
        if thresholds and "suspicious_ids" in thresholds:
            suspicious_ids = set(thresholds["suspicious_ids"])

        alerts: list[Alert] = []
        for event in events:
            counterparty_id = event.get("counterparty_id", "")
            if counterparty_id in suspicious_ids:
                alerts.append(Alert(
                    alert_id=uuid.uuid4().hex,
                    tenant_id=event.get("tenant_id", "") or tenant_id,
                    event_type=event.get("event_type", ""),
                    event_id=event.get("id", ""),
                    message_id=event.get("message_id", ""),
                    algorithm=self.name(),
                    severity="critical",
                    description=f"Transaction with suspicious counterparty {counterparty_id!r}",
                    details={"counterparty_id": counterparty_id},
                    created_at=_now_iso(),
                ))
        return alerts
