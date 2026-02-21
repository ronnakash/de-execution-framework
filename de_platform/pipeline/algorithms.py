"""Fraud detection algorithm interface and built-in implementations.

All algorithms implement FraudAlgorithm and return an Alert (or None) when
given an enriched event dict.
"""

from __future__ import annotations

import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from de_platform.services.cache.interface import CacheInterface


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
    def evaluate(self, event: dict[str, Any]) -> Alert | None:
        """Return an Alert if fraud is detected, None otherwise."""
        ...


class LargeNotionalAlgo(FraudAlgorithm):
    """Flag trades whose notional_usd (or amount_usd) exceeds a USD threshold."""

    def __init__(self, threshold_usd: float = 1_000_000) -> None:
        self.threshold_usd = threshold_usd

    def name(self) -> str:
        return "large_notional"

    def evaluate(self, event: dict[str, Any]) -> Alert | None:
        notional_usd = event.get("notional_usd") or event.get("amount_usd", 0)
        try:
            if float(notional_usd) > self.threshold_usd:
                return Alert(
                    alert_id=uuid.uuid4().hex,
                    tenant_id=event.get("tenant_id", ""),
                    event_type=event.get("event_type", ""),
                    event_id=event.get("id", ""),
                    message_id=event.get("message_id", ""),
                    algorithm=self.name(),
                    severity="high",
                    description=(
                        f"Notional exceeds ${self.threshold_usd:,.0f} USD threshold"
                    ),
                    details={
                        "notional_usd": float(notional_usd),
                        "threshold_usd": self.threshold_usd,
                    },
                    created_at=_now_iso(),
                )
        except (TypeError, ValueError):
            pass
        return None


class VelocityAlgo(FraudAlgorithm):
    """Flag when a tenant submits more than *max_events* events in *window_seconds*."""

    def __init__(
        self,
        cache: CacheInterface,
        max_events: int = 100,
        window_seconds: int = 60,
    ) -> None:
        self.cache = cache
        self.max_events = max_events
        self.window_seconds = window_seconds

    def name(self) -> str:
        return "velocity"

    def evaluate(self, event: dict[str, Any]) -> Alert | None:
        tenant_id = event.get("tenant_id", "")
        bucket = int(time.time() / self.window_seconds)
        cache_key = f"velocity:{tenant_id}:{bucket}"

        current: int = self.cache.get(cache_key) or 0
        current += 1
        self.cache.set(cache_key, current, ttl=self.window_seconds * 2)

        if current > self.max_events:
            return Alert(
                alert_id=uuid.uuid4().hex,
                tenant_id=tenant_id,
                event_type=event.get("event_type", ""),
                event_id=event.get("id", ""),
                message_id=event.get("message_id", ""),
                algorithm=self.name(),
                severity="medium",
                description=(
                    f"Tenant {tenant_id!r} exceeded {self.max_events} events"
                    f" in {self.window_seconds}s window"
                ),
                details={
                    "event_count": current,
                    "window_seconds": self.window_seconds,
                },
                created_at=_now_iso(),
            )
        return None


class SuspiciousCounterpartyAlgo(FraudAlgorithm):
    """Flag transactions whose counterparty_id appears in a blocklist."""

    def __init__(self, suspicious_ids: set[str]) -> None:
        self.suspicious_ids = suspicious_ids

    def name(self) -> str:
        return "suspicious_counterparty"

    def evaluate(self, event: dict[str, Any]) -> Alert | None:
        counterparty_id = event.get("counterparty_id", "")
        if counterparty_id in self.suspicious_ids:
            return Alert(
                alert_id=uuid.uuid4().hex,
                tenant_id=event.get("tenant_id", ""),
                event_type=event.get("event_type", ""),
                event_id=event.get("id", ""),
                message_id=event.get("message_id", ""),
                algorithm=self.name(),
                severity="critical",
                description=f"Transaction with suspicious counterparty {counterparty_id!r}",
                details={"counterparty_id": counterparty_id},
                created_at=_now_iso(),
            )
        return None
