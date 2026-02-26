"""Comprehensive unit tests for all fraud detection algorithms.

Covers LargeNotionalAlgo, VelocityAlgo, SuspiciousCounterpartyAlgo,
and the Alert dataclass — including edge cases, threshold overrides,
and the evaluate() backward-compat wrapper.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from de_platform.pipeline.algorithms import (
    Alert,
    LargeNotionalAlgo,
    SuspiciousCounterpartyAlgo,
    VelocityAlgo,
)

# ── Helpers ──────────────────────────────────────────────────────────────────


def _event(
    *,
    notional_usd: float | str | None = 500_000,
    amount_usd: float | None = None,
    tenant_id: str = "t1",
    event_type: str = "order",
    event_id: str = "o1",
    counterparty_id: str = "",
    message_id: str = "msg-1",
    **extra: object,
) -> dict:
    e: dict = {
        "id": event_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "message_id": message_id,
    }
    if notional_usd is not None:
        e["notional_usd"] = notional_usd
    if amount_usd is not None:
        e["amount_usd"] = amount_usd
    if counterparty_id:
        e["counterparty_id"] = counterparty_id
    e.update(extra)
    return e


def _events(count: int, **kwargs: object) -> list[dict]:
    return [
        _event(event_id=f"e-{i}", message_id=f"msg-{i}", **kwargs)
        for i in range(count)
    ]


W_START = datetime.min
W_END = datetime.max


# ═══════════════════════════════════════════════════════════════════════════
# LargeNotionalAlgo
# ═══════════════════════════════════════════════════════════════════════════


class TestLargeNotionalAlgo:
    def test_single_event_above_threshold(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=2_000_000)], "t1", W_START, W_END,
        )
        assert len(alerts) == 1

    def test_single_event_below_threshold(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=500_000)], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_single_event_at_threshold(self) -> None:
        """Exactly at threshold should NOT alert (> not >=)."""
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=1_000_000)], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_window_multiple_above_threshold(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        events = [_event(notional_usd=2_000_000, event_id=f"o{i}") for i in range(3)]
        alerts = algo.evaluate_window(events, "t1", W_START, W_END)
        assert len(alerts) == 3

    def test_window_mixed_above_below(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        events = [
            _event(notional_usd=2_000_000, event_id="o1"),
            _event(notional_usd=500_000, event_id="o2"),
            _event(notional_usd=3_000_000, event_id="o3"),
            _event(notional_usd=100_000, event_id="o4"),
            _event(notional_usd=1_500_000, event_id="o5"),
        ]
        alerts = algo.evaluate_window(events, "t1", W_START, W_END)
        assert len(alerts) == 3

    def test_custom_threshold_override(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=600_000)], "t1", W_START, W_END,
            thresholds={"threshold_usd": 500_000},
        )
        assert len(alerts) == 1

    def test_notional_usd_field(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=2_000_000)], "t1", W_START, W_END,
        )
        assert alerts[0].details["notional_usd"] == 2_000_000

    def test_amount_usd_fallback(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=None, amount_usd=2_000_000)], "t1", W_START, W_END,
        )
        assert len(alerts) == 1

    def test_non_numeric_notional_skipped(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd="abc")], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_none_notional_skipped(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        # Event with no notional_usd and no amount_usd
        e = {"id": "o1", "tenant_id": "t1", "event_type": "order", "message_id": "m1"}
        alerts = algo.evaluate_window([e], "t1", W_START, W_END)
        assert len(alerts) == 0

    def test_zero_notional(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=0)], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_negative_notional(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=-500_000)], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_alert_fields_correct(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [_event(notional_usd=2_000_000, tenant_id="acme", event_type="order",
                    event_id="o42", message_id="msg-42")],
            "acme", W_START, W_END,
        )
        a = alerts[0]
        assert a.tenant_id == "acme"
        assert a.event_type == "order"
        assert a.event_id == "o42"
        assert a.message_id == "msg-42"
        assert a.algorithm == "large_notional"
        assert a.severity == "high"
        assert "threshold" in a.description.lower()
        assert a.details["notional_usd"] == 2_000_000
        assert a.details["threshold_usd"] == 1_000_000
        assert len(a.alert_id) == 32  # uuid hex
        assert a.created_at  # non-empty ISO string

    def test_evaluate_single_event_convenience(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alert = algo.evaluate(_event(notional_usd=2_000_000))
        assert alert is not None
        assert alert.algorithm == "large_notional"

        no_alert = algo.evaluate(_event(notional_usd=500_000))
        assert no_alert is None


# ═══════════════════════════════════════════════════════════════════════════
# VelocityAlgo
# ═══════════════════════════════════════════════════════════════════════════


class TestVelocityAlgo:
    def _fresh_algo(self, max_events: int = 100, window_seconds: int = 60) -> VelocityAlgo:
        """Return a fresh VelocityAlgo with no suppression state."""
        return VelocityAlgo(max_events=max_events, window_seconds=window_seconds)

    def test_under_threshold_no_alert(self) -> None:
        algo = self._fresh_algo(max_events=100)
        alerts = algo.evaluate_window(_events(50), "t1", W_START, W_END)
        assert len(alerts) == 0

    def test_at_threshold_no_alert(self) -> None:
        algo = self._fresh_algo(max_events=100)
        alerts = algo.evaluate_window(_events(100), "t1", W_START, W_END)
        assert len(alerts) == 0

    def test_over_threshold_one_alert(self) -> None:
        algo = self._fresh_algo(max_events=100)
        alerts = algo.evaluate_window(_events(101), "t1", W_START, W_END)
        assert len(alerts) == 1

    def test_trigger_event_is_threshold_breaker(self) -> None:
        algo = self._fresh_algo(max_events=5)
        events = _events(10)
        alerts = algo.evaluate_window(events, "t1", W_START, W_END)
        assert alerts[0].event_id == events[5]["id"]  # events[max_events]

    def test_returns_at_most_one_alert(self) -> None:
        algo = self._fresh_algo(max_events=5)
        alerts = algo.evaluate_window(_events(200), "t1", W_START, W_END)
        assert len(alerts) == 1

    def test_custom_max_events_override(self) -> None:
        algo = self._fresh_algo(max_events=100)
        alerts = algo.evaluate_window(
            _events(15), "t1", W_START, W_END,
            thresholds={"max_events": 10},
        )
        assert len(alerts) == 1

    def test_custom_window_seconds_override(self) -> None:
        algo = self._fresh_algo(max_events=5, window_seconds=60)
        alerts = algo.evaluate_window(
            _events(10), "t1", W_START, W_END,
            thresholds={"window_seconds": 30},
        )
        assert len(alerts) == 1
        assert alerts[0].details["window_seconds"] == 30

    def test_empty_window(self) -> None:
        algo = self._fresh_algo(max_events=100)
        alerts = algo.evaluate_window([], "t1", W_START, W_END)
        assert len(alerts) == 0

    def test_single_event(self) -> None:
        algo = self._fresh_algo(max_events=100)
        alerts = algo.evaluate_window([_event()], "t1", W_START, W_END)
        assert len(alerts) == 0

    def test_alert_severity_is_medium(self) -> None:
        algo = self._fresh_algo(max_events=5)
        alerts = algo.evaluate_window(_events(10), "t1", W_START, W_END)
        assert alerts[0].severity == "medium"

    def test_alert_details_contain_event_count(self) -> None:
        algo = self._fresh_algo(max_events=5, window_seconds=60)
        alerts = algo.evaluate_window(_events(10), "t1", W_START, W_END)
        assert alerts[0].details["event_count"] == 10
        assert alerts[0].details["window_seconds"] == 60

    def test_evaluate_single_event_convenience(self) -> None:
        algo = self._fresh_algo(max_events=0)
        alert = algo.evaluate(_event())
        assert alert is not None
        assert alert.algorithm == "velocity"


# ═══════════════════════════════════════════════════════════════════════════
# SuspiciousCounterpartyAlgo
# ═══════════════════════════════════════════════════════════════════════════


class TestSuspiciousCounterpartyAlgo:
    def test_suspicious_counterparty_detected(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alerts = algo.evaluate_window(
            [_event(counterparty_id="BAD1")], "t1", W_START, W_END,
        )
        assert len(alerts) == 1

    def test_clean_counterparty_no_alert(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alerts = algo.evaluate_window(
            [_event(counterparty_id="GOOD1")], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_empty_blocklist_no_alerts(self) -> None:
        algo = SuspiciousCounterpartyAlgo()
        alerts = algo.evaluate_window(
            [_event(counterparty_id="ANY")], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_multiple_suspicious_in_window(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1", "BAD2", "BAD3"})
        events = [
            _event(counterparty_id="BAD1", event_id="o1"),
            _event(counterparty_id="BAD2", event_id="o2"),
            _event(counterparty_id="BAD3", event_id="o3"),
        ]
        alerts = algo.evaluate_window(events, "t1", W_START, W_END)
        assert len(alerts) == 3

    def test_mixed_suspicious_and_clean(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        events = [
            _event(counterparty_id="GOOD1", event_id="o1"),
            _event(counterparty_id="BAD1", event_id="o2"),
            _event(counterparty_id="GOOD2", event_id="o3"),
            _event(counterparty_id="GOOD3", event_id="o4"),
            _event(counterparty_id="BAD1", event_id="o5"),
        ]
        alerts = algo.evaluate_window(events, "t1", W_START, W_END)
        assert len(alerts) == 2

    def test_custom_suspicious_ids_override(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"OLD_BAD"})
        alerts = algo.evaluate_window(
            [_event(counterparty_id="NEW_BAD")], "t1", W_START, W_END,
            thresholds={"suspicious_ids": ["NEW_BAD"]},
        )
        assert len(alerts) == 1

    def test_missing_counterparty_field(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        e = {"id": "o1", "tenant_id": "t1", "event_type": "order", "message_id": "m1"}
        alerts = algo.evaluate_window([e], "t1", W_START, W_END)
        assert len(alerts) == 0

    def test_empty_counterparty_field(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alerts = algo.evaluate_window(
            [_event(counterparty_id="")], "t1", W_START, W_END,
        )
        assert len(alerts) == 0

    def test_alert_severity_is_critical(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alerts = algo.evaluate_window(
            [_event(counterparty_id="BAD1")], "t1", W_START, W_END,
        )
        assert alerts[0].severity == "critical"

    def test_alert_details_contain_counterparty(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alerts = algo.evaluate_window(
            [_event(counterparty_id="BAD1")], "t1", W_START, W_END,
        )
        assert alerts[0].details["counterparty_id"] == "BAD1"

    def test_evaluate_single_event_convenience(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alert = algo.evaluate(_event(counterparty_id="BAD1"))
        assert alert is not None
        assert alert.algorithm == "suspicious_counterparty"

        no_alert = algo.evaluate(_event(counterparty_id="GOOD"))
        assert no_alert is None


# ═══════════════════════════════════════════════════════════════════════════
# Alert Dataclass
# ═══════════════════════════════════════════════════════════════════════════


class TestAlert:
    def test_alert_to_dict(self) -> None:
        a = Alert(
            alert_id="abc123",
            tenant_id="t1",
            event_type="order",
            event_id="o1",
            message_id="msg-1",
            algorithm="large_notional",
            severity="high",
            description="Test alert",
            details={"foo": "bar"},
            created_at="2026-01-15T10:00:00+00:00",
        )
        d = a.to_dict()
        assert d["alert_id"] == "abc123"
        assert d["tenant_id"] == "t1"
        assert d["algorithm"] == "large_notional"
        assert d["severity"] == "high"
        # details should be serialized as JSON string
        assert json.loads(d["details"]) == {"foo": "bar"}

    def test_alert_to_dict_preserves_all_fields(self) -> None:
        a = Alert(
            alert_id="id1",
            tenant_id="t1",
            event_type="execution",
            event_id="e1",
            message_id="m1",
            algorithm="velocity",
            severity="medium",
            description="desc",
            details={"x": 1},
            created_at="2026-01-01T00:00:00Z",
        )
        d = a.to_dict()
        expected_keys = {
            "alert_id", "tenant_id", "event_type", "event_id", "message_id",
            "algorithm", "severity", "description", "details", "created_at",
        }
        assert set(d.keys()) == expected_keys
