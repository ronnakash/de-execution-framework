"""Tests for VelocityAlgo alert suppression (Phase 1 - WS2).

Verifies that overlapping windows within the cooldown period do not generate
duplicate velocity alerts, and that alerts resume after the cooldown expires.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from de_platform.pipeline.algorithms import VelocityAlgo


def _events(count: int, tenant_id: str = "t1") -> list[dict]:
    """Generate *count* minimal events."""
    return [
        {
            "id": f"e-{i}",
            "tenant_id": tenant_id,
            "event_type": "order",
            "message_id": f"msg-{i}",
        }
        for i in range(count)
    ]


class TestVelocitySuppression:
    """Velocity alert suppression within cooldown window."""

    def test_first_window_over_threshold_generates_alert(self) -> None:
        algo = VelocityAlgo(max_events=5, window_seconds=60)
        alerts = algo.evaluate_window(
            _events(10), "t1", datetime.min, datetime.max,
        )
        assert len(alerts) == 1
        assert alerts[0].algorithm == "velocity"

    def test_second_window_within_cooldown_suppressed(self) -> None:
        algo = VelocityAlgo(max_events=5, window_seconds=60)

        # First call — should alert
        alerts1 = algo.evaluate_window(
            _events(10), "t1", datetime.min, datetime.max,
        )
        assert len(alerts1) == 1

        # Second call immediately — should be suppressed (within 60s cooldown)
        alerts2 = algo.evaluate_window(
            _events(10), "t1", datetime.min, datetime.max,
        )
        assert len(alerts2) == 0

    def test_alert_after_cooldown_expires(self) -> None:
        algo = VelocityAlgo(max_events=5, window_seconds=60)

        # First call — alert
        now = datetime.now(timezone.utc)
        alerts1 = algo.evaluate_window(
            _events(10), "t1", datetime.min, datetime.max,
        )
        assert len(alerts1) == 1

        # Simulate time passing beyond the cooldown window
        future = now + timedelta(seconds=61)
        with patch(
            "de_platform.pipeline.algorithms.datetime"
        ) as mock_dt:
            mock_dt.now.return_value = future
            mock_dt.min = datetime.min
            mock_dt.max = datetime.max
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            alerts2 = algo.evaluate_window(
                _events(10), "t1", datetime.min, datetime.max,
            )
            assert len(alerts2) == 1

    def test_different_tenants_not_suppressed(self) -> None:
        algo = VelocityAlgo(max_events=5, window_seconds=60)

        alerts_t1 = algo.evaluate_window(
            _events(10, tenant_id="t1"), "t1", datetime.min, datetime.max,
        )
        assert len(alerts_t1) == 1

        # Different tenant — should NOT be suppressed
        alerts_t2 = algo.evaluate_window(
            _events(10, tenant_id="t2"), "t2", datetime.min, datetime.max,
        )
        assert len(alerts_t2) == 1

    def test_under_threshold_no_suppression_state(self) -> None:
        algo = VelocityAlgo(max_events=5, window_seconds=60)

        # Under threshold — no alert, no suppression tracking
        alerts = algo.evaluate_window(
            _events(3), "t1", datetime.min, datetime.max,
        )
        assert len(alerts) == 0
        assert "t1" not in algo._last_alert_time
