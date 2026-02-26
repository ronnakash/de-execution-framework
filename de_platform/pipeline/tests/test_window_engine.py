"""Tests for the SlidingWindowEngine."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from de_platform.pipeline.algorithms import LargeNotionalAlgo, VelocityAlgo
from de_platform.pipeline.window_engine import (
    SlidingWindowEngine,
    WindowConfig,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

_BASE = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def _event(
    minute_offset: int = 0,
    notional_usd: float = 500_000,
    tenant_id: str = "t1",
) -> dict:
    t = _BASE + timedelta(minutes=minute_offset)
    return {
        "id": f"o-{minute_offset}",
        "tenant_id": tenant_id,
        "event_type": "order",
        "message_id": f"msg-{minute_offset}",
        "notional_usd": notional_usd,
        "transact_time": t.isoformat(),
    }


def _default_config(_tenant_id: str) -> WindowConfig:
    return WindowConfig(window_size_minutes=5, window_slide_minutes=1)


def _zero_config(_tenant_id: str) -> WindowConfig:
    return WindowConfig(window_size_minutes=0, window_slide_minutes=0)


def _all_enabled(_tenant_id: str, _algo_name: str) -> tuple[bool, dict]:
    return True, {}


def _make_engine(
    config_fn=None,
    algo_config_fn=None,
    algos=None,
) -> SlidingWindowEngine:
    if algos is None:
        algos = [LargeNotionalAlgo(threshold_usd=1_000_000)]
    return SlidingWindowEngine(
        algorithms=algos,
        get_window_config=config_fn or _default_config,
        get_algo_config=algo_config_fn or _all_enabled,
    )


# ── Realtime path tests ──────────────────────────────────────────────────────


def test_ingest_no_alert_before_window_full() -> None:
    """Events that don't span a full window produce no alerts."""
    engine = _make_engine()
    # 4 events over 4 minutes — window needs 5 minutes
    for i in range(5):
        alerts = engine.ingest(_event(minute_offset=i, notional_usd=2_000_000))
    # Span is 4 minutes (0..4), not >= 5
    assert alerts == []


def test_ingest_alert_when_window_full() -> None:
    """Events spanning window_size trigger evaluation."""
    engine = _make_engine()
    all_alerts = []
    # 6 events spanning 5 minutes (0..5) — should trigger at minute 5
    for i in range(6):
        alerts = engine.ingest(_event(minute_offset=i, notional_usd=2_000_000))
        all_alerts.extend(alerts)
    assert len(all_alerts) >= 1
    assert all(a.algorithm == "large_notional" for a in all_alerts)


def test_window_slides() -> None:
    """After evaluation, old events are pruned and window advances."""
    engine = _make_engine()
    # Fill 6 events to trigger first window
    for i in range(6):
        engine.ingest(_event(minute_offset=i, notional_usd=2_000_000))

    state = engine._tenants["t1"]
    # After sliding, old events should be pruned
    # last_window_end should be set (slid forward)
    assert state.last_window_end is not None


def test_multiple_tenants_independent() -> None:
    """Each tenant has its own buffer and window state."""
    engine = _make_engine()
    engine.ingest(_event(minute_offset=0, tenant_id="t1"))
    engine.ingest(_event(minute_offset=0, tenant_id="t2"))

    assert "t1" in engine._tenants
    assert "t2" in engine._tenants
    assert len(engine._tenants["t1"].events) == 1
    assert len(engine._tenants["t2"].events) == 1


def test_immediate_eval_zero_window() -> None:
    """window_size=0 evaluates on every event, no buffering."""
    engine = _make_engine(config_fn=_zero_config)
    alerts = engine.ingest(_event(notional_usd=2_000_000))
    assert len(alerts) == 1
    assert alerts[0].algorithm == "large_notional"

    # Buffer should be cleared after immediate evaluation
    assert len(engine._tenants["t1"].events) == 0


def test_immediate_eval_no_alert() -> None:
    """window_size=0 with non-triggering event produces no alert."""
    engine = _make_engine(config_fn=_zero_config)
    alerts = engine.ingest(_event(notional_usd=100))
    assert alerts == []


# ── Batch path tests ─────────────────────────────────────────────────────────


def test_run_batch_full_day() -> None:
    """24h of events with 5-min windows produces correct alert count."""
    engine = _make_engine()
    # One event per minute for 60 minutes, all above threshold
    events = [_event(minute_offset=i, notional_usd=2_000_000) for i in range(60)]

    start = _BASE
    end = _BASE + timedelta(minutes=60)
    alerts = engine.run_batch(events, "t1", start, end)

    # With 5-min window, 1-min slide, from minute 0 to 60:
    # Windows: [0,5), [1,6), ..., [55,60) = 56 windows
    # Each window has 5 events, each above threshold => 5 alerts per window
    assert len(alerts) == 56 * 5


def test_run_batch_empty() -> None:
    """No events produces no alerts."""
    engine = _make_engine()
    alerts = engine.run_batch([], "t1", _BASE, _BASE + timedelta(hours=1))
    assert alerts == []


def test_run_batch_single_window() -> None:
    """Events spanning exactly one window produce alerts from that window."""
    engine = _make_engine()
    events = [_event(minute_offset=i, notional_usd=2_000_000) for i in range(5)]

    start = _BASE
    end = _BASE + timedelta(minutes=5)
    alerts = engine.run_batch(events, "t1", start, end)
    assert len(alerts) == 5  # one per event


def test_run_batch_zero_window() -> None:
    """Batch with window_size=0 evaluates every event individually."""
    engine = _make_engine(config_fn=_zero_config)
    events = [
        _event(minute_offset=0, notional_usd=2_000_000),
        _event(minute_offset=1, notional_usd=100),
        _event(minute_offset=2, notional_usd=3_000_000),
    ]
    alerts = engine.run_batch(events, "t1", _BASE, _BASE + timedelta(minutes=5))
    assert len(alerts) == 2  # only the two above threshold


# ── Config integration tests ─────────────────────────────────────────────────


def test_disabled_algo_skipped() -> None:
    """Disabled algo returns no alerts even with matching events."""
    def disabled(_t: str, _a: str) -> tuple[bool, dict]:
        return False, {}

    engine = _make_engine(config_fn=_zero_config, algo_config_fn=disabled)
    alerts = engine.ingest(_event(notional_usd=2_000_000))
    assert alerts == []


def test_per_tenant_window_config() -> None:
    """Different tenants can have different window sizes."""
    def tenant_config(tenant_id: str) -> WindowConfig:
        if tenant_id == "fast":
            return WindowConfig(window_size_minutes=0, window_slide_minutes=0)
        return WindowConfig(window_size_minutes=5, window_slide_minutes=1)

    engine = _make_engine(config_fn=tenant_config)

    # "fast" tenant: immediate evaluation
    alerts_fast = engine.ingest(_event(tenant_id="fast", notional_usd=2_000_000))
    assert len(alerts_fast) == 1

    # "slow" tenant: needs 5-min window, one event isn't enough
    alerts_slow = engine.ingest(_event(tenant_id="slow", notional_usd=2_000_000))
    assert alerts_slow == []


def test_velocity_in_windowed_mode() -> None:
    """VelocityAlgo fires when window has more events than max."""
    engine = _make_engine(
        algos=[VelocityAlgo(max_events=3, window_seconds=60)],
        config_fn=_zero_config,
    )

    # Ingest 4 events at once via evaluate_window (zero-window processes one at a time)
    # With zero-window, each event is evaluated individually — velocity needs multiple events
    # Use batch mode instead to test windowed velocity
    events = [_event(minute_offset=i) for i in range(5)]
    start = _BASE
    end = _BASE + timedelta(minutes=10)

    batch_engine = _make_engine(
        algos=[VelocityAlgo(max_events=3, window_seconds=60)],
    )
    alerts = batch_engine.run_batch(events, "t1", start, end)
    # Each 5-min window containing >3 events triggers velocity
    assert any(a.algorithm == "velocity" for a in alerts)
