"""Shared E2E test scenarios.

Each scenario function takes a PipelineHarness as its first argument and
performs assertions against it.  Test files import these functions and call
them with their specific harness implementation.

Scenarios:
  Matrix (parametrized with method x event_type):
    - scenario_valid_events:     100 valid events -> 100 rows + API accessible
    - scenario_invalid_events:   100 invalid events -> 100 errors, 0 valid
    - scenario_duplicate_events: same event 100x -> 1 valid + 99 duplicates

  General:
    - scenario_internal_dedup:          same message_id silently dropped
    - scenario_alert_via_method:        large-notional order triggers alert
    - scenario_large_notional:          algorithm fires above $1M
    - scenario_velocity:                velocity algo after threshold
    - scenario_suspicious_counterparty: blocklist counterparty triggers alert
"""

from __future__ import annotations

import json
import uuid
from typing import Any, TYPE_CHECKING

from de_platform.pipeline.topics import TRADE_NORMALIZATION

from tests.helpers.events import (
    EVENT_FACTORY,
    EVENT_TABLE,
    REST_ENDPOINT,
    make_invalid,
    make_multi_invalid,
    make_order,
    make_transaction,
)

if TYPE_CHECKING:
    from tests.helpers.harness import PipelineHarness


def _ensure_parsed(value: Any) -> Any:
    """Parse a JSON string to a native Python object, or return as-is.

    ClickHouse stores dicts/lists as JSON strings in String columns.
    MemoryDatabase keeps them as native Python types. This helper
    normalizes both to native types for assertions.
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


# ══════════════════════════════════════════════════════════════════════════════
# Matrix scenarios
# ══════════════════════════════════════════════════════════════════════════════


async def scenario_valid_events(
    harness: PipelineHarness, method: str, event_type: str
) -> None:
    """100 valid events -> 100 enriched rows in the data table, accessible via API."""
    tenant_id = harness.tenant_id
    events = [EVENT_FACTORY[event_type](tenant_id=tenant_id) for _ in range(100)]
    await harness.ingest(method, event_type, events)

    table = EVENT_TABLE[event_type]
    rows = await harness.wait_for_rows(table, expected=100)
    assert len(rows) == 100, f"Expected 100 rows in {table}, got {len(rows)}"

    # Normalizer must have enriched every row
    for row in rows:
        assert "primary_key" in row, "primary_key missing after normalization"
        assert "normalized_at" in row, "normalized_at missing after normalization"
        if event_type in ("order", "execution"):
            assert "notional_usd" in row
        else:
            assert "amount_usd" in row

    # No validation errors should exist (errors table checked after valid rows
    # are confirmed, so the pipeline has finished processing all events)
    error_rows = await harness.wait_for_rows("normalization_errors", expected=0)
    assert error_rows == [], (
        f"Expected 0 normalization errors, got {len(error_rows)}"
    )

    # Events must be accessible via the Data API
    status, body = await harness.query_api(
        f"events/{REST_ENDPOINT[event_type]}",
        {"tenant_id": tenant_id, "limit": "200"},
    )
    assert status == 200
    assert len(body) == 100, f"API returned {len(body)} events, expected 100"


async def scenario_invalid_events(
    harness: PipelineHarness, method: str, event_type: str
) -> None:
    """100 invalid events -> 100 rows in normalization_errors, 0 in valid table."""
    tenant_id = harness.tenant_id
    events = [make_invalid(event_type, tenant_id=tenant_id) for _ in range(100)]
    await harness.ingest(method, event_type, events)

    table = EVENT_TABLE[event_type]

    # All invalid events should end up as errors
    error_rows = await harness.wait_for_rows("normalization_errors", expected=100)
    assert len(error_rows) == 100, (
        f"Expected 100 error rows, got {len(error_rows)}"
    )
    for row in error_rows:
        assert row.get("event_type") == event_type

    # Nothing should be in the valid table
    valid_rows = await harness.wait_for_rows(table, expected=0)
    assert valid_rows == [], (
        f"Valid table '{table}' must be empty for 100% invalid input, "
        f"found {len(valid_rows)}"
    )


async def scenario_duplicate_events(
    harness: PipelineHarness, method: str, event_type: str
) -> None:
    """Same event data sent 100x -> 1 valid row, 99 external duplicates.

    Each ingestion assigns a fresh message_id, so events 2-100 share the same
    primary_key but have different message_ids -> classified as external duplicates.
    """
    tenant_id = harness.tenant_id
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    events = [EVENT_FACTORY[event_type](id_=fixed_id, tenant_id=tenant_id) for _ in range(100)]
    await harness.ingest(method, event_type, events)

    table = EVENT_TABLE[event_type]
    valid_rows = await harness.wait_for_rows(table, expected=1)
    dup_rows = await harness.wait_for_rows("duplicates", expected=99)

    assert len(valid_rows) == 1, (
        f"Expected exactly 1 valid row, got {len(valid_rows)}"
    )
    assert len(dup_rows) == 99, (
        f"Expected 99 duplicate rows, got {len(dup_rows)}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# General scenarios
# ══════════════════════════════════════════════════════════════════════════════


async def scenario_internal_dedup(harness: PipelineHarness) -> None:
    """Same message_id arriving at the normalizer twice -> second copy silently dropped.

    This simulates Kafka at-least-once delivery replaying a message.
    """
    tenant_id = harness.tenant_id
    event = {
        **make_order(id_="o-fixed", tenant_id=tenant_id),
        "message_id": f"dedup-test-{uuid.uuid4().hex[:8]}",
        "ingested_at": "2026-01-15T10:00:00+00:00",
        "event_type": "order",
    }

    # First pass: should produce 1 row
    await harness.publish_to_normalizer(TRADE_NORMALIZATION, event)
    rows = await harness.wait_for_rows("orders", expected=1)
    assert len(rows) == 1

    # Second pass: same message_id -> should still be 1 row
    await harness.publish_to_normalizer(TRADE_NORMALIZATION, dict(event))
    await harness.wait_for_no_new_rows("orders", known=1)


async def scenario_alert_via_method(
    harness: PipelineHarness, method: str
) -> None:
    """Large-notional order triggers a large_notional alert via the given method."""
    tenant_id = harness.tenant_id
    # $1.5M notional_usd: quantity=5000 x price=300 x USD rate 1.0
    big_order = make_order(quantity=5_000.0, price=300.0, currency="USD", tenant_id=tenant_id)
    await harness.ingest(method, "order", [big_order])

    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "large_notional"
    )
    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert large_notional, f"No large_notional alert via {method}"
    assert large_notional[0]["severity"] == "high"


async def scenario_large_notional(harness: PipelineHarness) -> None:
    """LargeNotionalAlgo: fires above $1M notional_usd."""
    tenant_id = harness.tenant_id
    # Above threshold ($1.5M = 5000 x 300 x 1.0)
    big = make_order(quantity=5_000.0, price=300.0, currency="USD", tenant_id=tenant_id)
    await harness.ingest("kafka", "order", [big])
    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "large_notional"
    )
    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert len(large_notional) >= 1
    assert large_notional[0]["severity"] == "high"


async def scenario_velocity(harness: PipelineHarness) -> None:
    """VelocityAlgo: fires when a tenant submits more than 100 events in the window.

    We send 151 events at 2s intervals (span = 300s = 5 min exactly).  The
    engine evaluates when span >= window_size (300s).  The window [t0, t0+5m)
    contains events 0-149 (150 events) — event 150 at t=300s is excluded by
    the strict upper bound.  150 > 100 triggers the velocity alert.

    The harness default is window_size=0 (per-event evaluation).  This scenario
    overrides the tenant's window config to enable sliding-window mode.
    """
    tenant_id = harness.tenant_id

    # Enable 5-min sliding window for this tenant (default is 0 = per-event)
    if hasattr(harness, "cache"):
        harness.cache.set(f"client_config:{tenant_id}", {
            "mode": "realtime",
            "window_size_minutes": 5,
            "window_slide_minutes": 1,
        })

    from datetime import datetime, timedelta

    base = datetime.fromisoformat("2026-01-15T10:00:00+00:00")
    orders = [
        make_order(
            tenant_id=tenant_id,
            transact_time=(base + timedelta(seconds=i * 2)).isoformat(),
        )
        for i in range(151)
    ]
    await harness.ingest("rest", "order", orders)

    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "velocity"
    )
    velocity = [a for a in alerts if a.get("algorithm") == "velocity"]
    assert velocity, "VelocityAlgo should fire after 151 events"


async def scenario_suspicious_counterparty(harness: PipelineHarness) -> None:
    """SuspiciousCounterpartyAlgo: fires for counterparty IDs in the blocklist.

    Requires the harness to be configured with suspicious-counterparty-ids=bad-cp-1.
    """
    tenant_id = harness.tenant_id
    suspicious_tx = make_transaction(counterparty_id="bad-cp-1", tenant_id=tenant_id)
    await harness.ingest("rest", "transaction", [suspicious_tx])

    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "suspicious_counterparty"
    )
    suspicious = [
        a for a in alerts if a.get("algorithm") == "suspicious_counterparty"
    ]
    assert suspicious, (
        "SuspiciousCounterpartyAlgo should fire for blocklisted counterparty"
    )


async def scenario_multi_error_consolidation(
    harness: PipelineHarness, method: str, event_type: str
) -> None:
    """Events with multiple validation errors produce exactly 1 error row each.

    Each event has at least 2 validation failures (empty id + bad currency).
    We ingest 10 such events and expect exactly 10 error rows, not 20+.
    Each row should have an ``errors`` field that is a list with >= 2 entries,
    and a ``raw_data`` field containing the original event.
    """
    tenant_id = harness.tenant_id
    events = [make_multi_invalid(event_type, tenant_id=tenant_id) for _ in range(10)]
    await harness.ingest(method, event_type, events)

    error_rows = await harness.wait_for_rows("normalization_errors", expected=10)
    assert len(error_rows) == 10, (
        f"Expected exactly 10 consolidated error rows, got {len(error_rows)}"
    )
    for row in error_rows:
        errors = _ensure_parsed(row.get("errors"))
        assert isinstance(errors, list), f"errors should be a list, got {type(errors)}"
        assert len(errors) >= 2, (
            f"Each error row should have >= 2 errors, got {len(errors)}"
        )
        raw_data = _ensure_parsed(row.get("raw_data"))
        assert raw_data is not None, "raw_data should contain the original event"

    # No valid rows should exist
    table = EVENT_TABLE[event_type]
    valid_rows = await harness.wait_for_rows(table, expected=0)
    assert valid_rows == [], (
        f"Valid table '{table}' must be empty for 100% invalid input"
    )


async def scenario_duplicate_contains_original_event(
    harness: PipelineHarness,
) -> None:
    """Duplicate record should contain the full original event.

    Ingest the same event twice (different message_id on second ingestion).
    The duplicate row should have ``original_event`` containing the full event.
    """
    tenant_id = harness.tenant_id
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    event = EVENT_FACTORY["order"](id_=fixed_id, tenant_id=tenant_id)
    await harness.ingest("kafka", "order", [event])

    # First event should land in the valid table
    rows = await harness.wait_for_rows("orders", expected=1)
    assert len(rows) == 1

    # Ingest the same event again (starter assigns new message_id)
    await harness.ingest("kafka", "order", [event])

    dup_rows = await harness.wait_for_rows("duplicates", expected=1)
    assert len(dup_rows) == 1
    dup = dup_rows[0]
    assert "original_event" in dup, "duplicate row must have original_event"
    original = _ensure_parsed(dup["original_event"])
    assert isinstance(original, dict), "original_event should be a dict"
    assert original.get("id") == fixed_id
