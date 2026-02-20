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

import uuid
from typing import TYPE_CHECKING

from de_platform.pipeline.topics import TRADE_NORMALIZATION

from tests.helpers.events import (
    EVENT_FACTORY,
    EVENT_TABLE,
    REST_ENDPOINT,
    make_invalid,
    make_order,
    make_transaction,
)

if TYPE_CHECKING:
    from tests.helpers.harness import PipelineHarness


# ══════════════════════════════════════════════════════════════════════════════
# Matrix scenarios
# ══════════════════════════════════════════════════════════════════════════════


async def scenario_valid_events(
    harness: PipelineHarness, method: str, event_type: str
) -> None:
    """100 valid events -> 100 enriched rows in the data table, accessible via API."""
    events = [EVENT_FACTORY[event_type]() for _ in range(100)]
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
        {"tenant_id": "acme", "limit": "200"},
    )
    assert status == 200
    assert len(body) == 100, f"API returned {len(body)} events, expected 100"


async def scenario_invalid_events(
    harness: PipelineHarness, method: str, event_type: str
) -> None:
    """100 invalid events -> 100 rows in normalization_errors, 0 in valid table."""
    events = [make_invalid(event_type) for _ in range(100)]
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
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    events = [EVENT_FACTORY[event_type](id_=fixed_id) for _ in range(100)]
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
    event = {
        **make_order(id_="o-fixed"),
        "message_id": "dedup-test-fixed-msg-id",
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
    # $1.5M notional_usd: quantity=5000 x price=300 x USD rate 1.0
    big_order = make_order(quantity=5_000.0, price=300.0, currency="USD")
    await harness.ingest(method, "order", [big_order])

    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "large_notional"
    )
    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert large_notional, f"No large_notional alert via {method}"
    assert large_notional[0]["severity"] == "high"


async def scenario_large_notional(harness: PipelineHarness) -> None:
    """LargeNotionalAlgo: fires above $1M notional_usd."""
    # Above threshold ($1.5M = 5000 x 300 x 1.0)
    big = make_order(quantity=5_000.0, price=300.0, currency="USD")
    await harness.ingest("kafka", "order", [big])
    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "large_notional"
    )
    large_notional = [a for a in alerts if a.get("algorithm") == "large_notional"]
    assert len(large_notional) >= 1
    assert large_notional[0]["severity"] == "high"


async def scenario_velocity(harness: PipelineHarness) -> None:
    """VelocityAlgo: fires when a tenant submits more than 100 events in the window."""
    orders = [make_order() for _ in range(101)]
    await harness.ingest("rest", "order", orders)

    alerts = await harness.wait_for_alert(
        lambda r: r.get("algorithm") == "velocity"
    )
    velocity = [a for a in alerts if a.get("algorithm") == "velocity"]
    assert velocity, "VelocityAlgo should fire after 101 events"


async def scenario_suspicious_counterparty(harness: PipelineHarness) -> None:
    """SuspiciousCounterpartyAlgo: fires for counterparty IDs in the blocklist.

    Requires the harness to be configured with suspicious-counterparty-ids=bad-cp-1.
    """
    suspicious_tx = make_transaction(counterparty_id="bad-cp-1")
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
