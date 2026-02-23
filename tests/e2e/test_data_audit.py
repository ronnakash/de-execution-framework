"""E2E tests: data audit service.

Ingests events through the pipeline and verifies that the data_audit
module correctly counts received, processed, and error events.

4 tests.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.helpers.events import make_invalid, make_order, make_transaction
from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


async def _wait_for_audit_counts(
    harness: RealInfraHarness,
    *,
    min_processed: int = 0,
    min_errors: int = 0,
    timeout: float = 60.0,
) -> dict:
    """Poll /audit/summary until expected counts appear."""
    deadline = asyncio.get_event_loop().time() + timeout
    last_body = {}
    while True:
        status, body = await harness.call_service(
            "data_audit", "GET", "/api/v1/audit/summary",
            params={"tenant_id": harness.tenant_id},
        )
        if status == 200:
            last_body = body
            total_processed = body.get("total_processed", 0)
            total_errors = body.get("total_errors", 0)
            if total_processed >= min_processed and total_errors >= min_errors:
                return body
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(
                f"Audit counts not met within {timeout}s: "
                f"wanted processed>={min_processed}, errors>={min_errors}, "
                f"got {last_body}"
            )
        await asyncio.sleep(0.5)


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_valid_events_counted(harness):
    events = [make_order(tenant_id=harness.tenant_id) for _ in range(5)]
    await harness.ingest("rest", "order", events)

    # Wait for persistence first
    await harness.wait_for_rows("orders", 5)

    summary = await _wait_for_audit_counts(harness, min_processed=5)
    assert summary["total_processed"] >= 5


async def test_invalid_events_counted_as_errors(harness):
    events = [make_invalid("order", tenant_id=harness.tenant_id) for _ in range(3)]
    await harness.ingest("rest", "order", events)

    # Wait for error rows in normalization_errors
    await harness.wait_for_rows("normalization_errors", 3)

    summary = await _wait_for_audit_counts(harness, min_errors=3)
    assert summary["total_errors"] >= 3


async def test_daily_audit_endpoint(harness):
    events = [make_order(tenant_id=harness.tenant_id) for _ in range(3)]
    await harness.ingest("rest", "order", events)

    await harness.wait_for_rows("orders", 3)
    await _wait_for_audit_counts(harness, min_processed=3)

    status, body = await harness.call_service(
        "data_audit", "GET", "/api/v1/audit/daily",
        params={"tenant_id": harness.tenant_id},
    )
    assert status == 200
    assert isinstance(body, list)
    assert len(body) > 0
    record = body[0]
    assert "date" in record
    assert "event_type" in record


async def test_summary_by_event_type(harness):
    orders = [make_order(tenant_id=harness.tenant_id) for _ in range(3)]
    txns = [make_transaction(tenant_id=harness.tenant_id) for _ in range(2)]
    await harness.ingest("rest", "order", orders)
    await harness.ingest("rest", "transaction", txns)

    await harness.wait_for_rows("orders", 3)
    await harness.wait_for_rows("transactions", 2)
    summary = await _wait_for_audit_counts(harness, min_processed=5)

    by_type = summary.get("by_event_type", {})
    assert "order" in by_type or "trade" in by_type
    assert "transaction" in by_type
