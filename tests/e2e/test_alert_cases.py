"""E2E tests: alert manager and case aggregation.

Ingests large-notional orders to trigger alerts through the full
pipeline, then verifies alert manager API for alerts, cases,
and case lifecycle.

7 tests.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.helpers.events import make_order
from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


def _big_order(tenant_id: str, **kwargs) -> dict:
    """Create a large-notional order (>$1M) that triggers the large_notional algorithm."""
    return make_order(
        tenant_id=tenant_id,
        quantity=10000,
        price=150.0,
        currency="USD",
        **kwargs,
    )


async def _wait_for_alerts_in_manager(
    harness: RealInfraHarness,
    *,
    min_count: int = 1,
    timeout: float = 90.0,
) -> list[dict]:
    """Poll alert_manager /alerts until expected count for this tenant."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        status, body = await harness.call_service(
            "alert_manager", "GET", "/api/v1/alerts",
            params={"tenant_id": harness.tenant_id},
        )
        if status == 200 and isinstance(body, list) and len(body) >= min_count:
            return body
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(
                f"Expected {min_count} alerts in alert_manager within {timeout}s, "
                f"got {len(body) if isinstance(body, list) else body}"
            )
        await asyncio.sleep(0.5)


async def _wait_for_cases(
    harness: RealInfraHarness,
    *,
    min_count: int = 1,
    timeout: float = 90.0,
) -> list[dict]:
    """Poll alert_manager /cases until expected count for this tenant."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        status, body = await harness.call_service(
            "alert_manager", "GET", "/api/v1/cases",
            params={"tenant_id": harness.tenant_id},
        )
        if status == 200 and isinstance(body, list) and len(body) >= min_count:
            return body
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(
                f"Expected {min_count} cases in alert_manager within {timeout}s, "
                f"got {len(body) if isinstance(body, list) else body}"
            )
        await asyncio.sleep(0.5)


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_alert_appears_in_alert_manager(harness):
    events = [_big_order(harness.tenant_id)]
    await harness.ingest("rest", "order", events)

    alerts = await _wait_for_alerts_in_manager(harness, min_count=1)
    assert any(a.get("algorithm") == "large_notional" for a in alerts)


async def test_alert_creates_case(harness):
    events = [_big_order(harness.tenant_id)]
    await harness.ingest("rest", "order", events)

    cases = await _wait_for_cases(harness, min_count=1)
    assert cases[0]["status"] == "open"


async def test_update_case_status(harness):
    events = [_big_order(harness.tenant_id)]
    await harness.ingest("rest", "order", events)

    cases = await _wait_for_cases(harness, min_count=1)
    case_id = cases[0]["case_id"]

    status, body = await harness.call_service(
        "alert_manager", "PUT", f"/api/v1/cases/{case_id}/status",
        json={"status": "investigating"},
    )
    assert status == 200
    assert body["status"] == "investigating"


async def test_case_detail_includes_alerts(harness):
    events = [_big_order(harness.tenant_id)]
    await harness.ingest("rest", "order", events)

    cases = await _wait_for_cases(harness, min_count=1)
    case_id = cases[0]["case_id"]

    status, body = await harness.call_service(
        "alert_manager", "GET", f"/api/v1/cases/{case_id}",
    )
    assert status == 200
    assert "alerts" in body
    assert isinstance(body["alerts"], list)
    assert len(body["alerts"]) >= 1


async def test_cases_summary(harness):
    events = [_big_order(harness.tenant_id)]
    await harness.ingest("rest", "order", events)

    await _wait_for_cases(harness, min_count=1)

    status, body = await harness.call_service(
        "alert_manager", "GET", "/api/v1/cases/summary",
        params={"tenant_id": harness.tenant_id},
    )
    assert status == 200
    assert "total" in body
    assert body["total"] >= 1
    assert "by_status" in body
    assert "by_severity" in body


async def test_multiple_alerts_aggregate_into_case(harness):
    events = [_big_order(harness.tenant_id) for _ in range(2)]
    await harness.ingest("rest", "order", events)

    alerts = await _wait_for_alerts_in_manager(harness, min_count=2)
    assert len(alerts) >= 2

    cases = await _wait_for_cases(harness, min_count=1)
    # Multiple alerts from same algorithm should aggregate into one case
    assert len(cases) >= 1
    assert cases[0].get("alert_count", 1) >= 2 or len(cases) == 1


async def test_alert_case_lookup(harness):
    events = [_big_order(harness.tenant_id)]
    await harness.ingest("rest", "order", events)

    alerts = await _wait_for_alerts_in_manager(harness, min_count=1)
    alert_id = alerts[0]["alert_id"]

    status, body = await harness.call_service(
        "alert_manager", "GET", f"/api/v1/alerts/{alert_id}/case",
    )
    assert status == 200
    assert "case_id" in body
