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
    async with harness.step(
        "Ingest large order",
        "Publish a $1.5M order via REST to trigger large_notional algorithm",
    ):
        events = [_big_order(harness.tenant_id)]
        await harness.ingest("rest", "order", events)

    async with harness.step(
        "Verify alert in manager",
        "Poll /alerts — expect large_notional alert for this tenant",
    ):
        alerts = await _wait_for_alerts_in_manager(harness, min_count=1)
        assert any(a.get("algorithm") == "large_notional" for a in alerts)


async def test_alert_creates_case(harness):
    async with harness.step(
        "Ingest large order",
        "Publish a $1.5M order via REST to trigger alert + case creation",
    ):
        events = [_big_order(harness.tenant_id)]
        await harness.ingest("rest", "order", events)

    async with harness.step(
        "Verify case created",
        "Poll /cases — expect at least 1 case with status=open",
    ):
        cases = await _wait_for_cases(harness, min_count=1)
        assert cases[0]["status"] == "open"


async def test_update_case_status(harness):
    async with harness.step(
        "Trigger alert and case",
        "Ingest large order, wait for case to appear",
    ):
        events = [_big_order(harness.tenant_id)]
        await harness.ingest("rest", "order", events)
        cases = await _wait_for_cases(harness, min_count=1)
        case_id = cases[0]["case_id"]

    async with harness.step(
        "Update case status",
        f"PUT /cases/{case_id}/status to 'investigating' — expect 200",
    ):
        status, body = await harness.call_service(
            "alert_manager", "PUT", f"/api/v1/cases/{case_id}/status",
            json={"status": "investigating"},
        )
        assert status == 200
        assert body["status"] == "investigating"


async def test_case_detail_includes_alerts(harness):
    async with harness.step(
        "Trigger alert and case",
        "Ingest large order, wait for case to appear",
    ):
        events = [_big_order(harness.tenant_id)]
        await harness.ingest("rest", "order", events)
        cases = await _wait_for_cases(harness, min_count=1)
        case_id = cases[0]["case_id"]

    async with harness.step(
        "Get case detail",
        f"GET /cases/{case_id} — expect alerts list with at least 1 alert",
    ):
        status, body = await harness.call_service(
            "alert_manager", "GET", f"/api/v1/cases/{case_id}",
        )
        assert status == 200
        assert "alerts" in body
        assert isinstance(body["alerts"], list)
        assert len(body["alerts"]) >= 1


async def test_cases_summary(harness):
    async with harness.step(
        "Trigger alert and case",
        "Ingest large order, wait for case to appear",
    ):
        events = [_big_order(harness.tenant_id)]
        await harness.ingest("rest", "order", events)
        await _wait_for_cases(harness, min_count=1)

    async with harness.step(
        "Get cases summary",
        "GET /cases/summary — expect total >= 1, by_status, by_severity breakdowns",
    ):
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
    async with harness.step(
        "Ingest 2 large orders",
        "Publish 2 large orders to trigger 2 alerts from same algorithm",
    ):
        events = [_big_order(harness.tenant_id) for _ in range(2)]
        await harness.ingest("rest", "order", events)

    async with harness.step(
        "Verify alert aggregation",
        "Wait for 2+ alerts, then check they aggregate into 1 case",
    ):
        alerts = await _wait_for_alerts_in_manager(harness, min_count=2)
        assert len(alerts) >= 2
        cases = await _wait_for_cases(harness, min_count=1)
        assert len(cases) >= 1
        assert cases[0].get("alert_count", 1) >= 2 or len(cases) == 1


async def test_alert_case_lookup(harness):
    async with harness.step(
        "Trigger alert",
        "Ingest large order, wait for alert to appear in manager",
    ):
        events = [_big_order(harness.tenant_id)]
        await harness.ingest("rest", "order", events)
        alerts = await _wait_for_alerts_in_manager(harness, min_count=1)
        alert_id = alerts[0]["alert_id"]

    async with harness.step(
        "Look up case from alert",
        f"GET /alerts/{alert_id}/case — expect case_id linking alert to case",
    ):
        status, body = await harness.call_service(
            "alert_manager", "GET", f"/api/v1/alerts/{alert_id}/case",
        )
        assert status == 200
        assert "case_id" in body
