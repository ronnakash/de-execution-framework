"""E2E tests: client configuration service.

Tests CRUD for client configs and algorithm config management.
No auth middleware (JWT_SECRET not set for client_config in SharedPipeline).

6 tests.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import pytest

from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_create_and_get_client(harness):
    tid = harness.tenant_id
    status, body = await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "Test Client", "mode": "realtime"},
    )
    assert status == 201
    assert body["tenant_id"] == tid

    status2, body2 = await harness.call_service(
        "client_config", "GET", f"/api/v1/clients/{tid}",
    )
    assert status2 == 200
    assert body2["tenant_id"] == tid
    assert body2["display_name"] == "Test Client"
    assert body2["mode"] == "realtime"


async def test_update_client_mode(harness):
    tid = harness.tenant_id
    await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "Update Test", "mode": "realtime"},
    )

    status, body = await harness.call_service(
        "client_config", "PUT", f"/api/v1/clients/{tid}",
        json={"mode": "batch"},
    )
    assert status == 200
    assert body["mode"] == "batch"

    status2, body2 = await harness.call_service(
        "client_config", "GET", f"/api/v1/clients/{tid}",
    )
    assert status2 == 200
    assert body2["mode"] == "batch"


async def test_delete_client(harness):
    tid = harness.tenant_id
    await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "Delete Test", "mode": "realtime"},
    )

    status, body = await harness.call_service(
        "client_config", "DELETE", f"/api/v1/clients/{tid}",
    )
    assert status == 200
    assert body["deleted"] == tid

    status2, _ = await harness.call_service(
        "client_config", "GET", f"/api/v1/clients/{tid}",
    )
    assert status2 == 404


async def test_list_clients(harness):
    tid = harness.tenant_id
    await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "List Test", "mode": "realtime"},
    )

    status, body = await harness.call_service(
        "client_config", "GET", "/api/v1/clients",
    )
    assert status == 200
    assert isinstance(body, list)
    tenant_ids = [c["tenant_id"] for c in body]
    assert tid in tenant_ids


async def test_upsert_algo_config(harness):
    tid = harness.tenant_id
    await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "Algo Test", "mode": "realtime"},
    )

    status, body = await harness.call_service(
        "client_config", "PUT", f"/api/v1/clients/{tid}/algos/large_notional",
        json={"enabled": True, "thresholds": {"notional_usd": 2000000}},
    )
    assert status == 200
    assert body["algorithm"] == "large_notional"
    assert body["enabled"] is True

    status2, body2 = await harness.call_service(
        "client_config", "GET", f"/api/v1/clients/{tid}/algos",
    )
    assert status2 == 200
    assert isinstance(body2, list)
    algos = {a["algorithm"]: a for a in body2}
    assert "large_notional" in algos
    assert algos["large_notional"]["thresholds"]["notional_usd"] == 2000000


async def test_create_duplicate_client_returns_409(harness):
    tid = harness.tenant_id
    status1, _ = await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "Dup Test", "mode": "realtime"},
    )
    assert status1 == 201

    status2, _ = await harness.call_service(
        "client_config", "POST", "/api/v1/clients",
        json={"tenant_id": tid, "display_name": "Dup Test 2", "mode": "batch"},
    )
    assert status2 == 409
