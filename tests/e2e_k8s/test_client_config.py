"""K8s E2E tests: client configuration service.

Same as tests/e2e/test_client_config.py — all tests are HTTP-only.

Requires: ``pytest -m e2e_k8s`` or ``make test-e2e-k8s``.
"""

from __future__ import annotations

import pytest

from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e_k8s]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_create_and_get_client(harness):
    tid = harness.tenant_id

    async with harness.step(
        "Create client",
        f"POST /clients with tenant_id={tid}, mode=realtime — expect 201",
    ):
        status, body = await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "Test Client", "mode": "realtime"},
        )
        assert status == 201
        assert body["tenant_id"] == tid

    async with harness.step(
        "Get client",
        f"GET /clients/{tid} — verify display_name and mode match",
    ):
        status2, body2 = await harness.call_service(
            "client_config", "GET", f"/api/v1/clients/{tid}",
        )
        assert status2 == 200
        assert body2["tenant_id"] == tid
        assert body2["display_name"] == "Test Client"
        assert body2["mode"] == "realtime"


async def test_update_client_mode(harness):
    tid = harness.tenant_id

    async with harness.step(
        "Create client",
        "POST /clients with mode=realtime",
    ):
        await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "Update Test", "mode": "realtime"},
        )

    async with harness.step(
        "Update mode to batch",
        f"PUT /clients/{tid} with mode=batch — expect 200",
    ):
        status, body = await harness.call_service(
            "client_config", "PUT", f"/api/v1/clients/{tid}",
            json={"mode": "batch"},
        )
        assert status == 200
        assert body["mode"] == "batch"

    async with harness.step(
        "Verify update persisted",
        f"GET /clients/{tid} — confirm mode is now batch",
    ):
        status2, body2 = await harness.call_service(
            "client_config", "GET", f"/api/v1/clients/{tid}",
        )
        assert status2 == 200
        assert body2["mode"] == "batch"


async def test_delete_client(harness):
    tid = harness.tenant_id

    async with harness.step(
        "Create client",
        "POST /clients — create a client to delete",
    ):
        await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "Delete Test", "mode": "realtime"},
        )

    async with harness.step(
        "Delete client",
        f"DELETE /clients/{tid} — expect 200 with deleted confirmation",
    ):
        status, body = await harness.call_service(
            "client_config", "DELETE", f"/api/v1/clients/{tid}",
        )
        assert status == 200
        assert body["deleted"] == tid

    async with harness.step(
        "Verify deletion",
        f"GET /clients/{tid} — expect 404 Not Found",
    ):
        status2, _ = await harness.call_service(
            "client_config", "GET", f"/api/v1/clients/{tid}",
        )
        assert status2 == 404


async def test_list_clients(harness):
    tid = harness.tenant_id

    async with harness.step(
        "Create client",
        "POST /clients — create a client to appear in listing",
    ):
        await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "List Test", "mode": "realtime"},
        )

    async with harness.step(
        "List all clients",
        "GET /clients — expect our tenant_id in the list",
    ):
        status, body = await harness.call_service(
            "client_config", "GET", "/api/v1/clients",
        )
        assert status == 200
        assert isinstance(body, list)
        tenant_ids = [c["tenant_id"] for c in body]
        assert tid in tenant_ids


async def test_upsert_algo_config(harness):
    tid = harness.tenant_id

    async with harness.step(
        "Create client",
        "POST /clients — create client before configuring algorithms",
    ):
        await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "Algo Test", "mode": "realtime"},
        )

    async with harness.step(
        "Set algorithm config",
        "PUT /clients/{tid}/algos/large_notional — enable with $2M threshold",
    ):
        status, body = await harness.call_service(
            "client_config", "PUT", f"/api/v1/clients/{tid}/algos/large_notional",
            json={"enabled": True, "thresholds": {"notional_usd": 2000000}},
        )
        assert status == 200
        assert body["algorithm"] == "large_notional"
        assert body["enabled"] is True

    async with harness.step(
        "Verify algo config",
        f"GET /clients/{tid}/algos — confirm large_notional enabled with correct threshold",
    ):
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

    async with harness.step(
        "Create client",
        f"POST /clients with tenant_id={tid} — expect 201",
    ):
        status1, _ = await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "Dup Test", "mode": "realtime"},
        )
        assert status1 == 201

    async with harness.step(
        "Create duplicate client",
        "POST /clients with same tenant_id — expect 409 Conflict",
    ):
        status2, _ = await harness.call_service(
            "client_config", "POST", "/api/v1/clients",
            json={"tenant_id": tid, "display_name": "Dup Test 2", "mode": "batch"},
        )
        assert status2 == 409
