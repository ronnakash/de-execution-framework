"""E2E tests: task scheduler service.

Tests CRUD for task definitions, manual task runs, and run details.
No auth middleware (JWT_SECRET not set for task_scheduler in SharedPipeline).

7 tests.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


def _unique_task_id() -> str:
    return f"task_{uuid.uuid4().hex[:12]}"


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_create_and_get_task(harness):
    task_id = _unique_task_id()
    status, body = await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Test Task",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )
    assert status == 201
    assert body["task_id"] == task_id

    status2, body2 = await harness.call_service(
        "task_scheduler", "GET", f"/api/v1/tasks/{task_id}",
    )
    assert status2 == 200
    assert body2["task_id"] == task_id
    assert body2["name"] == "Test Task"
    assert body2["module_name"] == "hello_world"


async def test_list_tasks(harness):
    task_id = _unique_task_id()
    await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "List Test",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )

    status, body = await harness.call_service(
        "task_scheduler", "GET", "/api/v1/tasks",
    )
    assert status == 200
    assert isinstance(body, list)
    task_ids = [t["task_id"] for t in body]
    assert task_id in task_ids


async def test_update_task(harness):
    task_id = _unique_task_id()
    await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Before Update",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )

    status, body = await harness.call_service(
        "task_scheduler", "PUT", f"/api/v1/tasks/{task_id}",
        json={"name": "After Update"},
    )
    assert status == 200
    assert body["name"] == "After Update"

    status2, body2 = await harness.call_service(
        "task_scheduler", "GET", f"/api/v1/tasks/{task_id}",
    )
    assert status2 == 200
    assert body2["name"] == "After Update"


async def test_delete_task(harness):
    task_id = _unique_task_id()
    await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Delete Test",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )

    status, body = await harness.call_service(
        "task_scheduler", "DELETE", f"/api/v1/tasks/{task_id}",
    )
    assert status == 200
    assert body["deleted"] == task_id

    status2, _ = await harness.call_service(
        "task_scheduler", "GET", f"/api/v1/tasks/{task_id}",
    )
    assert status2 == 404


async def test_trigger_manual_run(harness):
    task_id = _unique_task_id()
    await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Manual Run Test",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )

    status, body = await harness.call_service(
        "task_scheduler", "POST", f"/api/v1/tasks/{task_id}/run",
    )
    assert status == 201
    assert body["task_id"] == task_id
    assert "run_id" in body
    assert body["status"] in ("running", "completed", "failed")


async def test_get_run_details(harness):
    task_id = _unique_task_id()
    await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Run Details Test",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )

    _, run_body = await harness.call_service(
        "task_scheduler", "POST", f"/api/v1/tasks/{task_id}/run",
    )
    run_id = run_body["run_id"]

    # Wait briefly for the run to complete
    await asyncio.sleep(2)

    status, body = await harness.call_service(
        "task_scheduler", "GET", f"/api/v1/runs/{run_id}",
    )
    assert status == 200
    assert body["run_id"] == run_id
    assert body["task_id"] == task_id
    assert "status" in body
    assert "started_at" in body


async def test_create_duplicate_task_returns_409(harness):
    task_id = _unique_task_id()
    status1, _ = await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Dup Test",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )
    assert status1 == 201

    status2, _ = await harness.call_service(
        "task_scheduler", "POST", "/api/v1/tasks",
        json={
            "task_id": task_id,
            "name": "Dup Test 2",
            "module_name": "hello_world",
            "default_args": {},
            "enabled": False,
        },
    )
    assert status2 == 409
