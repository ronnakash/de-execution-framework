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

    async with harness.step(
        "Create task",
        f"POST /tasks with task_id={task_id}, module=hello_world — expect 201",
    ):
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

    async with harness.step(
        "Get task",
        f"GET /tasks/{task_id} — verify name and module_name",
    ):
        status2, body2 = await harness.call_service(
            "task_scheduler", "GET", f"/api/v1/tasks/{task_id}",
        )
        assert status2 == 200
        assert body2["task_id"] == task_id
        assert body2["name"] == "Test Task"
        assert body2["module_name"] == "hello_world"


async def test_list_tasks(harness):
    task_id = _unique_task_id()

    async with harness.step(
        "Create task",
        "POST /tasks — create a task to appear in listing",
    ):
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

    async with harness.step(
        "List tasks",
        "GET /tasks — expect our task_id in the list",
    ):
        status, body = await harness.call_service(
            "task_scheduler", "GET", "/api/v1/tasks",
        )
        assert status == 200
        assert isinstance(body, list)
        task_ids = [t["task_id"] for t in body]
        assert task_id in task_ids


async def test_update_task(harness):
    task_id = _unique_task_id()

    async with harness.step(
        "Create task",
        "POST /tasks with name='Before Update'",
    ):
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

    async with harness.step(
        "Update task name",
        f"PUT /tasks/{task_id} with name='After Update' — expect 200",
    ):
        status, body = await harness.call_service(
            "task_scheduler", "PUT", f"/api/v1/tasks/{task_id}",
            json={"name": "After Update"},
        )
        assert status == 200
        assert body["name"] == "After Update"

    async with harness.step(
        "Verify update persisted",
        f"GET /tasks/{task_id} — confirm name is 'After Update'",
    ):
        status2, body2 = await harness.call_service(
            "task_scheduler", "GET", f"/api/v1/tasks/{task_id}",
        )
        assert status2 == 200
        assert body2["name"] == "After Update"


async def test_delete_task(harness):
    task_id = _unique_task_id()

    async with harness.step(
        "Create task",
        "POST /tasks — create a task to delete",
    ):
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

    async with harness.step(
        "Delete task",
        f"DELETE /tasks/{task_id} — expect 200 with deleted confirmation",
    ):
        status, body = await harness.call_service(
            "task_scheduler", "DELETE", f"/api/v1/tasks/{task_id}",
        )
        assert status == 200
        assert body["deleted"] == task_id

    async with harness.step(
        "Verify deletion",
        f"GET /tasks/{task_id} — expect 404 Not Found",
    ):
        status2, _ = await harness.call_service(
            "task_scheduler", "GET", f"/api/v1/tasks/{task_id}",
        )
        assert status2 == 404


async def test_trigger_manual_run(harness):
    task_id = _unique_task_id()

    async with harness.step(
        "Create enabled task",
        f"POST /tasks with task_id={task_id}, enabled=True",
    ):
        await harness.call_service(
            "task_scheduler", "POST", "/api/v1/tasks",
            json={
                "task_id": task_id,
                "name": "Manual Run Test",
                "module_name": "hello_world",
                "default_args": {},
                "enabled": True,
            },
        )

    async with harness.step(
        "Trigger manual run",
        f"POST /tasks/{task_id}/run — expect 201 with run_id and status",
    ):
        status, body = await harness.call_service(
            "task_scheduler", "POST", f"/api/v1/tasks/{task_id}/run",
        )
        assert status == 201
        assert body["task_id"] == task_id
        assert "run_id" in body
        assert body["status"] in ("running", "completed", "failed")


async def test_get_run_details(harness):
    task_id = _unique_task_id()

    async with harness.step(
        "Create and trigger task",
        "POST /tasks + POST /tasks/{id}/run to get a run_id",
    ):
        await harness.call_service(
            "task_scheduler", "POST", "/api/v1/tasks",
            json={
                "task_id": task_id,
                "name": "Run Details Test",
                "module_name": "hello_world",
                "default_args": {},
                "enabled": True,
            },
        )
        _, run_body = await harness.call_service(
            "task_scheduler", "POST", f"/api/v1/tasks/{task_id}/run",
        )
        run_id = run_body["run_id"]

    async with harness.step(
        "Wait for completion",
        "Pause 2s for the run to complete",
    ):
        await asyncio.sleep(2)

    async with harness.step(
        "Get run details",
        f"GET /runs/{run_id} — expect run_id, task_id, status, started_at",
    ):
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

    async with harness.step(
        "Create task",
        f"POST /tasks with task_id={task_id} — expect 201",
    ):
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

    async with harness.step(
        "Create duplicate task",
        "POST /tasks with same task_id — expect 409 Conflict",
    ):
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
