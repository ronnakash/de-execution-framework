"""Tests for the Task Scheduler Service."""

from __future__ import annotations

import json

from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.task_scheduler.main import TaskSchedulerModule
from de_platform.pipeline.task_arg_generators import batch_algos_args
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets

# ── Helpers ──────────────────────────────────────────────────────────────────


def _stub_executor(module_name: str, args: dict) -> tuple[int, str | None]:
    """Test stub that simulates a successful subprocess run."""
    return 0, None


def _failing_executor(module_name: str, args: dict) -> tuple[int, str | None]:
    """Test stub that simulates a failed subprocess run."""
    return 1, "module crashed"


async def _setup_module(
    executor=_stub_executor,
) -> tuple[TaskSchedulerModule, MemoryDatabase]:
    db = MemoryDatabase()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})
    secrets = EnvSecrets()

    module = TaskSchedulerModule(
        config=config, logger=logger, db=db,
        lifecycle=lifecycle, metrics=NoopMetrics(), secrets=secrets,
    )
    module._execute_task_fn = executor
    await module.initialize()
    return module, db


def _seed_task(db: MemoryDatabase, **overrides: object) -> dict:
    """Insert a task_definitions row with defaults."""
    row = {
        "task_id": "test_task",
        "name": "Test Task",
        "module_name": "hello_world",
        "default_args": json.dumps({}),
        "schedule_cron": None,
        "enabled": True,
        "arg_generator": None,
    }
    row.update(overrides)
    db.insert_one("task_definitions", row)
    return row


# ── Task CRUD tests ─────────────────────────────────────────────────────────


async def test_create_task() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks", json={
            "task_id": "my_task",
            "name": "My Task",
            "module_name": "currency_loader",
            "default_args": {"rates-file": "rates.json"},
            "schedule_cron": "0 * * * *",
        })
        assert resp.status == 201
        data = await resp.json()
        assert data["task_id"] == "my_task"
        assert data["module_name"] == "currency_loader"
        assert data["default_args"]["rates-file"] == "rates.json"

    rows = db.fetch_all("SELECT * FROM task_definitions")
    assert len(rows) == 1


async def test_create_task_missing_fields() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks", json={
            "task_id": "t1",
        })
        assert resp.status == 400


async def test_create_task_duplicate() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks", json={
            "task_id": "t1",
            "name": "Dup",
            "module_name": "hello_world",
        })
        assert resp.status == 409


async def test_list_tasks() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1", name="Task 1")
    _seed_task(db, task_id="t2", name="Task 2")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/tasks")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2


async def test_get_task() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1", name="Task 1")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/tasks/t1")
        assert resp.status == 200
        data = await resp.json()
        assert data["name"] == "Task 1"


async def test_get_task_not_found() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/tasks/nonexistent")
        assert resp.status == 404


async def test_update_task() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1", name="Old Name")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/tasks/t1", json={
            "name": "New Name",
            "schedule_cron": "30 2 * * *",
        })
        assert resp.status == 200
        data = await resp.json()
        assert data["name"] == "New Name"
        assert data["schedule_cron"] == "30 2 * * *"


async def test_update_task_not_found() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/tasks/nonexistent", json={"name": "X"})
        assert resp.status == 404


async def test_delete_task() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.delete("/api/v1/tasks/t1")
        assert resp.status == 200

    rows = db.fetch_all("SELECT * FROM task_definitions")
    assert len(rows) == 0


async def test_delete_task_not_found() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.delete("/api/v1/tasks/nonexistent")
        assert resp.status == 404


async def test_delete_task_cascades_runs() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1")
    db.insert_one("task_runs", {
        "run_id": "r1", "task_id": "t1", "status": "completed",
        "args": json.dumps({}),
    })
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.delete("/api/v1/tasks/t1")
        assert resp.status == 200

    assert len(db.fetch_all("SELECT * FROM task_definitions")) == 0
    assert len(db.fetch_all("SELECT * FROM task_runs")) == 0


# ── Task trigger / run tests ────────────────────────────────────────────────


async def test_trigger_run_success() -> None:
    module, db = await _setup_module(executor=_stub_executor)
    _seed_task(db, task_id="t1", module_name="hello_world")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks/t1/run")
        assert resp.status == 201
        data = await resp.json()
        assert data["status"] == "completed"
        assert data["exit_code"] == 0
        assert data["task_id"] == "t1"

    runs = db.fetch_all("SELECT * FROM task_runs")
    assert len(runs) == 1
    assert runs[0]["status"] == "completed"


async def test_trigger_run_failure() -> None:
    module, db = await _setup_module(executor=_failing_executor)
    _seed_task(db, task_id="t1", module_name="hello_world")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks/t1/run")
        assert resp.status == 201
        data = await resp.json()
        assert data["status"] == "failed"
        assert data["exit_code"] == 1
        assert data["error_message"] == "module crashed"


async def test_trigger_run_not_found() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks/nonexistent/run")
        assert resp.status == 404


async def test_trigger_disabled_task() -> None:
    module, db = await _setup_module()
    _seed_task(db, task_id="t1", enabled=False)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/tasks/t1/run")
        assert resp.status == 400


# ── Run listing tests ───────────────────────────────────────────────────────


async def test_list_runs() -> None:
    module, db = await _setup_module(executor=_stub_executor)
    _seed_task(db, task_id="t1")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        # Trigger two runs
        await client.post("/api/v1/tasks/t1/run")
        await client.post("/api/v1/tasks/t1/run")

        resp = await client.get("/api/v1/tasks/t1/runs")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2


async def test_list_runs_task_not_found() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/tasks/nonexistent/runs")
        assert resp.status == 404


async def test_get_run() -> None:
    module, db = await _setup_module(executor=_stub_executor)
    _seed_task(db, task_id="t1")
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        trigger_resp = await client.post("/api/v1/tasks/t1/run")
        run_data = await trigger_resp.json()
        run_id = run_data["run_id"]

        resp = await client.get(f"/api/v1/runs/{run_id}")
        assert resp.status == 200
        data = await resp.json()
        assert data["run_id"] == run_id
        assert data["status"] == "completed"


async def test_get_run_not_found() -> None:
    module, db = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/runs/nonexistent")
        assert resp.status == 404


# ── Arg generator tests ─────────────────────────────────────────────────────


async def test_arg_generator_called() -> None:
    captured_args: list[dict] = []

    def capturing_executor(module_name: str, args: dict) -> tuple[int, str | None]:
        captured_args.append(args)
        return 0, None

    module, db = await _setup_module(executor=capturing_executor)
    _seed_task(
        db,
        task_id="batch_t1",
        module_name="batch_algos",
        default_args=json.dumps({"tenant_id": "acme"}),
        arg_generator="de_platform.pipeline.task_arg_generators.batch_algos_args",
    )

    await module._trigger_task_run("batch_t1")

    assert len(captured_args) == 1
    assert captured_args[0]["tenant-id"] == "acme"
    assert captured_args[0]["start-date"] == "2026-01-01"
    assert captured_args[0]["end-date"] == "2026-01-01"


async def test_arg_generator_uses_last_run() -> None:
    captured_args: list[dict] = []

    def capturing_executor(module_name: str, args: dict) -> tuple[int, str | None]:
        captured_args.append(args)
        return 0, None

    module, db = await _setup_module(executor=capturing_executor)
    _seed_task(
        db,
        task_id="batch_t1",
        module_name="batch_algos",
        default_args=json.dumps({"tenant_id": "acme"}),
        arg_generator="de_platform.pipeline.task_arg_generators.batch_algos_args",
    )

    # First run — starts from 2026-01-01
    await module._trigger_task_run("batch_t1")

    # Second run — should increment from last end_date
    await module._trigger_task_run("batch_t1")

    assert len(captured_args) == 2
    assert captured_args[1]["start-date"] == "2026-01-02"
    assert captured_args[1]["end-date"] == "2026-01-02"


# ── batch_algos_args unit tests ─────────────────────────────────────────────


def test_batch_algos_args_first_run() -> None:
    task_def = {
        "default_args": {"tenant_id": "acme"},
    }
    result = batch_algos_args(task_def, None)
    assert result["tenant-id"] == "acme"
    assert result["start-date"] == "2026-01-01"
    assert result["end-date"] == "2026-01-01"


def test_batch_algos_args_subsequent_run() -> None:
    task_def = {
        "default_args": {"tenant_id": "acme"},
    }
    last_run = {
        "args": {"end_date": "2026-01-15"},
    }
    result = batch_algos_args(task_def, last_run)
    assert result["start-date"] == "2026-01-16"
    assert result["end-date"] == "2026-01-16"


def test_batch_algos_args_json_string_args() -> None:
    task_def = {
        "default_args": json.dumps({"tenant_id": "acme"}),
    }
    last_run = {
        "args": json.dumps({"end_date": "2026-03-01"}),
    }
    result = batch_algos_args(task_def, last_run)
    assert result["tenant-id"] == "acme"
    assert result["start-date"] == "2026-03-02"
