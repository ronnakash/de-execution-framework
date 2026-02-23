"""Task Scheduler Service.

Centralized task scheduling and tracking.  Loads task definitions from
Postgres, registers cron schedules with APScheduler, executes modules as
subprocesses, and tracks run history.

REST API::

    GET    /api/v1/tasks                  — list task definitions
    GET    /api/v1/tasks/{task_id}        — get task definition
    POST   /api/v1/tasks                  — create task definition
    PUT    /api/v1/tasks/{task_id}        — update task definition
    DELETE /api/v1/tasks/{task_id}        — delete task definition
    POST   /api/v1/tasks/{task_id}/run    — trigger manual run
    GET    /api/v1/tasks/{task_id}/runs   — list runs for a task
    GET    /api/v1/runs/{run_id}          — get run details
"""

from __future__ import annotations

import asyncio
import importlib
import json
import subprocess
import sys
import uuid
from datetime import date, datetime
from typing import Any

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.metrics.interface import MetricsInterface
from de_platform.services.secrets.interface import SecretsInterface


def _json_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _dumps(obj: object) -> str:
    return json.dumps(obj, default=_json_default)


def _parse_json_field(value: Any) -> Any:
    """Parse a JSON string field into a dict/list, or return as-is."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


class TaskSchedulerModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        secrets: SecretsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.secrets = secrets
        self._runner: web.AppRunner | None = None
        self._scheduler: AsyncIOScheduler | None = None
        # Subprocess executor for running tasks — allows test injection
        self._execute_task_fn = _execute_subprocess

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8006)
        await self.db.connect_async()

        # LIFO shutdown: stop scheduler → stop server → disconnect DB
        self.lifecycle.on_shutdown(self.db.disconnect_async)
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self._stop_scheduler)

        self.log.info("Task Scheduler initialized", port=self.port)

    async def execute(self) -> int:
        # Start APScheduler
        self._scheduler = AsyncIOScheduler()
        self._scheduler.start()
        await self._load_and_schedule_tasks()

        # Start HTTP server
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Task Scheduler listening", module="task_scheduler", port=self.port)

        while not self.lifecycle.is_shutting_down:
            await asyncio.sleep(0.1)

        return 0

    # ── Scheduler ─────────────────────────────────────────────────────────

    async def _load_and_schedule_tasks(self) -> None:
        """Load all enabled task definitions and register cron jobs."""
        rows = await self.db.fetch_all_async("SELECT * FROM task_definitions")
        for task_def in rows:
            if task_def.get("enabled") and task_def.get("schedule_cron"):
                self._register_cron_job(task_def)

    def _register_cron_job(self, task_def: dict) -> None:
        """Register a single task definition with APScheduler."""
        if self._scheduler is None:
            return
        task_id = task_def["task_id"]
        cron_expr = task_def["schedule_cron"]

        # Remove existing job if any (for re-registration on update)
        try:
            self._scheduler.remove_job(task_id)
        except Exception:
            pass

        try:
            trigger = CronTrigger.from_crontab(cron_expr)
            self._scheduler.add_job(
                self._run_scheduled_task,
                trigger=trigger,
                args=[task_id],
                id=task_id,
                replace_existing=True,
            )
            self.log.info("Cron job registered", task_id=task_id, cron=cron_expr)
        except ValueError as exc:
            self.log.error("Invalid cron expression", task_id=task_id,
                           cron=cron_expr, error=str(exc))

    def _unregister_cron_job(self, task_id: str) -> None:
        """Remove a cron job from APScheduler."""
        if self._scheduler is None:
            return
        try:
            self._scheduler.remove_job(task_id)
        except Exception:
            pass

    async def _run_scheduled_task(self, task_id: str) -> None:
        """Callback invoked by APScheduler when a cron job fires."""
        await self._trigger_task_run(task_id)

    async def _trigger_task_run(self, task_id: str) -> dict | None:
        """Create a task run, resolve args, execute the module, update status."""
        task_def = await self._find_task(task_id)
        if task_def is None:
            self.log.error("Task not found for run", task_id=task_id)
            return None

        if not task_def.get("enabled", True):
            self.log.info("Task disabled, skipping", task_id=task_id)
            return None

        # Resolve args
        args = await self._resolve_args(task_def)

        # Create run record
        run_id = uuid.uuid4().hex
        now = datetime.utcnow()
        run = {
            "run_id": run_id,
            "task_id": task_id,
            "status": "running",
            "args": json.dumps(args) if isinstance(args, dict) else args,
            "started_at": now,
            "completed_at": None,
            "exit_code": None,
            "error_message": None,
            "created_at": now,
        }
        await self.db.insert_one_async("task_runs", run)
        self.log.info("Task run started", task_id=task_id, run_id=run_id)

        self.metrics.counter("task_runs_started_total", tags={
            "service": "task_scheduler", "task_id": task_id,
        })

        # Execute
        module_name = task_def["module_name"]
        exit_code, error_message = await self._execute_module(module_name, args)

        # Update run record
        completed_at = datetime.utcnow()
        status = "completed" if exit_code == 0 else "failed"

        updated_run = dict(run)
        updated_run["status"] = status
        updated_run["completed_at"] = completed_at
        updated_run["exit_code"] = exit_code
        updated_run["error_message"] = error_message

        await self.db.execute_async(
            "DELETE FROM task_runs WHERE run_id = $1", [run_id],
        )
        await self.db.insert_one_async("task_runs", updated_run)

        self.metrics.counter("task_runs_completed_total", tags={
            "service": "task_scheduler", "task_id": task_id, "status": status,
        })
        self.log.info("Task run completed", task_id=task_id, run_id=run_id,
                       status=status, exit_code=exit_code)
        return updated_run

    async def _resolve_args(self, task_def: dict) -> dict:
        """Resolve arguments for a task run.

        If an arg_generator is configured, import and call it. Otherwise
        use default_args from the task definition.
        """
        default_args = _parse_json_field(task_def.get("default_args", {}))
        if not isinstance(default_args, dict):
            default_args = {}

        arg_generator = task_def.get("arg_generator")
        if not arg_generator:
            return default_args

        # Find the last completed run for this task
        last_run = await self._find_last_run(task_def["task_id"])

        # Import and call the generator
        try:
            module_path, func_name = arg_generator.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            gen_func = getattr(mod, func_name)

            # Prepare task_def with parsed JSON fields
            prepared = dict(task_def)
            prepared["default_args"] = default_args
            if last_run:
                prepared_run = dict(last_run)
                prepared_run["args"] = _parse_json_field(prepared_run.get("args", {}))
            else:
                prepared_run = None

            result = gen_func(prepared, prepared_run)
            return result if isinstance(result, dict) else default_args
        except Exception as exc:
            self.log.error("Arg generator failed", generator=arg_generator, error=str(exc))
            return default_args

    async def _execute_module(
        self, module_name: str, args: dict,
    ) -> tuple[int, str | None]:
        """Execute a module as a subprocess and wait for completion."""
        try:
            exit_code, error = await asyncio.get_event_loop().run_in_executor(
                None, self._execute_task_fn, module_name, args,
            )
            return exit_code, error
        except Exception as exc:
            return 1, str(exc)

    async def _find_last_run(self, task_id: str) -> dict | None:
        """Find the most recent completed run for a task."""
        rows = await self.db.fetch_all_async("SELECT * FROM task_runs")
        task_runs = [
            r for r in rows
            if r.get("task_id") == task_id and r.get("status") == "completed"
        ]
        if not task_runs:
            return None
        # Sort by completed_at descending
        task_runs.sort(
            key=lambda r: r.get("completed_at") or datetime.min,
            reverse=True,
        )
        return task_runs[0]

    # ── REST API ──────────────────────────────────────────────────────────

    def _create_app(self) -> web.Application:
        middlewares: list = []
        jwt_secret = self.secrets.get("JWT_SECRET")
        if jwt_secret:
            from de_platform.pipeline.auth_middleware import create_auth_middleware
            middlewares.append(create_auth_middleware(jwt_secret))

        app = web.Application(middlewares=middlewares)
        app.router.add_get("/api/v1/tasks", self._list_tasks)
        app.router.add_get("/api/v1/tasks/{task_id}", self._get_task)
        app.router.add_post("/api/v1/tasks", self._create_task)
        app.router.add_put("/api/v1/tasks/{task_id}", self._update_task)
        app.router.add_delete("/api/v1/tasks/{task_id}", self._delete_task)
        app.router.add_post("/api/v1/tasks/{task_id}/run", self._trigger_run)
        app.router.add_get("/api/v1/tasks/{task_id}/runs", self._list_runs)
        app.router.add_get("/api/v1/runs/{run_id}", self._get_run)
        app.router.add_post("/api/v1/query/tasks", self._query_tasks)
        app.router.add_post("/api/v1/query/runs", self._query_runs)
        return app

    async def _list_tasks(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "task_scheduler", "endpoint": "/api/v1/tasks", "method": "GET",
        })
        rows = await self.db.fetch_all_async("SELECT * FROM task_definitions")
        for row in rows:
            row["default_args"] = _parse_json_field(row.get("default_args", {}))
        return web.json_response(dumps=_dumps, data=rows)

    async def _get_task(self, request: web.Request) -> web.Response:
        task_id = request.match_info["task_id"]
        task = await self._find_task(task_id)
        if task is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "task not found"}),
                content_type="application/json",
            )
        task["default_args"] = _parse_json_field(task.get("default_args", {}))
        return web.json_response(dumps=_dumps, data=task)

    async def _create_task(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "task_scheduler", "endpoint": "/api/v1/tasks", "method": "POST",
        })
        body = await request.json()

        task_id = body.get("task_id")
        name = body.get("name")
        module_name = body.get("module_name")
        if not task_id or not name or not module_name:
            raise web.HTTPBadRequest(
                text=json.dumps({"error": "task_id, name, and module_name are required"}),
                content_type="application/json",
            )

        existing = await self._find_task(task_id)
        if existing is not None:
            raise web.HTTPConflict(
                text=json.dumps({"error": "task already exists"}),
                content_type="application/json",
            )

        default_args = body.get("default_args", {})
        row = {
            "task_id": task_id,
            "name": name,
            "module_name": module_name,
            "default_args": (
                json.dumps(default_args) if isinstance(default_args, dict) else default_args
            ),
            "schedule_cron": body.get("schedule_cron"),
            "enabled": body.get("enabled", True),
            "arg_generator": body.get("arg_generator"),
            "created_at": datetime.utcnow(),
        }
        await self.db.insert_one_async("task_definitions", row)

        # Register cron if applicable
        if row["enabled"] and row["schedule_cron"]:
            self._register_cron_job(row)

        self.log.info("Task created", task_id=task_id)
        row["default_args"] = default_args
        return web.json_response(dumps=_dumps, data=row, status=201)

    async def _update_task(self, request: web.Request) -> web.Response:
        task_id = request.match_info["task_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "task_scheduler", "endpoint": f"/api/v1/tasks/{task_id}", "method": "PUT",
        })
        existing = await self._find_task(task_id)
        if existing is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "task not found"}),
                content_type="application/json",
            )

        body = await request.json()
        if "name" in body:
            existing["name"] = body["name"]
        if "module_name" in body:
            existing["module_name"] = body["module_name"]
        if "default_args" in body:
            da = body["default_args"]
            existing["default_args"] = json.dumps(da) if isinstance(da, dict) else da
        if "schedule_cron" in body:
            existing["schedule_cron"] = body["schedule_cron"]
        if "enabled" in body:
            existing["enabled"] = body["enabled"]
        if "arg_generator" in body:
            existing["arg_generator"] = body["arg_generator"]

        await self.db.execute_async(
            "DELETE FROM task_definitions WHERE task_id = $1", [task_id],
        )
        await self.db.insert_one_async("task_definitions", existing)

        # Re-register or remove cron
        if existing.get("enabled") and existing.get("schedule_cron"):
            self._register_cron_job(existing)
        else:
            self._unregister_cron_job(task_id)

        self.log.info("Task updated", task_id=task_id)
        existing["default_args"] = _parse_json_field(existing.get("default_args", {}))
        return web.json_response(dumps=_dumps, data=existing)

    async def _delete_task(self, request: web.Request) -> web.Response:
        task_id = request.match_info["task_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "task_scheduler", "endpoint": f"/api/v1/tasks/{task_id}", "method": "DELETE",
        })
        existing = await self._find_task(task_id)
        if existing is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "task not found"}),
                content_type="application/json",
            )

        # Delete runs first (foreign key)
        runs = await self.db.fetch_all_async("SELECT * FROM task_runs")
        for run in runs:
            if run.get("task_id") == task_id:
                await self.db.execute_async(
                    "DELETE FROM task_runs WHERE run_id = $1", [run["run_id"]],
                )

        await self.db.execute_async(
            "DELETE FROM task_definitions WHERE task_id = $1", [task_id],
        )
        self._unregister_cron_job(task_id)

        self.log.info("Task deleted", task_id=task_id)
        return web.json_response(dumps=_dumps, data={"deleted": task_id})

    async def _trigger_run(self, request: web.Request) -> web.Response:
        task_id = request.match_info["task_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "task_scheduler",
            "endpoint": f"/api/v1/tasks/{task_id}/run", "method": "POST",
        })
        task = await self._find_task(task_id)
        if task is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "task not found"}),
                content_type="application/json",
            )

        run = await self._trigger_task_run(task_id)
        if run is None:
            raise web.HTTPBadRequest(
                text=json.dumps({"error": "task could not be triggered"}),
                content_type="application/json",
            )

        run["args"] = _parse_json_field(run.get("args", {}))
        return web.json_response(dumps=_dumps, data=run, status=201)

    async def _list_runs(self, request: web.Request) -> web.Response:
        task_id = request.match_info["task_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "task_scheduler",
            "endpoint": f"/api/v1/tasks/{task_id}/runs", "method": "GET",
        })
        task = await self._find_task(task_id)
        if task is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "task not found"}),
                content_type="application/json",
            )

        rows = await self.db.fetch_all_async("SELECT * FROM task_runs")
        runs = [r for r in rows if r.get("task_id") == task_id]
        for run in runs:
            run["args"] = _parse_json_field(run.get("args", {}))
        return web.json_response(dumps=_dumps, data=runs)

    async def _get_run(self, request: web.Request) -> web.Response:
        run_id = request.match_info["run_id"]
        rows = await self.db.fetch_all_async("SELECT * FROM task_runs")
        for row in rows:
            if row.get("run_id") == run_id:
                row["args"] = _parse_json_field(row.get("args", {}))
                return web.json_response(dumps=_dumps, data=row)
        raise web.HTTPNotFound(
            text=json.dumps({"error": "run not found"}),
            content_type="application/json",
        )

    # ── Query endpoints ────────────────────────────────────────────────

    async def _query_tasks(self, request: web.Request) -> web.Response:
        from de_platform.pipeline.query_framework import handle_query

        body = await request.json()
        rows = await self.db.fetch_all_async("SELECT * FROM task_definitions")
        for row in rows:
            row["default_args"] = _parse_json_field(row.get("default_args", {}))
        return web.json_response(dumps=_dumps, data=handle_query(rows, body))

    async def _query_runs(self, request: web.Request) -> web.Response:
        from de_platform.pipeline.query_framework import handle_query

        body = await request.json()
        rows = await self.db.fetch_all_async("SELECT * FROM task_runs")
        for row in rows:
            row["args"] = _parse_json_field(row.get("args", {}))
        return web.json_response(dumps=_dumps, data=handle_query(rows, body))

    # ── Helpers ────────────────────────────────────────────────────────────

    async def _find_task(self, task_id: str) -> dict | None:
        rows = await self.db.fetch_all_async("SELECT * FROM task_definitions")
        for row in rows:
            if row.get("task_id") == task_id:
                return row
        return None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    async def _stop_scheduler(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None


def _execute_subprocess(
    module_name: str, args: dict,
) -> tuple[int, str | None]:
    """Execute a module as a subprocess.

    Builds the CLI command and runs it, capturing exit code and stderr.
    """
    cmd = [sys.executable, "-m", "de_platform", "run", module_name]

    for key, value in args.items():
        if value is not None and value != "":
            cmd.extend([f"--{key}", str(value)])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour max
        )
        error_msg = result.stderr.strip() if result.returncode != 0 else None
        return result.returncode, error_msg
    except subprocess.TimeoutExpired:
        return 1, "Task timed out after 3600 seconds"
    except Exception as exc:
        return 1, str(exc)


module_class = TaskSchedulerModule
