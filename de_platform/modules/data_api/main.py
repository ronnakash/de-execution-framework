"""Data API Service.

REST API serving query endpoints for events.  Alert queries are handled by
the ``alert_manager`` service.

Endpoints::

    GET /api/v1/events/orders?tenant_id=X&date=2026-01-15&limit=50
    GET /api/v1/events/executions?tenant_id=X&date=2026-01-15&limit=50
    GET /api/v1/events/transactions?tenant_id=X&date=2026-01-15&limit=50

Static UI::

    GET /ui/           → Events Explorer single-page app

Event endpoints query ClickHouse (same DatabaseInterface, different backing store
selected via CLI flag at startup).
"""

from __future__ import annotations

import asyncio
import json
import pathlib
from datetime import date, datetime

from aiohttp import web


def _json_default(obj: object) -> str:
    """JSON serializer for types not handled by the default encoder."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _dumps(obj: object) -> str:
    return json.dumps(obj, default=_json_default)

_STATIC_DIR = pathlib.Path(__file__).parent / "static"

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.metrics.interface import MetricsInterface
from de_platform.services.secrets.interface import SecretsInterface


class DataApiModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db_factory: DatabaseFactory,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        secrets: SecretsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db_factory = db_factory
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.secrets = secrets
        self._runner: web.AppRunner | None = None

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8002)
        self.events_db: DatabaseInterface = self.db_factory.get("events")
        await self.events_db.connect_async()
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self.events_db.disconnect_async)
        self.log.info("Data API initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Data API listening", module="data_api", port=self.port)

        while not self.lifecycle.is_shutting_down:
            await asyncio.sleep(0.1)

        return 0

    def _create_app(self) -> web.Application:
        middlewares: list = []
        jwt_secret = self.secrets.get("JWT_SECRET")
        if jwt_secret:
            from de_platform.pipeline.auth_middleware import create_auth_middleware

            middlewares.append(create_auth_middleware(jwt_secret))
        app = web.Application(middlewares=middlewares)
        app.router.add_get("/api/v1/events/orders", self._get_orders)
        app.router.add_get("/api/v1/events/executions", self._get_executions)
        app.router.add_get("/api/v1/events/transactions", self._get_transactions)
        # Serve static UI if the directory is present
        if _STATIC_DIR.exists():
            app.router.add_static("/ui", _STATIC_DIR)
        return app

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _resolve_tenant_id(self, request: web.Request) -> str | None:
        """Get tenant_id from JWT (if auth active) with admin override via query param."""
        jwt_tenant = request.get("tenant_id")
        query_tenant = request.rel_url.query.get("tenant_id")
        if jwt_tenant:
            # Admin users can query other tenants via ?tenant_id=
            if request.get("role") == "admin" and query_tenant:
                return query_tenant
            return jwt_tenant
        return query_tenant

    # ── Event endpoints ───────────────────────────────────────────────────────

    async def _get_orders(self, request: web.Request) -> web.Response:
        return await self._get_events_for_table(request, "orders")

    async def _get_executions(self, request: web.Request) -> web.Response:
        return await self._get_events_for_table(request, "executions")

    async def _get_transactions(self, request: web.Request) -> web.Response:
        return await self._get_events_for_table(request, "transactions")

    async def _get_events_for_table(
        self, request: web.Request, table: str
    ) -> web.Response:
        self.metrics.counter("http_requests_total", tags={"service": "data_api", "endpoint": f"/api/v1/events/{table}", "method": "GET"})
        tenant_id = self._resolve_tenant_id(request)
        date = request.rel_url.query.get("date")
        limit = int(request.rel_url.query.get("limit", 50))

        rows = await self.events_db.fetch_all_async(f"SELECT * FROM {table}")
        if tenant_id:
            rows = [r for r in rows if r.get("tenant_id") == tenant_id]
        if date:
            rows = [r for r in rows if r.get("transact_time", "").startswith(date)]
        result = rows[:limit]
        self.log.debug(
            "Events query served",
            endpoint=f"/api/v1/events/{table}",
            tenant_id=tenant_id or "",
            status_code=200,
            result_count=len(result),
        )
        return web.json_response(dumps=_dumps, data=result)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None


module_class = DataApiModule
