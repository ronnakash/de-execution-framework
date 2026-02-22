"""Client Configuration Service.

REST API for managing per-tenant configuration: processing mode (realtime/batch),
algorithm settings (enabled/disabled, thresholds), and scheduling.

On every mutation the service writes the updated config to Redis cache keys
and publishes to the ``client_config_updates`` pub-sub channel so that
downstream services (normalizer, algos) see changes in real time.

Endpoints::

    GET    /api/v1/clients                           — list all clients
    GET    /api/v1/clients/{tenant_id}               — get client config
    POST   /api/v1/clients                           — create client
    PUT    /api/v1/clients/{tenant_id}               — update client config
    DELETE /api/v1/clients/{tenant_id}               — delete client

    GET    /api/v1/clients/{tenant_id}/algos         — get algo config
    PUT    /api/v1/clients/{tenant_id}/algos/{algo}  — upsert algo config
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime

from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.client_config_cache import CHANNEL
from de_platform.services.cache.interface import CacheInterface
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


class ClientConfigModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        cache: CacheInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        secrets: SecretsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.cache = cache
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.secrets = secrets
        self._runner: web.AppRunner | None = None

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8003)
        await self.db.connect_async()
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self.db.disconnect_async)
        self.log.info("Client Config API initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Client Config API listening", module="client_config", port=self.port)

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
        app.router.add_get("/api/v1/clients", self._list_clients)
        app.router.add_get("/api/v1/clients/{tenant_id}", self._get_client)
        app.router.add_post("/api/v1/clients", self._create_client)
        app.router.add_put("/api/v1/clients/{tenant_id}", self._update_client)
        app.router.add_delete("/api/v1/clients/{tenant_id}", self._delete_client)
        app.router.add_get("/api/v1/clients/{tenant_id}/algos", self._get_algos)
        app.router.add_put("/api/v1/clients/{tenant_id}/algos/{algo}", self._update_algo)
        return app

    # ── Auth helpers ──────────────────────────────────────────────────────

    def _check_tenant_access(self, request: web.Request, tenant_id: str) -> web.Response | None:
        """Return an error response if the user cannot access this tenant, else None."""
        jwt_tenant = request.get("tenant_id")
        if jwt_tenant and jwt_tenant != tenant_id and request.get("role") != "admin":
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Access denied to this tenant"}),
            )
        return None

    def _require_admin(self, request: web.Request) -> web.Response | None:
        """Return 403 if auth is active and user is not admin, else None."""
        # Only enforce when auth middleware has set a role
        if request.get("role") and request["role"] != "admin":
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Admin role required"}),
            )
        return None

    # ── Client endpoints ──────────────────────────────────────────────────

    async def _list_clients(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config", "endpoint": "/api/v1/clients", "method": "GET",
        })
        rows = await self.db.fetch_all_async("SELECT * FROM clients")
        return web.json_response(dumps=_dumps, data=rows)

    async def _get_client(self, request: web.Request) -> web.Response:
        tenant_id = request.match_info["tenant_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config", "endpoint": f"/api/v1/clients/{tenant_id}", "method": "GET",
        })
        denied = self._check_tenant_access(request, tenant_id)
        if denied:
            return denied
        row = await self._find_client(tenant_id)
        if row is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "client not found"}),
                content_type="application/json",
            )
        return web.json_response(dumps=_dumps, data=row)

    async def _create_client(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config", "endpoint": "/api/v1/clients", "method": "POST",
        })
        denied = self._require_admin(request)
        if denied:
            return denied
        body = await request.json()

        tenant_id = body.get("tenant_id")
        display_name = body.get("display_name")
        if not tenant_id or not display_name:
            raise web.HTTPBadRequest(
                text=json.dumps({"error": "tenant_id and display_name are required"}),
                content_type="application/json",
            )

        existing = await self._find_client(tenant_id)
        if existing is not None:
            raise web.HTTPConflict(
                text=json.dumps({"error": "client already exists"}),
                content_type="application/json",
            )

        row = {
            "tenant_id": tenant_id,
            "display_name": display_name,
            "mode": body.get("mode", "batch"),
            "algo_run_hour": body.get("algo_run_hour"),
        }
        await self.db.insert_one_async("clients", row)
        self._sync_client_to_cache(row)

        self.log.info("Client created", tenant_id=tenant_id)
        return web.json_response(dumps=_dumps, data=row, status=201)

    async def _update_client(self, request: web.Request) -> web.Response:
        tenant_id = request.match_info["tenant_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config", "endpoint": f"/api/v1/clients/{tenant_id}", "method": "PUT",
        })
        denied = self._require_admin(request)
        if denied:
            return denied
        denied = self._check_tenant_access(request, tenant_id)
        if denied:
            return denied
        existing = await self._find_client(tenant_id)
        if existing is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "client not found"}),
                content_type="application/json",
            )

        body = await request.json()
        if "display_name" in body:
            existing["display_name"] = body["display_name"]
        if "mode" in body:
            existing["mode"] = body["mode"]
        if "algo_run_hour" in body:
            existing["algo_run_hour"] = body["algo_run_hour"]

        # MemoryDatabase doesn't support UPDATE, so delete + re-insert
        await self.db.execute_async(
            "DELETE FROM clients WHERE tenant_id = $1", [tenant_id],
        )
        await self.db.insert_one_async("clients", existing)
        self._sync_client_to_cache(existing)

        self.log.info("Client updated", tenant_id=tenant_id)
        return web.json_response(dumps=_dumps, data=existing)

    async def _delete_client(self, request: web.Request) -> web.Response:
        tenant_id = request.match_info["tenant_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config", "endpoint": f"/api/v1/clients/{tenant_id}", "method": "DELETE",
        })
        denied = self._require_admin(request)
        if denied:
            return denied
        denied = self._check_tenant_access(request, tenant_id)
        if denied:
            return denied
        existing = await self._find_client(tenant_id)
        if existing is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "client not found"}),
                content_type="application/json",
            )

        # Delete algo configs first (foreign key), then client
        await self.db.execute_async(
            "DELETE FROM client_algo_config WHERE tenant_id = $1", [tenant_id],
        )
        await self.db.execute_async(
            "DELETE FROM clients WHERE tenant_id = $1", [tenant_id],
        )

        # Clear cache
        self.cache.delete(f"client_config:{tenant_id}")
        # Clear all algo config keys for this tenant
        algo_rows = await self._find_algo_configs(tenant_id)
        for algo_row in algo_rows:
            self.cache.delete(f"algo_config:{tenant_id}:{algo_row['algorithm']}")
        self.cache.publish_channel(CHANNEL, tenant_id)

        self.log.info("Client deleted", tenant_id=tenant_id)
        return web.json_response(dumps=_dumps, data={"deleted": tenant_id})

    # ── Algo config endpoints ─────────────────────────────────────────────

    async def _get_algos(self, request: web.Request) -> web.Response:
        tenant_id = request.match_info["tenant_id"]
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config", "endpoint": f"/api/v1/clients/{tenant_id}/algos", "method": "GET",
        })
        denied = self._check_tenant_access(request, tenant_id)
        if denied:
            return denied
        client = await self._find_client(tenant_id)
        if client is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "client not found"}),
                content_type="application/json",
            )
        rows = await self._find_algo_configs(tenant_id)
        return web.json_response(dumps=_dumps, data=rows)

    async def _update_algo(self, request: web.Request) -> web.Response:
        tenant_id = request.match_info["tenant_id"]
        algo = request.match_info["algo"]
        self.metrics.counter("http_requests_total", tags={
            "service": "client_config",
            "endpoint": f"/api/v1/clients/{tenant_id}/algos/{algo}",
            "method": "PUT",
        })
        denied = self._require_admin(request)
        if denied:
            return denied
        denied = self._check_tenant_access(request, tenant_id)
        if denied:
            return denied
        client = await self._find_client(tenant_id)
        if client is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "client not found"}),
                content_type="application/json",
            )

        body = await request.json()
        row = {
            "tenant_id": tenant_id,
            "algorithm": algo,
            "enabled": body.get("enabled", True),
            "thresholds": body.get("thresholds", {}),
        }

        # Read existing BEFORE delete so we can preserve other algos
        existing = await self._find_algo_configs(tenant_id)
        updated = {r["algorithm"]: r for r in existing}
        updated[algo] = row

        # Upsert: delete all + re-insert
        await self.db.execute_async(
            "DELETE FROM client_algo_config WHERE tenant_id = $1", [tenant_id],
        )
        for algo_row in updated.values():
            # Ensure thresholds is stored as JSON string for Postgres JSONB
            db_row = dict(algo_row)
            if isinstance(db_row.get("thresholds"), dict):
                db_row["thresholds"] = json.dumps(db_row["thresholds"])
            await self.db.insert_one_async("client_algo_config", db_row)

        self._sync_algo_to_cache(tenant_id, algo, row)

        self.log.info("Algo config updated", tenant_id=tenant_id, algorithm=algo)
        return web.json_response(dumps=_dumps, data=row)

    # ── Helpers ────────────────────────────────────────────────────────────

    async def _find_client(self, tenant_id: str) -> dict | None:
        rows = await self.db.fetch_all_async("SELECT * FROM clients")
        for row in rows:
            if row.get("tenant_id") == tenant_id:
                return row
        return None

    async def _find_algo_configs(self, tenant_id: str) -> list[dict]:
        rows = await self.db.fetch_all_async("SELECT * FROM client_algo_config")
        return [r for r in rows if r.get("tenant_id") == tenant_id]

    def _sync_client_to_cache(self, row: dict) -> None:
        """Write client config to cache and notify subscribers."""
        tenant_id = row["tenant_id"]
        self.cache.set(f"client_config:{tenant_id}", {
            "mode": row.get("mode", "batch"),
            "algo_run_hour": row.get("algo_run_hour"),
            "display_name": row.get("display_name", ""),
        })
        self.cache.publish_channel(CHANNEL, tenant_id)

    def _sync_algo_to_cache(self, tenant_id: str, algo: str, row: dict) -> None:
        """Write algo config to cache and notify subscribers."""
        thresholds = row.get("thresholds", {})
        if isinstance(thresholds, str):
            thresholds = json.loads(thresholds)
        self.cache.set(f"algo_config:{tenant_id}:{algo}", {
            "enabled": row.get("enabled", True),
            "thresholds": thresholds,
        })
        self.cache.publish_channel(CHANNEL, tenant_id)

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None


module_class = ClientConfigModule
