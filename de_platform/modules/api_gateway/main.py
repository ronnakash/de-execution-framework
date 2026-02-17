"""API Gateway module: REST API with cache-aside pattern and correlation ID propagation."""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import AsyncModule
from de_platform.services.cache.interface import CacheInterface
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface

CACHE_TTL = 60


class ApiGatewayModule(AsyncModule):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        cache: CacheInterface,
        lifecycle: LifecycleManager,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.cache = cache
        self.lifecycle = lifecycle
        self._runner: web.AppRunner | None = None
        self._start_time = time.monotonic()

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8000)
        self.db.connect()
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self.db.disconnect)
        self.log.info("API Gateway initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        self.log.info("API Gateway listening", port=self.port)

        while not self.lifecycle.is_shutting_down:
            await asyncio.sleep(0.5)

        return 0

    async def teardown(self) -> None:
        pass

    def _create_app(self) -> web.Application:
        app = web.Application(middlewares=[self._correlation_middleware, self._logging_middleware])
        app.router.add_get("/events", self._get_events)
        app.router.add_get("/events/{id}", self._get_event_by_id)
        app.router.add_get("/status", self._get_status)
        return app

    @web.middleware
    async def _correlation_middleware(self, request: web.Request, handler):
        correlation_id = request.headers.get("X-Correlation-ID", uuid.uuid4().hex)
        request["correlation_id"] = correlation_id
        response = await handler(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response

    @web.middleware
    async def _logging_middleware(self, request: web.Request, handler):
        start = time.monotonic()
        try:
            response = await handler(request)
            duration_ms = (time.monotonic() - start) * 1000
            self.log.info(
                "Request handled",
                method=request.method,
                path=request.path,
                status=response.status,
                duration_ms=round(duration_ms, 2),
                correlation_id=request.get("correlation_id", ""),
            )
            return response
        except web.HTTPException as exc:
            duration_ms = (time.monotonic() - start) * 1000
            self.log.info(
                "Request handled",
                method=request.method,
                path=request.path,
                status=exc.status,
                duration_ms=round(duration_ms, 2),
                correlation_id=request.get("correlation_id", ""),
            )
            raise

    async def _get_events(self, request: web.Request) -> web.Response:
        # Cache-aside: check cache first
        cached = self.cache.get("events:list")
        if cached is not None:
            return web.json_response(cached)

        rows = self.db.fetch_all("SELECT * FROM events")
        self.cache.set("events:list", rows, ttl=CACHE_TTL)
        return web.json_response(rows)

    async def _get_event_by_id(self, request: web.Request) -> web.Response:
        event_id = request.match_info["id"]
        cache_key = f"events:{event_id}"

        cached = self.cache.get(cache_key)
        if cached is not None:
            return web.json_response(cached)

        row = self.db.fetch_one("SELECT * FROM events WHERE id = $1", [event_id])
        if row is None:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "event not found"}),
                content_type="application/json",
            )

        self.cache.set(cache_key, row, ttl=CACHE_TTL)
        return web.json_response(row)

    async def _get_status(self, request: web.Request) -> web.Response:
        uptime = time.monotonic() - self._start_time
        return web.json_response({
            "module": "api_gateway",
            "version": "1.0.0",
            "uptime_seconds": round(uptime, 1),
            "db_connected": self.db.is_connected(),
        })

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None


module_class = ApiGatewayModule
