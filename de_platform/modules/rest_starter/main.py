"""REST API Starter module.

Accepts batches of orders, executions, and transactions via HTTP POST.
Valid events are published to the appropriate Kafka normalization topic.
Invalid events are published to the normalization_errors topic and returned
in the HTTP response.

Endpoints::

    POST /api/v1/orders
    POST /api/v1/executions
    POST /api/v1/transactions
    GET  /api/v1/health

Request body (all endpoints)::

    {"events": [ {...}, ... ]}

Response::

    {"accepted": N, "rejected": N, "errors": [...]}
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.serialization import dto_to_message, error_to_dict
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.pipeline.validation import group_errors_by_event, validate_events
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface

# Maps endpoint event_type → normalization topic
_TOPIC_MAP: dict[str, str] = {
    "order": TRADE_NORMALIZATION,
    "execution": TRADE_NORMALIZATION,
    "transaction": TX_NORMALIZATION,
}


class RestStarterModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        lifecycle: LifecycleManager,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.lifecycle = lifecycle
        self._runner: web.AppRunner | None = None

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8001)

    async def execute(self) -> int:
        app = web.Application()
        app.router.add_post("/api/v1/orders", self._ingest_orders)
        app.router.add_post("/api/v1/executions", self._ingest_executions)
        app.router.add_post("/api/v1/transactions", self._ingest_transactions)
        app.router.add_get("/api/v1/health", self._health)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()

        self.log.info("REST Starter listening", port=self.port)
        self.lifecycle.on_shutdown(self._stop_server)

        while not self.lifecycle.is_shutting_down:
            await asyncio.sleep(0.1)

        return 0

    async def teardown(self) -> None:
        await self._stop_server()

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    # ------------------------------------------------------------------
    # Request handlers
    # ------------------------------------------------------------------

    async def _ingest_orders(self, request: web.Request) -> web.Response:
        return await self._handle_ingest(request, "order")

    async def _ingest_executions(self, request: web.Request) -> web.Response:
        return await self._handle_ingest(request, "execution")

    async def _ingest_transactions(self, request: web.Request) -> web.Response:
        return await self._handle_ingest(request, "transaction")

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _handle_ingest(self, request: web.Request, event_type: str) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON body"}, status=400)

        events: list[dict[str, Any]] = body.get("events", [])
        if not isinstance(events, list):
            return web.json_response({"error": "'events' must be a list"}, status=400)

        valid, errors = validate_events(event_type, events)

        # Publish valid events
        topic = _TOPIC_MAP[event_type]
        for raw in valid:
            msg = dto_to_message_from_raw(raw, event_type)
            self.mq.publish(topic, msg)

        # Publish one consolidated error message per invalid event
        for event_index, event_errors in group_errors_by_event(errors).items():
            raw_event = events[event_index] if event_index < len(events) else {}
            err_msg = error_to_dict(raw_event, event_type, event_errors)
            self.mq.publish(NORMALIZATION_ERRORS, err_msg)

        # Deduplicate error indices for response
        rejected_indices = {e.event_index for e in errors}

        self.log.info(
            "Ingested events",
            event_type=event_type,
            accepted=len(valid),
            rejected=len(rejected_indices),
        )

        return web.json_response({
            "accepted": len(valid),
            "rejected": len(rejected_indices),
            "errors": [
                {"event_index": e.event_index, "field": e.field, "message": e.message}
                for e in errors
            ],
        })


def dto_to_message_from_raw(raw: dict[str, Any], event_type: str) -> dict[str, Any]:
    """Attach message_id and ingested_at to a raw (already-validated) event dict."""
    from de_platform.pipeline.serialization import _now_iso  # noqa: PLC0415

    msg = dict(raw)
    msg["message_id"] = uuid.uuid4().hex
    msg["ingested_at"] = _now_iso()
    msg["event_type"] = event_type
    return msg


module_class = RestStarterModule
