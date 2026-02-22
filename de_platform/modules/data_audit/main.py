"""Data Audit Service.

Tracks incoming data volumes, processed counts, errors, and duplicates
per tenant by consuming from existing pipeline Kafka topics with its own
consumer group.

Kafka consumer — reads from normalization, persistence, error, and
duplicate topics, accumulates in-memory counters, and periodically
flushes to the ``daily_audit`` PostgreSQL table.

REST API::

    GET /api/v1/audit/daily          — daily audit records (filterable)
    GET /api/v1/audit/summary        — aggregate summary per tenant
    GET /api/v1/audit/files          — list file audit records
    GET /api/v1/audit/files/{file_id} — get single file audit record
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import date, datetime
from typing import Any

from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.topics import (
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TRADE_NORMALIZATION,
    TRANSACTIONS_PERSISTENCE,
    TX_NORMALIZATION,
)
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface
from de_platform.services.secrets.interface import SecretsInterface

# Counter field names
_RECEIVED = "received_count"
_PROCESSED = "processed_count"
_ERROR = "error_count"
_DUPLICATE = "duplicate_count"

# Topic → (counter field, default event_type or None to read from message)
_TOPIC_MAPPING: list[tuple[str, str, str | None]] = [
    (TRADE_NORMALIZATION, _RECEIVED, None),          # event_type from msg
    (TX_NORMALIZATION, _RECEIVED, "transaction"),
    (ORDERS_PERSISTENCE, _PROCESSED, "order"),
    (EXECUTIONS_PERSISTENCE, _PROCESSED, "execution"),
    (TRANSACTIONS_PERSISTENCE, _PROCESSED, "transaction"),
    (NORMALIZATION_ERRORS, _ERROR, None),            # event_type from msg
    (DUPLICATES, _DUPLICATE, None),                  # event_type from msg
]


def _json_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _dumps(obj: object) -> str:
    return json.dumps(obj, default=_json_default)


def _extract_date(msg: dict) -> str:
    """Extract date string (YYYY-MM-DD) from message transact_time or today."""
    tt = msg.get("transact_time")
    if tt:
        try:
            if isinstance(tt, str):
                dt = datetime.fromisoformat(tt)
            elif isinstance(tt, datetime):
                dt = tt
            else:
                return date.today().isoformat()
            return dt.date().isoformat()
        except (ValueError, TypeError):
            pass
    return date.today().isoformat()


class DataAuditModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        db: DatabaseInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        secrets: SecretsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.db = db
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.secrets = secrets
        self._runner: web.AppRunner | None = None

        # In-memory counters: (tenant_id, date_str, event_type) → {field: count}
        self._counters: dict[tuple[str, str, str], dict[str, int]] = {}
        self._msg_count_since_flush = 0
        self._last_flush_time = 0.0

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8005)
        self._flush_interval = int(self.config.get("flush-interval", 5))
        self._flush_threshold = int(self.config.get("flush-threshold", 100))

        await self.db.connect_async()

        # LIFO shutdown order: flush counters → stop server → disconnect DB
        self.lifecycle.on_shutdown(self.db.disconnect_async)
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self._flush_counters)

        self._last_flush_time = time.monotonic()
        self.log.info("Data Audit initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Data Audit listening", module="data_audit", port=self.port)

        while not self.lifecycle.is_shutting_down:
            try:
                self._consume_all_topics()
                await self._maybe_flush()
            except Exception as exc:
                self.log.error("Processing error", module="data_audit", error=str(exc))
            await asyncio.sleep(0.01)

        return 0

    # ── Kafka consumer ────────────────────────────────────────────────────

    def _consume_all_topics(self) -> None:
        """Poll one message from each topic per iteration."""
        for topic, counter_field, default_event_type in _TOPIC_MAPPING:
            msg = self.mq.consume_one(topic)
            if msg:
                self._count_message(msg, counter_field, default_event_type)

    def _count_message(
        self, msg: dict, counter_field: str, default_event_type: str | None,
    ) -> None:
        """Increment in-memory counter for a consumed message."""
        tenant_id = msg.get("tenant_id", "unknown")
        event_type = default_event_type or msg.get("event_type", "unknown")
        date_str = _extract_date(msg)

        key = (tenant_id, date_str, event_type)
        if key not in self._counters:
            self._counters[key] = {
                _RECEIVED: 0, _PROCESSED: 0, _ERROR: 0, _DUPLICATE: 0,
            }
        self._counters[key][counter_field] += 1
        self._msg_count_since_flush += 1

        self.metrics.counter("audit_events_counted_total", tags={
            "service": "data_audit", "tenant_id": tenant_id,
            "event_type": event_type, "counter": counter_field,
        })

    # ── Flush logic ───────────────────────────────────────────────────────

    async def _maybe_flush(self) -> None:
        """Flush if threshold or interval exceeded."""
        elapsed = time.monotonic() - self._last_flush_time
        if (
            self._msg_count_since_flush >= self._flush_threshold
            or (self._counters and elapsed >= self._flush_interval)
        ):
            await self._flush_counters()

    async def _flush_counters(self) -> None:
        """Upsert accumulated counters into daily_audit table."""
        if not self._counters:
            return

        # Snapshot and reset
        snapshot = dict(self._counters)
        self._counters = {}
        self._msg_count_since_flush = 0
        self._last_flush_time = time.monotonic()

        for (tenant_id, date_str, event_type), counts in snapshot.items():
            # Fetch existing row (MemoryDB compatible: fetch_all + filter)
            rows = await self.db.fetch_all_async("SELECT * FROM daily_audit")
            existing = None
            for row in rows:
                row_date = row.get("date")
                if isinstance(row_date, date):
                    row_date = row_date.isoformat()
                if (
                    row.get("tenant_id") == tenant_id
                    and row_date == date_str
                    and row.get("event_type") == event_type
                ):
                    existing = row
                    break

            if existing:
                # Update: delete + re-insert (MemoryDB compatible)
                merged = dict(existing)
                merged[_RECEIVED] = merged.get(_RECEIVED, 0) + counts[_RECEIVED]
                merged[_PROCESSED] = merged.get(_PROCESSED, 0) + counts[_PROCESSED]
                merged[_ERROR] = merged.get(_ERROR, 0) + counts[_ERROR]
                merged[_DUPLICATE] = merged.get(_DUPLICATE, 0) + counts[_DUPLICATE]
                merged["updated_at"] = datetime.utcnow()
                row_id = existing.get("id")
                await self.db.execute_async(
                    "DELETE FROM daily_audit WHERE id = $1", [row_id],
                )
                await self.db.insert_one_async("daily_audit", merged)
            else:
                await self.db.insert_one_async("daily_audit", {
                    "tenant_id": tenant_id,
                    "date": date_str,
                    "event_type": event_type,
                    _RECEIVED: counts[_RECEIVED],
                    _PROCESSED: counts[_PROCESSED],
                    _ERROR: counts[_ERROR],
                    _DUPLICATE: counts[_DUPLICATE],
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                })

        self.log.info("Counters flushed", module="data_audit", keys=len(snapshot))

    # ── REST API ──────────────────────────────────────────────────────────

    def _create_app(self) -> web.Application:
        middlewares: list = []
        jwt_secret = self.secrets.get("JWT_SECRET")
        if jwt_secret:
            from de_platform.pipeline.auth_middleware import create_auth_middleware
            middlewares.append(create_auth_middleware(jwt_secret))

        app = web.Application(middlewares=middlewares)
        app.router.add_get("/api/v1/audit/daily", self._get_daily)
        app.router.add_get("/api/v1/audit/summary", self._get_summary)
        app.router.add_get("/api/v1/audit/files", self._list_files)
        app.router.add_get("/api/v1/audit/files/{file_id}", self._get_file)
        return app

    async def _get_daily(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "data_audit", "endpoint": "/api/v1/audit/daily", "method": "GET",
        })
        tenant_id = request.rel_url.query.get("tenant_id")
        date_param = request.rel_url.query.get("date")
        start_date = request.rel_url.query.get("start_date")
        end_date = request.rel_url.query.get("end_date")

        rows = await self.db.fetch_all_async("SELECT * FROM daily_audit")

        if tenant_id:
            rows = [r for r in rows if r.get("tenant_id") == tenant_id]

        if date_param:
            rows = [r for r in rows if _date_matches(r.get("date"), date_param)]

        if start_date:
            rows = [r for r in rows if _date_gte(r.get("date"), start_date)]

        if end_date:
            rows = [r for r in rows if _date_lte(r.get("date"), end_date)]

        return web.json_response(dumps=_dumps, data=rows)

    async def _get_summary(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "data_audit", "endpoint": "/api/v1/audit/summary", "method": "GET",
        })
        tenant_id = request.rel_url.query.get("tenant_id")

        rows = await self.db.fetch_all_async("SELECT * FROM daily_audit")
        if tenant_id:
            rows = [r for r in rows if r.get("tenant_id") == tenant_id]

        summary: dict[str, Any] = {
            "total_received": 0,
            "total_processed": 0,
            "total_errors": 0,
            "total_duplicates": 0,
            "by_event_type": {},
        }

        for row in rows:
            summary["total_received"] += row.get(_RECEIVED, 0)
            summary["total_processed"] += row.get(_PROCESSED, 0)
            summary["total_errors"] += row.get(_ERROR, 0)
            summary["total_duplicates"] += row.get(_DUPLICATE, 0)

            et = row.get("event_type", "unknown")
            if et not in summary["by_event_type"]:
                summary["by_event_type"][et] = {
                    "received": 0, "processed": 0, "errors": 0, "duplicates": 0,
                }
            summary["by_event_type"][et]["received"] += row.get(_RECEIVED, 0)
            summary["by_event_type"][et]["processed"] += row.get(_PROCESSED, 0)
            summary["by_event_type"][et]["errors"] += row.get(_ERROR, 0)
            summary["by_event_type"][et]["duplicates"] += row.get(_DUPLICATE, 0)

        return web.json_response(dumps=_dumps, data=summary)

    async def _list_files(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "data_audit", "endpoint": "/api/v1/audit/files", "method": "GET",
        })
        tenant_id = request.rel_url.query.get("tenant_id")

        rows = await self.db.fetch_all_async("SELECT * FROM file_audit")
        if tenant_id:
            rows = [r for r in rows if r.get("tenant_id") == tenant_id]
        return web.json_response(dumps=_dumps, data=rows)

    async def _get_file(self, request: web.Request) -> web.Response:
        file_id = request.match_info["file_id"]
        rows = await self.db.fetch_all_async("SELECT * FROM file_audit")
        for row in rows:
            if row.get("file_id") == file_id:
                return web.json_response(dumps=_dumps, data=row)
        raise web.HTTPNotFound(
            text=json.dumps({"error": "file not found"}),
            content_type="application/json",
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None


def _date_matches(row_date: Any, target: str) -> bool:
    """Check if a row date matches a target date string."""
    if isinstance(row_date, date):
        return row_date.isoformat() == target
    return str(row_date) == target


def _date_gte(row_date: Any, target: str) -> bool:
    if isinstance(row_date, date):
        return row_date.isoformat() >= target
    return str(row_date) >= target


def _date_lte(row_date: Any, target: str) -> bool:
    if isinstance(row_date, date):
        return row_date.isoformat() <= target
    return str(row_date) <= target


module_class = DataAuditModule
