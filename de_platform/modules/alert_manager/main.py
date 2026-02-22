"""Alert Management Service.

Single owner of the alert lifecycle: deduplication, persistence, case
aggregation, and query API.

Kafka consumer — reads from the ``alerts`` topic, deduplicates by
``(algorithm, event_id)``, persists to the ``alerts`` PostgreSQL table,
and aggregates alerts into cases.

REST API::

    GET    /api/v1/alerts                       — list alerts (tenant-scoped)
    GET    /api/v1/alerts/{alert_id}            — get alert detail
    GET    /api/v1/alerts/{alert_id}/case       — get the case for this alert

    GET    /api/v1/cases                        — list cases (tenant-scoped)
    GET    /api/v1/cases/{case_id}              — get case detail + its alerts
    PUT    /api/v1/cases/{case_id}/status       — update case status
    GET    /api/v1/cases/summary                — aggregate stats
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import date, datetime, timedelta
from typing import Any

from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.topics import ALERTS
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface
from de_platform.services.secrets.interface import SecretsInterface

_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}
_DEFAULT_AGGREGATION_MINUTES = 60


def _json_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _dumps(obj: object) -> str:
    return json.dumps(obj, default=_json_default)


class AlertManagerModule(Module):
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
        self._known_alerts: set[tuple[str, str]] = set()

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8007)
        await self.db.connect_async()
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self.db.disconnect_async)

        await self._load_dedup_set()
        self.log.info("Alert Manager initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Alert Manager listening", module="alert_manager", port=self.port)

        while not self.lifecycle.is_shutting_down:
            try:
                await self._consume_and_process()
            except Exception as exc:
                self.log.error("Processing error", module="alert_manager", error=str(exc))
            await asyncio.sleep(0.01)

        return 0

    # ── Kafka consumer ────────────────────────────────────────────────────

    async def _consume_and_process(self) -> None:
        msg = self.mq.consume_one(ALERTS)
        if not msg:
            return

        alert_id = msg.get("alert_id", "")
        algorithm = msg.get("algorithm", "")
        event_id = msg.get("event_id", "")
        tenant_id = msg.get("tenant_id", "")
        dedup_key = (algorithm, event_id)

        if dedup_key in self._known_alerts:
            self.log.debug("Alert deduplicated", alert_id=alert_id,
                           algorithm=algorithm, event_id=event_id)
            self.metrics.counter("alerts_deduplicated_total", tags={
                "service": "alert_manager", "tenant_id": tenant_id,
            })
            return

        db_row = self._prepare_db_row(msg)
        await self.db.insert_one_async("alerts", db_row)
        self._known_alerts.add(dedup_key)

        self.metrics.counter("alerts_persisted_total", tags={
            "service": "alert_manager", "tenant_id": tenant_id,
            "algorithm": algorithm, "severity": msg.get("severity", ""),
        })

        await self._aggregate_into_case(msg)

        self.log.info("Alert processed", alert_id=alert_id,
                      tenant_id=tenant_id, algorithm=algorithm)

    async def _load_dedup_set(self) -> None:
        self._known_alerts = set()
        rows = await self.db.fetch_all_async("SELECT * FROM alerts")
        for row in rows:
            self._known_alerts.add((row.get("algorithm", ""), row.get("event_id", "")))
        self.log.info("Dedup set loaded", count=len(self._known_alerts))

    @staticmethod
    def _prepare_db_row(msg: dict) -> dict:
        row = dict(msg)
        if isinstance(row.get("details"), dict):
            row["details"] = json.dumps(row["details"])
        if isinstance(row.get("created_at"), str):
            dt = datetime.fromisoformat(row["created_at"])
            row["created_at"] = dt.replace(tzinfo=None)
        return row

    # ── Case aggregation ──────────────────────────────────────────────────

    async def _aggregate_into_case(self, alert: dict) -> None:
        tenant_id = alert.get("tenant_id", "")
        algorithm = alert.get("algorithm", "")
        event_id = alert.get("event_id", "")
        alert_id = alert.get("alert_id", "")
        severity = alert.get("severity", "medium")
        created_at = alert.get("created_at", "")

        case = await self._find_matching_case(tenant_id, algorithm, event_id)

        if case:
            await self._add_alert_to_case(case, alert_id, severity, algorithm, created_at)
        else:
            await self._create_case(tenant_id, alert_id, severity, algorithm, created_at)

    async def _find_matching_case(
        self, tenant_id: str, algorithm: str, event_id: str,
    ) -> dict | None:
        all_cases = await self.db.fetch_all_async("SELECT * FROM cases")
        open_cases = [
            c for c in all_cases
            if c.get("tenant_id") == tenant_id and c.get("status") == "open"
        ]
        if not open_cases:
            return None

        # Rule 2: same event_id in case alerts (cross-algorithm grouping)
        all_case_alerts = await self.db.fetch_all_async("SELECT * FROM case_alerts")
        all_alerts = await self.db.fetch_all_async("SELECT * FROM alerts")
        alert_by_id = {a.get("alert_id"): a for a in all_alerts}

        for case in open_cases:
            case_alert_ids = {
                r["alert_id"] for r in all_case_alerts
                if r.get("case_id") == case["case_id"]
            }
            for aid in case_alert_ids:
                a = alert_by_id.get(aid)
                if a and a.get("event_id") == event_id:
                    return case

        # Rule 1: same algorithm, within aggregation window
        for case in open_cases:
            case_algos = case.get("algorithms", [])
            if isinstance(case_algos, str):
                case_algos = [case_algos]
            if algorithm in case_algos:
                last_alert = case.get("last_alert_at")
                if last_alert:
                    if isinstance(last_alert, str):
                        last_alert = datetime.fromisoformat(last_alert)
                    cutoff = datetime.utcnow() - timedelta(
                        minutes=_DEFAULT_AGGREGATION_MINUTES,
                    )
                    if last_alert.replace(tzinfo=None) >= cutoff:
                        return case

        return None

    async def _create_case(
        self, tenant_id: str, alert_id: str,
        severity: str, algorithm: str, alert_time: Any,
    ) -> None:
        case_id = uuid.uuid4().hex
        now = datetime.utcnow()
        alert_dt = self._to_naive_dt(alert_time) or now

        case = {
            "case_id": case_id,
            "tenant_id": tenant_id,
            "status": "open",
            "severity": severity,
            "title": _generate_title(1, [algorithm], tenant_id),
            "description": None,
            "alert_count": 1,
            "first_alert_at": alert_dt,
            "last_alert_at": alert_dt,
            "algorithms": [algorithm],
            "created_at": now,
            "updated_at": now,
        }
        await self.db.insert_one_async("cases", case)
        await self.db.insert_one_async("case_alerts", {
            "case_id": case_id, "alert_id": alert_id,
        })

    async def _add_alert_to_case(
        self, case: dict, alert_id: str,
        severity: str, algorithm: str, alert_time: Any,
    ) -> None:
        case_id = case["case_id"]
        new_count = case.get("alert_count", 0) + 1
        existing_algos = case.get("algorithms", [])
        if isinstance(existing_algos, str):
            existing_algos = [existing_algos]
        algorithms = list(set(existing_algos + [algorithm]))
        new_severity = max(
            case.get("severity", "medium"), severity,
            key=lambda s: _SEVERITY_ORDER.get(s, 0),
        )
        alert_dt = self._to_naive_dt(alert_time) or datetime.utcnow()

        updated = dict(case)
        updated["alert_count"] = new_count
        updated["algorithms"] = algorithms
        updated["severity"] = new_severity
        updated["last_alert_at"] = alert_dt
        updated["title"] = _generate_title(new_count, algorithms, case["tenant_id"])
        updated["updated_at"] = datetime.utcnow()

        await self.db.execute_async(
            "DELETE FROM cases WHERE case_id = $1", [case_id],
        )
        await self.db.insert_one_async("cases", updated)
        await self.db.insert_one_async("case_alerts", {
            "case_id": case_id, "alert_id": alert_id,
        })

    @staticmethod
    def _to_naive_dt(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).replace(tzinfo=None)
            except (ValueError, TypeError):
                return None
        return None

    # ── REST API ──────────────────────────────────────────────────────────

    def _create_app(self) -> web.Application:
        middlewares: list = []
        jwt_secret = self.secrets.get("JWT_SECRET")
        if jwt_secret:
            from de_platform.pipeline.auth_middleware import create_auth_middleware
            middlewares.append(create_auth_middleware(jwt_secret))

        app = web.Application(middlewares=middlewares)
        app.router.add_get("/api/v1/alerts", self._list_alerts)
        app.router.add_get("/api/v1/alerts/{alert_id}", self._get_alert)
        app.router.add_get("/api/v1/alerts/{alert_id}/case", self._get_alert_case)
        app.router.add_get("/api/v1/cases", self._list_cases)
        app.router.add_get("/api/v1/cases/summary", self._cases_summary)
        app.router.add_get("/api/v1/cases/{case_id}", self._get_case)
        app.router.add_put("/api/v1/cases/{case_id}/status", self._update_case_status)
        return app

    def _resolve_tenant_id(self, request: web.Request) -> str | None:
        jwt_tenant = request.get("tenant_id")
        query_tenant = request.rel_url.query.get("tenant_id")
        if jwt_tenant:
            if request.get("role") == "admin" and query_tenant:
                return query_tenant
            return jwt_tenant
        return query_tenant

    async def _list_alerts(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "alert_manager", "endpoint": "/api/v1/alerts", "method": "GET",
        })
        tenant_id = self._resolve_tenant_id(request)
        severity = request.rel_url.query.get("severity")
        algorithm = request.rel_url.query.get("algorithm")
        limit = int(request.rel_url.query.get("limit", 50))
        offset = int(request.rel_url.query.get("offset", 0))

        rows = await self.db.fetch_all_async("SELECT * FROM alerts")
        if tenant_id:
            rows = [r for r in rows if r.get("tenant_id") == tenant_id]
        if severity:
            rows = [r for r in rows if r.get("severity") == severity]
        if algorithm:
            rows = [r for r in rows if r.get("algorithm") == algorithm]
        result = rows[offset: offset + limit]
        return web.json_response(dumps=_dumps, data=result)

    async def _get_alert(self, request: web.Request) -> web.Response:
        alert_id = request.match_info["alert_id"]
        rows = await self.db.fetch_all_async("SELECT * FROM alerts")
        for row in rows:
            if row.get("alert_id") == alert_id:
                return web.json_response(dumps=_dumps, data=row)
        raise web.HTTPNotFound(
            text=json.dumps({"error": "alert not found"}),
            content_type="application/json",
        )

    async def _get_alert_case(self, request: web.Request) -> web.Response:
        alert_id = request.match_info["alert_id"]
        case_alerts = await self.db.fetch_all_async("SELECT * FROM case_alerts")
        for ca in case_alerts:
            if ca.get("alert_id") == alert_id:
                cases = await self.db.fetch_all_async("SELECT * FROM cases")
                for c in cases:
                    if c.get("case_id") == ca["case_id"]:
                        return web.json_response(dumps=_dumps, data=c)
        raise web.HTTPNotFound(
            text=json.dumps({"error": "no case found for this alert"}),
            content_type="application/json",
        )

    async def _list_cases(self, request: web.Request) -> web.Response:
        self.metrics.counter("http_requests_total", tags={
            "service": "alert_manager", "endpoint": "/api/v1/cases", "method": "GET",
        })
        tenant_id = self._resolve_tenant_id(request)
        status = request.rel_url.query.get("status")
        severity = request.rel_url.query.get("severity")
        limit = int(request.rel_url.query.get("limit", 50))
        offset = int(request.rel_url.query.get("offset", 0))

        rows = await self.db.fetch_all_async("SELECT * FROM cases")
        if tenant_id:
            rows = [r for r in rows if r.get("tenant_id") == tenant_id]
        if status:
            rows = [r for r in rows if r.get("status") == status]
        if severity:
            rows = [r for r in rows if r.get("severity") == severity]
        result = rows[offset: offset + limit]
        return web.json_response(dumps=_dumps, data=result)

    async def _get_case(self, request: web.Request) -> web.Response:
        case_id = request.match_info["case_id"]
        cases = await self.db.fetch_all_async("SELECT * FROM cases")
        case = None
        for c in cases:
            if c.get("case_id") == case_id:
                case = c
                break
        if not case:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "case not found"}),
                content_type="application/json",
            )

        # Attach the alert list
        case_alerts = await self.db.fetch_all_async("SELECT * FROM case_alerts")
        alert_ids = {ca["alert_id"] for ca in case_alerts if ca.get("case_id") == case_id}
        all_alerts = await self.db.fetch_all_async("SELECT * FROM alerts")
        alerts = [a for a in all_alerts if a.get("alert_id") in alert_ids]

        result = dict(case)
        result["alerts"] = alerts
        return web.json_response(dumps=_dumps, data=result)

    async def _update_case_status(self, request: web.Request) -> web.Response:
        case_id = request.match_info["case_id"]
        body = await request.json()
        new_status = body.get("status")
        valid = {"open", "investigating", "resolved", "dismissed"}
        if new_status not in valid:
            raise web.HTTPBadRequest(
                text=json.dumps({"error": f"status must be one of {valid}"}),
                content_type="application/json",
            )

        cases = await self.db.fetch_all_async("SELECT * FROM cases")
        case = None
        for c in cases:
            if c.get("case_id") == case_id:
                case = c
                break
        if not case:
            raise web.HTTPNotFound(
                text=json.dumps({"error": "case not found"}),
                content_type="application/json",
            )

        updated = dict(case)
        updated["status"] = new_status
        updated["updated_at"] = datetime.utcnow()

        await self.db.execute_async("DELETE FROM cases WHERE case_id = $1", [case_id])
        await self.db.insert_one_async("cases", updated)

        self.log.info("Case status updated", case_id=case_id, status=new_status)
        return web.json_response(dumps=_dumps, data=updated)

    async def _cases_summary(self, request: web.Request) -> web.Response:
        tenant_id = self._resolve_tenant_id(request)
        cases = await self.db.fetch_all_async("SELECT * FROM cases")
        if tenant_id:
            cases = [c for c in cases if c.get("tenant_id") == tenant_id]

        summary = {
            "total": len(cases),
            "by_status": {},
            "by_severity": {},
        }
        for c in cases:
            status = c.get("status", "open")
            severity = c.get("severity", "medium")
            summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1
        return web.json_response(dumps=_dumps, data=summary)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None


def _generate_title(
    alert_count: int, algorithms: list[str], tenant_id: str,
) -> str:
    algo_display = {
        "large_notional": "Large notional detected",
        "velocity": "Velocity threshold exceeded",
        "suspicious_counterparty": "Suspicious counterparty activity",
    }
    if len(algorithms) == 1:
        label = algo_display.get(algorithms[0], algorithms[0])
        return f"{label} — {tenant_id} ({alert_count} alerts)"
    return (
        f"Multiple fraud signals — {tenant_id} "
        f"({alert_count} alerts, {len(algorithms)} algorithms)"
    )


module_class = AlertManagerModule
