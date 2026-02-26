"""RealInfraHarness -- lightweight per-test harness using SharedPipeline."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Callable

import aiohttp

from tests.helpers.diagnostics import TestDiagnostics
from tests.helpers.harness.protocol import poll_until
from tests.helpers.harness.shared_pipeline import SharedPipeline, _launch_module
from tests.helpers.ingestion import (
    ingest_files_subprocess,
    ingest_kafka_publish,
    ingest_rest_http,
)
from tests.helpers.step_logger import StepLogger


class RealInfraHarness:
    """Per-test harness backed by a SharedPipeline.

    Each test gets a unique tenant_id.  All ingestion and assertion
    methods filter by this tenant_id so tests are data-isolated.

    Manages its own async PostgresDatabase connection (created in __aenter__,
    closed in __aexit__) since asyncpg connections are tied to event loops.
    Shares the pipeline's sync ClickHouse and Kafka connections.
    """

    _SERVICE_PORT_ATTRS = {
        "data_api": "api_port",
        "client_config": "config_port",
        "auth": "auth_port",
        "alert_manager": "alert_manager_port",
        "data_audit": "data_audit_port",
        "task_scheduler": "task_scheduler_port",
    }

    def __init__(self, pipeline: SharedPipeline) -> None:
        self._pipeline = pipeline
        self.tenant_id: str = f"INTEGRATION_CLIENT_{uuid.uuid4().hex[:12]}"
        self._alerts_db: Any = None
        self._screenshot_page: Any = None
        self.diagnostics: TestDiagnostics | None = None
        self.step_logger: StepLogger = StepLogger()

    async def __aenter__(self) -> RealInfraHarness:
        from de_platform.services.database.postgres_database import PostgresDatabase
        from de_platform.services.secrets.env_secrets import EnvSecrets

        secrets = EnvSecrets(
            overrides=self._pipeline._infra.to_env_overrides(
                group_id=f"assert-{uuid.uuid4().hex[:8]}"
            )
        )
        alerts_db = PostgresDatabase(secrets=secrets, prefix="DB_ALERTS")
        await alerts_db.connect_async()
        self._alerts_db = alerts_db

        self.diagnostics = TestDiagnostics(
            clickhouse_db=self._pipeline._clickhouse_db,
            postgres_db=self._alerts_db,
            tenant_id=self.tenant_id,
        )

        # Set up Playwright page for UI screenshots (if browser available)
        page = None
        browser = getattr(self._pipeline, "_screenshot_browser", None)
        if browser is not None:
            try:
                from de_platform.pipeline.auth_middleware import encode_token

                page = browser.new_page(viewport={"width": 1280, "height": 720})
                token = encode_token(
                    user_id="e2e_admin",
                    tenant_id=self.tenant_id,
                    role="admin",
                    secret=self._pipeline.JWT_SECRET,
                )
                ui_url = f"http://127.0.0.1:{self._pipeline.api_port}/ui"
                page.goto(ui_url, wait_until="networkidle", timeout=10000)
                page.evaluate(
                    f"localStorage.setItem('access_token', '{token}')"
                )
                page.reload(wait_until="networkidle", timeout=10000)
                self._screenshot_page = page
            except Exception:
                if page is not None:
                    try:
                        page.close()
                    except Exception:
                        pass
                self._screenshot_page = None

        self.step_logger = StepLogger(
            diagnostics=self.diagnostics, page=self._screenshot_page,
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._screenshot_page is not None:
            try:
                self._screenshot_page.close()
            except Exception:
                pass
            self._screenshot_page = None
        if self._alerts_db:
            await self._alerts_db.disconnect_async()

    def _filter_by_tenant(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("tenant_id") == self.tenant_id]

    async def _snapshot_text(self) -> str:
        if self.diagnostics is None:
            return ""
        try:
            snap = await self.diagnostics.snapshot()
            return TestDiagnostics.format_snapshot(snap)
        except Exception as e:
            return f"(diagnostics failed: {e})"

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None:
        p = self._pipeline
        if method == "rest":
            await ingest_rest_http(p.rest_port, event_type, events)
        elif method == "kafka":
            ingest_kafka_publish(p._kafka_producer, event_type, events)
        elif method == "files":
            ingest_files_subprocess(
                p._minio_fs, p._infra, event_type, events, _launch_module,
            )
        else:
            raise ValueError(f"Unknown method: {method!r}")

    def _ch_fetch_tenant(self, table: str) -> list[dict]:
        """Fetch rows from ClickHouse filtered by this harness's tenant_id."""
        ch = self._pipeline._clickhouse_db
        query = f"SELECT * FROM {table} WHERE tenant_id = {{p1:String}}"
        return ch.fetch_all(query, [self.tenant_id])

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 60.0
    ) -> list[dict]:
        def _check() -> bool:
            return len(self._ch_fetch_tenant(table)) >= expected

        def _on_timeout() -> str:
            filtered = self._ch_fetch_tenant(table)
            return (
                f"wait_for_rows('{table}', expected={expected}) "
                f"timed out after {timeout}s (got {len(filtered)} "
                f"for tenant {self.tenant_id})"
            )

        await poll_until(_check, timeout=timeout, on_timeout=_on_timeout)
        return self._ch_fetch_tenant(table)

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 5.0
    ) -> None:
        await asyncio.sleep(timeout)
        rows = self._ch_fetch_tenant(table)
        if len(rows) != known:
            snap = await self._snapshot_text()
            raise AssertionError(
                f"Expected {known} rows in {table} to remain unchanged, "
                f"got {len(rows)}\n\n{snap}"
            )

    async def fetch_alerts(self) -> list[dict]:
        rows = await self._alerts_db.fetch_all_async("SELECT * FROM alerts")
        return self._filter_by_tenant(rows)

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 90.0
    ) -> list[dict]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            rows = await self._alerts_db.fetch_all_async("SELECT * FROM alerts")
            rows = self._filter_by_tenant(rows)
            if any(predicate(r) for r in rows):
                return rows
            if loop.time() > deadline:
                snap = await self._snapshot_text()
                raise TimeoutError(
                    f"wait_for_alert: condition not met within {timeout}s"
                    f"\n\n{snap}"
                )
            await asyncio.sleep(0.1)

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"http://127.0.0.1:{self._pipeline.api_port}/api/v1/{endpoint}?{qs}"
        headers = self._auth_headers()
        async with aiohttp.ClientSession() as session:
            resp = await session.get(url, headers=headers)
            body = await resp.json()
            return resp.status, body

    def _auth_headers(self) -> dict[str, str]:
        """Generate a Bearer token header for data_api requests."""
        from de_platform.pipeline.auth_middleware import encode_token

        token = encode_token(
            user_id="e2e_admin",
            tenant_id=self.tenant_id,
            role="admin",
            secret=self._pipeline.JWT_SECRET,
        )
        return {"Authorization": f"Bearer {token}"}

    _NO_HEADERS = object()  # sentinel: "caller didn't specify headers"

    async def call_service(
        self,
        service: str,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None | object = _NO_HEADERS,
    ) -> tuple[int, Any]:
        port = getattr(self._pipeline, self._SERVICE_PORT_ATTRS[service])
        url = f"http://127.0.0.1:{port}{path}"
        # Auto-inject auth headers when caller didn't specify any,
        # so tests work against K8s where JWT_SECRET is always set.
        # Pass headers=None explicitly to send NO auth (e.g. to test 401).
        if headers is self._NO_HEADERS:
            headers = self._auth_headers()
        async with aiohttp.ClientSession() as session:
            resp = await session.request(
                method, url, json=json, params=params, headers=headers,
            )
            try:
                body = await resp.json()
            except Exception:
                body = await resp.text()
            return resp.status, body

    def step(self, name: str, description: str = ""):
        return self.step_logger.step(name, description)

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None:
        self._pipeline._kafka_producer.publish(topic, msg)
