"""Playwright UI E2E test fixtures.

Requires:
- ``make infra-up`` (docker-compose services)
- ``make build-ui`` (React build -> static files)
- SharedPipeline running (started automatically via session fixture)

Usage::

    make infra-up
    make build-ui
    pytest tests/e2e_ui/ -v -m e2e_ui
"""

from __future__ import annotations

import json as _json
import time
import urllib.error
import urllib.request
import uuid

import pytest

# Re-use infrastructure fixtures from E2E tests by direct import.
# pytest discovers fixtures that are in scope in conftest.py modules.
from tests.e2e.conftest import (  # noqa: F401
    _init_schemas,
    infra,
    shared_pipeline,
)
from tests.helpers.step_logger import StepLogger


@pytest.fixture(scope="session")
def browser():
    """Launch a Playwright Chromium browser for the test session."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        yield b
        b.close()


@pytest.fixture
def base_url(shared_pipeline) -> str:
    """Base URL for the data_api UI."""
    return f"http://localhost:{shared_pipeline.api_port}"


@pytest.fixture
def page(browser, base_url):
    """Provide a fresh browser page."""
    context = browser.new_context()
    pg = context.new_page()
    yield pg
    context.close()


@pytest.fixture
def logged_in_page(page, base_url):
    """Provide a page that has completed login with the E2E admin user."""
    page.goto(f"{base_url}/ui/#/login")
    page.fill('input[type="email"]', "admin@e2e.test")
    page.fill('input[type="password"]', "e2e_password")
    page.click('button[type="submit"]')
    page.wait_for_url("**/alerts**", timeout=10000)
    return page


@pytest.fixture
def step_logger(page):
    """Provide a StepLogger for UI tests (collected by the report plugin).

    Calls ``finalize()`` on teardown to capture the screenshot for the last step.
    """
    sl = StepLogger(page=page)
    yield sl
    sl.finalize()


# ── Pipeline data helper ─────────────────────────────────────────────────────


class PipelineDataHelper:
    """Sync helper for generating pipeline data from UI tests.

    Uses ``urllib.request`` (stdlib, sync) for HTTP — same pattern as
    ``_wait_for_http_sync()`` in ``harness.py``.
    """

    def __init__(self, pipeline) -> None:
        self._pipeline = pipeline
        self.tenant_id: str = f"UI_TEST_{uuid.uuid4().hex[:12]}"

    # ── Ingestion ─────────────────────────────────────────────────────

    def ingest(self, event_type: str, events: list[dict]) -> None:
        """POST events to the REST ingestion endpoint (sync)."""
        from tests.helpers.events import REST_ENDPOINT

        endpoint = REST_ENDPOINT[event_type]
        url = f"http://127.0.0.1:{self._pipeline.rest_port}/api/v1/{endpoint}"
        body = _json.dumps({"events": events}).encode()
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            assert resp.status == 200

    # ── Polling helpers ───────────────────────────────────────────────

    def wait_for_rows(self, table: str, expected: int, timeout: float = 60.0) -> list[dict]:
        """Poll ClickHouse until at least ``expected`` rows appear for this tenant."""
        ch = self._pipeline._clickhouse_db
        deadline = time.monotonic() + timeout
        while True:
            query = f"SELECT * FROM {table} WHERE tenant_id = {{p1:String}}"
            rows = ch.fetch_all(query, [self.tenant_id])
            if len(rows) >= expected:
                return rows
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"wait_for_rows('{table}', {expected}): got {len(rows)} "
                    f"for tenant {self.tenant_id} after {timeout}s"
                )
            time.sleep(0.5)

    def wait_for_alerts(self, timeout: float = 90.0) -> list[dict]:
        """Poll alert_manager REST API until alerts appear for this tenant."""
        url = (
            f"http://127.0.0.1:{self._pipeline.alert_manager_port}"
            f"/api/v1/alerts?tenant_id={self.tenant_id}"
        )
        deadline = time.monotonic() + timeout
        while True:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    data = _json.loads(resp.read())
                    alerts = data if isinstance(data, list) else data.get("alerts", [])
                    if alerts:
                        return alerts
            except (urllib.error.URLError, OSError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"wait_for_alerts: no alerts for tenant {self.tenant_id} "
                    f"after {timeout}s"
                )
            time.sleep(0.5)

    def wait_for_cases(self, timeout: float = 90.0) -> list[dict]:
        """Poll alert_manager REST API until cases appear for this tenant."""
        url = (
            f"http://127.0.0.1:{self._pipeline.alert_manager_port}"
            f"/api/v1/cases?tenant_id={self.tenant_id}"
        )
        deadline = time.monotonic() + timeout
        while True:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    data = _json.loads(resp.read())
                    cases = data if isinstance(data, list) else data.get("cases", [])
                    if cases:
                        return cases
            except (urllib.error.URLError, OSError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"wait_for_cases: no cases for tenant {self.tenant_id} "
                    f"after {timeout}s"
                )
            time.sleep(0.5)

    def wait_for_audit_summary(
        self, *, min_processed: int = 0, timeout: float = 60.0,
    ) -> dict:
        """Poll data_audit REST API until processed count meets threshold."""
        url = (
            f"http://127.0.0.1:{self._pipeline.data_audit_port}"
            f"/api/v1/audit/summary?tenant_id={self.tenant_id}"
        )
        deadline = time.monotonic() + timeout
        while True:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    data = _json.loads(resp.read())
                    processed = data.get("total_processed", 0)
                    if processed >= min_processed:
                        return data
            except (urllib.error.URLError, OSError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"wait_for_audit_summary: processed < {min_processed} "
                    f"for tenant {self.tenant_id} after {timeout}s"
                )
            time.sleep(0.5)


@pytest.fixture
def pipeline_data(shared_pipeline):
    """Provide a PipelineDataHelper for data-driven UI tests."""
    return PipelineDataHelper(shared_pipeline)
