"""StressTestRunner — orchestrates stress test scenarios.

Generates events, ingests them at a configurable rate via REST,
waits for pipeline processing, and collects metrics.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp

from tests.stress.generators import generate_mixed_batch
from tests.stress.metrics import StressMetrics
from tests.stress.report import StressTestResult

REST_ENDPOINTS = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}


class StressTestRunner:
    """Orchestrates stress test scenarios against the pipeline."""

    def __init__(
        self,
        rest_url: str,
        data_api_url: str,
    ) -> None:
        self.rest_url = rest_url.rstrip("/")
        self.data_api_url = data_api_url.rstrip("/")

    async def run_scenario(
        self,
        scenario_name: str,
        tenants: list[str],
        events_per_tenant: int,
        event_types: list[str] | None = None,
        rate_per_second: int = 0,
    ) -> StressTestResult:
        """Run a stress test scenario and collect metrics.

        Args:
            scenario_name: Name for this scenario.
            tenants: List of tenant IDs to generate events for.
            events_per_tenant: Number of events per tenant.
            event_types: Event types to generate (default: all).
            rate_per_second: Throttle rate. 0 = no throttle (burst).
        """
        types = event_types or ["order", "execution", "transaction"]
        metrics = StressMetrics()
        metrics.start()

        # Generate all events up front
        all_events: list[tuple[str, str, dict[str, Any]]] = []
        for tenant in tenants:
            batch = generate_mixed_batch(tenant, events_per_tenant, types)
            for etype, event in batch:
                all_events.append((tenant, etype, event))

        # Ingest events concurrently per tenant
        async with aiohttp.ClientSession() as session:
            tasks = []
            for tenant in tenants:
                tenant_events = [(et, ev) for t, et, ev in all_events if t == tenant]
                tasks.append(
                    self._ingest_tenant_events(
                        session, tenant, tenant_events, rate_per_second, metrics
                    )
                )
            await asyncio.gather(*tasks)

        metrics.stop()

        return StressTestResult(
            scenario_name=scenario_name,
            tenants=tenants,
            events_per_tenant=events_per_tenant,
            event_types=types,
            metrics=metrics,
        )

    async def _ingest_tenant_events(
        self,
        session: aiohttp.ClientSession,
        tenant_id: str,
        events: list[tuple[str, dict[str, Any]]],
        rate_per_second: int,
        metrics: StressMetrics,
    ) -> None:
        """Ingest events for a single tenant at the specified rate."""
        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0

        for etype, event in events:
            t0 = time.monotonic()
            endpoint = REST_ENDPOINTS[etype]
            url = f"{self.rest_url}/api/v1/ingest/{endpoint}"
            try:
                async with session.post(url, json=event) as resp:
                    if resp.status in (200, 201, 202):
                        metrics.record_ingestion(t0)
                    else:
                        metrics.record_error()
            except Exception:
                metrics.record_error()

            if interval > 0:
                elapsed = time.monotonic() - t0
                if elapsed < interval:
                    await asyncio.sleep(interval - elapsed)

    async def wait_for_pipeline(
        self,
        tenant_id: str,
        table: str,
        expected_count: int,
        timeout: float = 60.0,
    ) -> int:
        """Poll the data API until expected row count is reached.

        Returns the actual row count when complete or at timeout.
        """
        deadline = time.monotonic() + timeout
        url = f"{self.data_api_url}/api/v1/events/{table}?tenant_id={tenant_id}"

        async with aiohttp.ClientSession() as session:
            while time.monotonic() < deadline:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if len(data) >= expected_count:
                                return len(data)
                except Exception:
                    pass
                await asyncio.sleep(1.0)

        return 0
