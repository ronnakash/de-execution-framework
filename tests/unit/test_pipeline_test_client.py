"""Tests for PipelineTestClient."""

from __future__ import annotations

from tests.helpers.harness.memory import MemoryHarness
from tests.helpers.pipeline_test_client import PipelineTestClient


async def test_client_tenant_id():
    """Client exposes the harness tenant_id."""
    h = MemoryHarness()
    client = PipelineTestClient(h)
    assert client.tenant_id == "acme"


async def test_client_ingest_events():
    """Client delegates ingest_events to harness.ingest()."""
    from tests.helpers.events import make_order

    h = MemoryHarness()
    client = PipelineTestClient(h)

    events = [make_order(tenant_id="acme")]
    await client.ingest_events("order", events, method="rest")

    rows = await client.wait_for_rows("orders", 1)
    assert len(rows) >= 1


async def test_client_ingest_file():
    """Client delegates ingest_file to harness.ingest('files', ...)."""
    from tests.helpers.events import make_order

    h = MemoryHarness()
    client = PipelineTestClient(h)

    events = [make_order(tenant_id="acme")]
    await client.ingest_file("order", events)

    rows = await client.wait_for_rows("orders", 1)
    assert len(rows) >= 1


async def test_client_fetch_alerts():
    """Client fetch_alerts delegates to harness."""
    h = MemoryHarness()
    await h._ensure_modules()
    client = PipelineTestClient(h)
    alerts = await client.fetch_alerts()
    assert isinstance(alerts, list)


async def test_client_step():
    """Client step() returns a context manager from the harness."""
    h = MemoryHarness()
    client = PipelineTestClient(h)

    async with client.step("test step"):
        pass

    assert len(h.step_logger.steps) == 1
    assert h.step_logger.steps[0].name == "test step"
