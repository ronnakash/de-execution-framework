"""Single-tenant stress tests."""

from __future__ import annotations

import pytest

from tests.stress.report import save_report
from tests.stress.scenarios import get_scenario

pytestmark = pytest.mark.stress


async def test_single_tenant_1k(stress_runner):
    """Ingest 1000 mixed events for a single tenant at 200 events/sec."""
    scenario = get_scenario("single_tenant_1k")
    result = await stress_runner.run_scenario(**scenario)

    assert result.metrics.total_events > 0
    assert result.metrics.error_rate < 0.05  # less than 5% errors

    save_report([result])


async def test_burst_5k(stress_runner):
    """Burst-ingest 5000 events as fast as possible (no throttle)."""
    scenario = get_scenario("burst_5k")
    result = await stress_runner.run_scenario(**scenario)

    assert result.metrics.total_events > 0
    assert result.metrics.error_rate < 0.05

    save_report([result])
