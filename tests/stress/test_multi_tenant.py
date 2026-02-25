"""Multi-tenant concurrent stress tests."""

from __future__ import annotations

import pytest

from tests.stress.report import save_report
from tests.stress.scenarios import get_scenario

pytestmark = pytest.mark.stress


async def test_multi_tenant_10x1k(stress_runner):
    """10 tenants sending 1000 events each concurrently."""
    scenario = get_scenario("multi_tenant_10x1k")
    result = await stress_runner.run_scenario(**scenario)

    assert result.metrics.total_events > 0
    assert result.metrics.error_rate < 0.05

    save_report([result])
