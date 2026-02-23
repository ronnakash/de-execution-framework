"""E2E tests: algorithm evaluation through the full pipeline.

Tests alert generation across ingestion methods and individual
algorithm scenarios (large notional, velocity, suspicious counterparty).

6 tests: 3 alert_generation + 1 large_notional + 1 velocity + 1 suspicious_cp.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import pytest

from tests.helpers import scenarios
from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


@pytest.fixture
async def harness_suspicious_cp(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


# ── Algorithm scenarios (6 tests) ────────────────────────────────────────────


@pytest.mark.parametrize("method", ["rest", "kafka", "files"])
async def test_alert_generation(harness, method):
    await scenarios.scenario_alert_via_method(harness, method)


async def test_large_notional_algorithm(harness):
    await scenarios.scenario_large_notional(harness)


async def test_velocity_algorithm(harness):
    await scenarios.scenario_velocity(harness)


async def test_suspicious_counterparty_algorithm(harness_suspicious_cp):
    await scenarios.scenario_suspicious_counterparty(harness_suspicious_cp)
