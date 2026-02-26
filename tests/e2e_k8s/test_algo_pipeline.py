"""K8s E2E tests: algorithm evaluation through the full pipeline.

REST-only variant of tests/e2e/test_algo_pipeline.py.

Requires: ``pytest -m e2e_k8s`` or ``make test-e2e-k8s``.
"""

from __future__ import annotations

import pytest

from tests.helpers import scenarios
from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e_k8s]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


# ── Algorithm scenarios (REST only) ──────────────────────────────────────────


async def test_alert_generation(harness):
    await scenarios.scenario_alert_via_method(harness, "rest")


async def test_velocity_algorithm(harness):
    await scenarios.scenario_velocity(harness)


async def test_suspicious_counterparty_algorithm(harness):
    await scenarios.scenario_suspicious_counterparty(harness)
