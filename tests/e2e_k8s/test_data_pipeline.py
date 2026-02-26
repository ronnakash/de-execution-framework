"""K8s E2E tests: data flow through the full pipeline.

REST-only variant of tests/e2e/test_data_pipeline.py.  Kafka and file
ingestion are not available from outside the K8s cluster.

Requires: ``pytest -m e2e_k8s`` or ``make test-e2e-k8s``.
"""

from __future__ import annotations

import pytest

from tests.helpers import scenarios
from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e_k8s]

REST_MATRIX = [("rest", etype) for etype in ("order", "execution", "transaction")]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


# ── Matrix scenarios (REST only) ─────────────────────────────────────────────


@pytest.mark.parametrize("method,event_type", REST_MATRIX)
async def test_valid_events(harness, method, event_type):
    await scenarios.scenario_valid_events(harness, method, event_type)


@pytest.mark.parametrize("method,event_type", REST_MATRIX)
async def test_invalid_events(harness, method, event_type):
    await scenarios.scenario_invalid_events(harness, method, event_type)


@pytest.mark.parametrize("method,event_type", REST_MATRIX)
async def test_duplicate_events(harness, method, event_type):
    await scenarios.scenario_duplicate_events(harness, method, event_type)


@pytest.mark.parametrize("method,event_type", REST_MATRIX)
async def test_multi_error_consolidation(harness, method, event_type):
    await scenarios.scenario_multi_error_consolidation(harness, method, event_type)
