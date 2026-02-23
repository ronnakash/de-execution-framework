"""E2E tests: data flow through the full pipeline.

Tests valid, invalid, duplicate, and error consolidation scenarios
across all ingestion methods (rest, kafka, files) and event types
(order, execution, transaction).

29 tests: 9 valid + 9 invalid + 9 duplicate + 1 internal_dedup + 1 duplicate_contains_original.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import pytest

from tests.helpers import scenarios
from tests.helpers.events import FULL_MATRIX
from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


# ── Matrix scenarios (27 tests) ──────────────────────────────────────────────


@pytest.mark.parametrize("method,event_type", FULL_MATRIX)
async def test_valid_events(harness, method, event_type):
    await scenarios.scenario_valid_events(harness, method, event_type)


@pytest.mark.parametrize("method,event_type", FULL_MATRIX)
async def test_invalid_events(harness, method, event_type):
    await scenarios.scenario_invalid_events(harness, method, event_type)


@pytest.mark.parametrize("method,event_type", FULL_MATRIX)
async def test_duplicate_events(harness, method, event_type):
    await scenarios.scenario_duplicate_events(harness, method, event_type)


# ── General scenarios ─────────────────────────────────────────────────────────


async def test_internal_dedup(harness):
    await scenarios.scenario_internal_dedup(harness)


@pytest.mark.parametrize("method,event_type", FULL_MATRIX)
async def test_multi_error_consolidation(harness, method, event_type):
    await scenarios.scenario_multi_error_consolidation(harness, method, event_type)


async def test_duplicate_contains_original_event(harness):
    await scenarios.scenario_duplicate_contains_original_event(harness)
