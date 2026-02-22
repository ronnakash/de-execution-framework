"""Real-infra E2E tests (container-style).

All tests run against a single shared pipeline of 6 module subprocesses
with real Postgres, ClickHouse, Redis, Kafka, and MinIO (via docker-compose).

34 tests: 27 matrix (3 methods x 3 event types x 3 scenarios) + 7 general.

Requires: ``pytest -m real_infra`` or ``make test-real-infra``.
"""

from __future__ import annotations

import pytest

from tests.helpers.events import FULL_MATRIX
from tests.helpers.harness import RealInfraHarness
from tests.helpers import scenarios

pytestmark = [pytest.mark.real_infra]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


@pytest.fixture
async def harness_suspicious_cp(shared_pipeline):
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


# ── General scenarios (7 tests) ──────────────────────────────────────────────


async def test_internal_dedup(harness):
    await scenarios.scenario_internal_dedup(harness)


@pytest.mark.parametrize("method", ["rest", "kafka", "files"])
async def test_alert_generation(harness, method):
    await scenarios.scenario_alert_via_method(harness, method)


async def test_large_notional_algorithm(harness):
    await scenarios.scenario_large_notional(harness)


async def test_velocity_algorithm(harness):
    await scenarios.scenario_velocity(harness)


async def test_suspicious_counterparty_algorithm(harness_suspicious_cp):
    await scenarios.scenario_suspicious_counterparty(harness_suspicious_cp)


# ── Bug fix scenarios ────────────────────────────────────────────────────────


@pytest.mark.parametrize("method,event_type", FULL_MATRIX)
async def test_multi_error_consolidation(harness, method, event_type):
    await scenarios.scenario_multi_error_consolidation(harness, method, event_type)


async def test_duplicate_contains_original_event(harness):
    await scenarios.scenario_duplicate_contains_original_event(harness)
