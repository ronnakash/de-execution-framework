"""Subprocess E2E tests via SubprocessHarness.

The most production-realistic test suite: each pipeline module runs as a
separate OS subprocess via ``python3 -m de_platform run <module> ...``.

34 tests: 27 matrix (3 methods x 3 event types x 3 scenarios) + 7 general.

Requires: ``pytest -m real_infra`` or ``make test-real-infra``.
"""

from __future__ import annotations

import pytest

from tests.helpers.events import FULL_MATRIX
from tests.helpers.harness import SubprocessHarness
from tests.helpers import scenarios

pytestmark = [pytest.mark.real_infra]


@pytest.fixture
async def harness(infra):
    async with SubprocessHarness(infra) as h:
        yield h


@pytest.fixture
async def harness_suspicious_cp(infra):
    async with SubprocessHarness(
        infra, algos_extra=["--suspicious-counterparty-ids", "bad-cp-1"]
    ) as h:
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
