"""Predefined stress test scenarios.

Each scenario is a dict of keyword arguments for StressTestRunner.run_scenario().
"""

from __future__ import annotations

import uuid
from typing import Any


def _tenant_ids(count: int) -> list[str]:
    """Generate unique tenant IDs for stress tests."""
    return [f"STRESS_{uuid.uuid4().hex[:8]}" for _ in range(count)]


SCENARIOS: dict[str, dict[str, Any]] = {
    "single_tenant_1k": {
        "scenario_name": "single_tenant_1k",
        "tenants": _tenant_ids(1),
        "events_per_tenant": 1000,
        "event_types": ["order", "execution", "transaction"],
        "rate_per_second": 200,
    },
    "single_tenant_10k": {
        "scenario_name": "single_tenant_10k",
        "tenants": _tenant_ids(1),
        "events_per_tenant": 10000,
        "event_types": ["order", "execution", "transaction"],
        "rate_per_second": 500,
    },
    "multi_tenant_10x1k": {
        "scenario_name": "multi_tenant_10x1k",
        "tenants": _tenant_ids(10),
        "events_per_tenant": 1000,
        "event_types": ["order", "execution", "transaction"],
        "rate_per_second": 100,
    },
    "burst_5k": {
        "scenario_name": "burst_5k",
        "tenants": _tenant_ids(1),
        "events_per_tenant": 5000,
        "event_types": ["order", "execution", "transaction"],
        "rate_per_second": 0,  # no throttle
    },
}


def get_scenario(name: str) -> dict[str, Any]:
    """Get a scenario by name, generating fresh tenant IDs each time."""
    base = SCENARIOS[name].copy()
    base["tenants"] = _tenant_ids(len(base["tenants"]))
    return base
