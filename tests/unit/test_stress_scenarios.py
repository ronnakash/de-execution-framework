"""Tests for stress test scenario definitions."""

from tests.stress.scenarios import SCENARIOS, get_scenario


def test_all_scenarios_exist():
    expected = {"single_tenant_1k", "single_tenant_10k", "multi_tenant_10x1k", "burst_5k"}
    assert set(SCENARIOS.keys()) == expected


def test_scenario_has_required_keys():
    for name, scenario in SCENARIOS.items():
        assert "scenario_name" in scenario, f"{name} missing scenario_name"
        assert "tenants" in scenario, f"{name} missing tenants"
        assert "events_per_tenant" in scenario, f"{name} missing events_per_tenant"
        assert "event_types" in scenario, f"{name} missing event_types"
        assert "rate_per_second" in scenario, f"{name} missing rate_per_second"


def test_single_tenant_1k_config():
    s = SCENARIOS["single_tenant_1k"]
    assert len(s["tenants"]) == 1
    assert s["events_per_tenant"] == 1000
    assert s["rate_per_second"] == 200


def test_multi_tenant_has_10_tenants():
    s = SCENARIOS["multi_tenant_10x1k"]
    assert len(s["tenants"]) == 10
    assert s["events_per_tenant"] == 1000


def test_burst_has_zero_rate():
    s = SCENARIOS["burst_5k"]
    assert s["rate_per_second"] == 0


def test_get_scenario_returns_fresh_tenants():
    s1 = get_scenario("single_tenant_1k")
    s2 = get_scenario("single_tenant_1k")
    # Each call should produce different tenant IDs
    assert s1["tenants"] != s2["tenants"]


def test_get_scenario_preserves_event_count():
    s = get_scenario("multi_tenant_10x1k")
    assert len(s["tenants"]) == 10
    assert s["events_per_tenant"] == 1000
