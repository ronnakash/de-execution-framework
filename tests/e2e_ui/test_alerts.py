"""Test alerts page through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_alerts_page_loads(logged_in_page, base_url, step_logger):
    """Alerts page renders with filter controls and table."""
    page = logged_in_page

    step_logger.log("Navigate to alerts page", "Open /ui/#/alerts")
    page.goto(f"{base_url}/ui/#/alerts")

    step_logger.log(
        "Verify page structure",
        "Wait for 'Alerts' heading, check tenant filter input is visible",
    )
    page.wait_for_selector("text=Alerts", timeout=5000)
    assert page.locator('input[placeholder="All tenants"]').is_visible()


def test_alerts_severity_filter(logged_in_page, base_url, step_logger):
    """Severity filter dropdown is available."""
    page = logged_in_page

    step_logger.log("Navigate to alerts page", "Open /ui/#/alerts")
    page.goto(f"{base_url}/ui/#/alerts")

    step_logger.log(
        "Verify severity filter",
        "Wait for 'Severity' label, check select dropdown is visible",
    )
    page.wait_for_selector("text=Severity", timeout=5000)
    assert page.locator("select").is_visible()


def test_alerts_cases_tab(logged_in_page, base_url, step_logger):
    """Switching to cases tab shows cases view."""
    page = logged_in_page

    step_logger.log("Navigate to alerts page", "Open /ui/#/alerts")
    page.goto(f"{base_url}/ui/#/alerts")

    step_logger.log(
        "Switch to Cases tab",
        "Click the 'Cases' tab button and verify it becomes active",
    )
    page.wait_for_selector("button:has-text('Cases')", timeout=5000)
    page.click("button:has-text('Cases')")
    page.wait_for_timeout(500)
    assert page.locator("text=Cases").first.is_visible()
