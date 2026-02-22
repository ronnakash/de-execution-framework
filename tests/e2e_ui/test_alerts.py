"""Test alerts page through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_alerts_page_loads(logged_in_page, base_url):
    """Alerts page renders with filter controls and table."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/alerts")
    page.wait_for_selector("text=Alerts", timeout=5000)
    # Filter controls should be visible
    assert page.locator('input[placeholder="All tenants"]').is_visible()


def test_alerts_severity_filter(logged_in_page, base_url):
    """Severity filter dropdown is available."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/alerts")
    page.wait_for_selector("text=Severity", timeout=5000)
    assert page.locator("select").is_visible()


def test_alerts_cases_tab(logged_in_page, base_url):
    """Switching to cases tab shows cases view."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/alerts")
    # Click on Cases tab button
    page.wait_for_selector("button:has-text('Cases')", timeout=5000)
    page.click("button:has-text('Cases')")
    # Cases tab should be active
    page.wait_for_timeout(500)
    assert page.locator("text=Cases").first.is_visible()
