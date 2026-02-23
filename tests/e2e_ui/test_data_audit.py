"""Test data audit page through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_audit_page_loads(logged_in_page, base_url, step_logger):
    """Data audit page renders with summary cards and filters."""
    page = logged_in_page

    step_logger.log("Navigate to audit page", "Open /ui/#/audit")
    page.goto(f"{base_url}/ui/#/audit")

    step_logger.log(
        "Verify page structure",
        "Wait for 'Data Audit' heading, check tenant filter input is visible",
    )
    page.wait_for_selector("text=Data Audit", timeout=5000)
    assert page.locator('input[placeholder="All tenants"]').is_visible()


def test_audit_summary_cards(logged_in_page, base_url, step_logger):
    """Summary cards display with count labels."""
    page = logged_in_page

    step_logger.log("Navigate to audit page", "Open /ui/#/audit")
    page.goto(f"{base_url}/ui/#/audit")

    step_logger.log(
        "Verify summary cards",
        "Check all 4 metric cards: Total Received, Processed, Errors, Duplicates",
    )
    page.wait_for_selector("text=Total Received", timeout=5000)
    assert page.locator("text=Total Processed").is_visible()
    assert page.locator("text=Total Errors").is_visible()
    assert page.locator("text=Total Duplicates").is_visible()


def test_audit_date_filters(logged_in_page, base_url, step_logger):
    """Start and end date inputs are available."""
    page = logged_in_page

    step_logger.log("Navigate to audit page", "Open /ui/#/audit")
    page.goto(f"{base_url}/ui/#/audit")

    step_logger.log(
        "Verify date filters",
        "Check Start Date and End Date labels, confirm 2+ date input fields",
    )
    page.wait_for_selector("text=Start Date", timeout=5000)
    assert page.locator("text=End Date").is_visible()
    assert page.locator('input[type="date"]').count() >= 2
