"""Test client configuration page through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_config_page_loads(logged_in_page, base_url, step_logger):
    """Client config page renders with table and create button."""
    page = logged_in_page

    step_logger.log("Navigate to config page", "Open /ui/#/config")
    page.goto(f"{base_url}/ui/#/config")

    step_logger.log(
        "Verify page structure",
        "Wait for 'Client Configuration' heading, check 'New Client' button visible",
    )
    page.wait_for_selector("text=Client Configuration", timeout=5000)
    assert page.locator("text=New Client").is_visible()


def test_create_client_form_toggle(logged_in_page, base_url, step_logger):
    """Clicking New Client shows the create form, Cancel hides it."""
    page = logged_in_page

    step_logger.log("Navigate to config page", "Open /ui/#/config")
    page.goto(f"{base_url}/ui/#/config")

    step_logger.log(
        "Open create form",
        "Click 'New Client' button, wait for form fields to appear",
    )
    page.wait_for_selector("text=New Client", timeout=5000)
    page.click("text=New Client")
    page.wait_for_selector("text=Tenant ID", timeout=3000)
    assert page.locator("text=Display Name").is_visible()
    assert page.locator("text=Mode").is_visible()

    step_logger.log(
        "Close create form",
        "Click 'Cancel' button, verify 'New Client' button reappears",
    )
    page.click("text=Cancel")
    page.wait_for_timeout(300)
    assert page.locator("button:has-text('New Client')").is_visible()


def test_events_page_loads(logged_in_page, base_url, step_logger):
    """Events page renders with type buttons and filters."""
    page = logged_in_page

    step_logger.log("Navigate to events page", "Open /ui/#/events")
    page.goto(f"{base_url}/ui/#/events")

    step_logger.log(
        "Verify page structure",
        "Wait for 'Events Explorer' heading, check event type buttons",
    )
    page.wait_for_selector("text=Events Explorer", timeout=5000)
    assert page.locator("button:has-text('orders')").is_visible()
    assert page.locator("button:has-text('executions')").is_visible()
    assert page.locator("button:has-text('transactions')").is_visible()


def test_tasks_page_loads(logged_in_page, base_url, step_logger):
    """Tasks page renders with table and create button."""
    page = logged_in_page

    step_logger.log("Navigate to tasks page", "Open /ui/#/tasks")
    page.goto(f"{base_url}/ui/#/tasks")

    step_logger.log(
        "Verify page structure",
        "Wait for 'Task Manager' heading, check 'New Task' button visible",
    )
    page.wait_for_selector("text=Task Manager", timeout=5000)
    assert page.locator("text=New Task").is_visible()
