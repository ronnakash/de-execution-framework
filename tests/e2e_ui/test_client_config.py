"""Test client configuration page through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_config_page_loads(logged_in_page, base_url):
    """Client config page renders with table and create button."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/config")
    page.wait_for_selector("text=Client Configuration", timeout=5000)
    assert page.locator("text=New Client").is_visible()


def test_create_client_form_toggle(logged_in_page, base_url):
    """Clicking New Client shows the create form, Cancel hides it."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/config")
    page.wait_for_selector("text=New Client", timeout=5000)
    page.click("text=New Client")
    # Form fields should appear
    page.wait_for_selector("text=Tenant ID", timeout=3000)
    assert page.locator("text=Display Name").is_visible()
    assert page.locator("text=Mode").is_visible()
    # Cancel hides the form
    page.click("text=Cancel")
    page.wait_for_timeout(300)
    # "New Client" button should reappear
    assert page.locator("button:has-text('New Client')").is_visible()


def test_events_page_loads(logged_in_page, base_url):
    """Events page renders with type buttons and filters."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/events")
    page.wait_for_selector("text=Events Explorer", timeout=5000)
    assert page.locator("button:has-text('orders')").is_visible()
    assert page.locator("button:has-text('executions')").is_visible()
    assert page.locator("button:has-text('transactions')").is_visible()


def test_tasks_page_loads(logged_in_page, base_url):
    """Tasks page renders with table and create button."""
    page = logged_in_page
    page.goto(f"{base_url}/ui/#/tasks")
    page.wait_for_selector("text=Task Manager", timeout=5000)
    assert page.locator("text=New Task").is_visible()
