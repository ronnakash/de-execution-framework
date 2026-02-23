"""Test login flow through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_login_page_loads(page, base_url, step_logger):
    """Login page renders with email and password fields."""
    step_logger.log("Navigate to login page", "Open /ui/#/login in the browser")
    page.goto(f"{base_url}/ui/#/login")

    step_logger.log(
        "Verify form elements",
        "Check email input, password input, and submit button are visible",
    )
    assert page.locator('input[type="email"]').is_visible()
    assert page.locator('input[type="password"]').is_visible()
    assert page.locator('button[type="submit"]').is_visible()


def test_login_success(page, base_url, step_logger):
    """Successful login redirects to alerts dashboard."""
    step_logger.log("Navigate to login page", "Open /ui/#/login")
    page.goto(f"{base_url}/ui/#/login")

    step_logger.log(
        "Submit valid credentials",
        "Fill email=admin@e2e.test, password=e2e_password and click submit",
    )
    page.fill('input[type="email"]', "admin@e2e.test")
    page.fill('input[type="password"]', "e2e_password")
    page.click('button[type="submit"]')

    step_logger.log(
        "Verify redirect to alerts",
        "Wait for URL to contain /alerts, check navigation links visible",
    )
    page.wait_for_url("**/alerts**", timeout=10000)
    assert page.locator("text=Alerts").first.is_visible()
    assert page.locator("text=Events").first.is_visible()


def test_login_failure(page, base_url, step_logger):
    """Invalid credentials show error message."""
    step_logger.log("Navigate to login page", "Open /ui/#/login")
    page.goto(f"{base_url}/ui/#/login")

    step_logger.log(
        "Submit invalid credentials",
        "Fill email=bad@example.com, password=wrong and click submit",
    )
    page.fill('input[type="email"]', "bad@example.com")
    page.fill('input[type="password"]', "wrong")
    page.click('button[type="submit"]')

    step_logger.log(
        "Verify error shown",
        "Wait for red error banner (.bg-red-50), confirm still on login page",
    )
    page.wait_for_selector(".bg-red-50", timeout=5000)
    assert "login" in page.url.lower()


def test_logout(logged_in_page, base_url, step_logger):
    """Clicking logout returns to login page."""
    page = logged_in_page

    step_logger.log("Click logout", "Click the Logout button in the navigation")
    page.click("text=Logout")

    step_logger.log(
        "Verify redirect to login",
        "Wait for URL to contain /login, check email field is visible",
    )
    page.wait_for_url("**/login**", timeout=5000)
    assert page.locator('input[type="email"]').is_visible()
