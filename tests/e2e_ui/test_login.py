"""Test login flow through the UI."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e_ui


def test_login_page_loads(page, base_url):
    """Login page renders with email and password fields."""
    page.goto(f"{base_url}/ui/#/login")
    assert page.locator('input[type="email"]').is_visible()
    assert page.locator('input[type="password"]').is_visible()
    assert page.locator('button[type="submit"]').is_visible()


def test_login_success(page, base_url):
    """Successful login redirects to alerts dashboard."""
    page.goto(f"{base_url}/ui/#/login")
    page.fill('input[type="email"]', "admin@e2e.test")
    page.fill('input[type="password"]', "e2e_password")
    page.click('button[type="submit"]')
    page.wait_for_url("**/alerts**", timeout=10000)
    # Navigation should be visible
    assert page.locator("text=Alerts").first.is_visible()
    assert page.locator("text=Events").first.is_visible()


def test_login_failure(page, base_url):
    """Invalid credentials show error message."""
    page.goto(f"{base_url}/ui/#/login")
    page.fill('input[type="email"]', "bad@example.com")
    page.fill('input[type="password"]', "wrong")
    page.click('button[type="submit"]')
    # Should see error and remain on login
    page.wait_for_selector(".bg-red-50", timeout=5000)
    assert "login" in page.url.lower()


def test_logout(logged_in_page, base_url):
    """Clicking logout returns to login page."""
    page = logged_in_page
    page.click("text=Logout")
    page.wait_for_url("**/login**", timeout=5000)
    assert page.locator('input[type="email"]').is_visible()
