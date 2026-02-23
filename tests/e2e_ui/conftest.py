"""Playwright UI E2E test fixtures.

Requires:
- ``make infra-up`` (docker-compose services)
- ``make build-ui`` (React build -> static files)
- SharedPipeline running (started automatically via session fixture)

Usage::

    make infra-up
    make build-ui
    pytest tests/e2e_ui/ -v -m e2e_ui
"""

from __future__ import annotations

import pytest

# Re-use infrastructure fixtures from E2E tests by direct import.
# pytest discovers fixtures that are in scope in conftest.py modules.
from tests.e2e.conftest import (  # noqa: F401
    _init_schemas,
    infra,
    shared_pipeline,
)
from tests.helpers.step_logger import StepLogger


@pytest.fixture(scope="session")
def browser():
    """Launch a Playwright Chromium browser for the test session."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        yield b
        b.close()


@pytest.fixture
def base_url(shared_pipeline) -> str:
    """Base URL for the data_api UI."""
    return f"http://localhost:{shared_pipeline.api_port}"


@pytest.fixture
def page(browser, base_url):
    """Provide a fresh browser page."""
    context = browser.new_context()
    pg = context.new_page()
    yield pg
    context.close()


@pytest.fixture
def logged_in_page(page, base_url):
    """Provide a page that has completed login with the E2E admin user."""
    page.goto(f"{base_url}/ui/#/login")
    page.fill('input[type="email"]', "admin@e2e.test")
    page.fill('input[type="password"]', "e2e_password")
    page.click('button[type="submit"]')
    page.wait_for_url("**/alerts**", timeout=10000)
    return page


@pytest.fixture
def step_logger():
    """Provide a StepLogger for UI tests (collected by the report plugin)."""
    return StepLogger()
