"""Pytest fixtures for stress tests."""

from __future__ import annotations

import os

import pytest

from tests.stress.runner import StressTestRunner


@pytest.fixture(scope="session")
def rest_url() -> str:
    """REST starter base URL."""
    return os.environ.get("STRESS_REST_URL", "http://localhost:8001")


@pytest.fixture(scope="session")
def data_api_url() -> str:
    """Data API base URL."""
    return os.environ.get("STRESS_DATA_API_URL", "http://localhost:8002")


@pytest.fixture(scope="session")
def stress_runner(rest_url: str, data_api_url: str) -> StressTestRunner:
    """Shared StressTestRunner instance for the session."""
    return StressTestRunner(rest_url=rest_url, data_api_url=data_api_url)
