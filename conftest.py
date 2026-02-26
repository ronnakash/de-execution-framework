"""Root-level pytest configuration."""

from __future__ import annotations

import pytest

from tests.helpers.pytest_pipeline_report import PipelineReportPlugin

# Register the pipeline report plugin
_report_plugin = PipelineReportPlugin()


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(_report_plugin, "pipeline-report")


@pytest.fixture
def pipeline_report_plugin() -> PipelineReportPlugin:
    """Access the pipeline report plugin instance."""
    return _report_plugin
