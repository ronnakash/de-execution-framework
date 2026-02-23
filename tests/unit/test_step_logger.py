"""Tests for StepLogger."""

from __future__ import annotations

import pytest

from tests.helpers.step_logger import StepLogger


@pytest.mark.asyncio
async def test_step_logger_records_steps() -> None:
    logger = StepLogger()
    async with logger.step("step 1", "description"):
        pass
    async with logger.step("step 2"):
        pass
    assert len(logger.steps) == 2
    assert logger.steps[0].name == "step 1"
    assert logger.steps[0].description == "description"
    assert logger.steps[0].duration_seconds >= 0
    assert logger.steps[1].name == "step 2"


@pytest.mark.asyncio
async def test_step_logger_records_errors() -> None:
    logger = StepLogger()
    with pytest.raises(ValueError):
        async with logger.step("failing step"):
            raise ValueError("boom")
    assert len(logger.steps) == 1
    assert logger.steps[0].error == "boom"
    assert logger.steps[0].duration_seconds >= 0


@pytest.mark.asyncio
async def test_step_logger_to_dicts() -> None:
    logger = StepLogger()
    async with logger.step("first", "desc"):
        pass
    dicts = logger.to_dicts()
    assert len(dicts) == 1
    assert dicts[0]["name"] == "first"
    assert dicts[0]["description"] == "desc"
    assert "duration_seconds" in dicts[0]
    assert dicts[0]["error"] is None


@pytest.mark.asyncio
async def test_step_logger_no_diagnostics_no_delta() -> None:
    """Without diagnostics, steps should have no delta."""
    logger = StepLogger()
    async with logger.step("no diag"):
        pass
    assert logger.steps[0].delta is None
    assert logger.steps[0].snapshot_before is None
    assert logger.steps[0].snapshot_after is None
