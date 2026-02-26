"""Tests for StepLogger ui_step and settle_ms improvements."""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.helpers.step_logger import StepLogger


async def test_step_no_screenshot_by_default():
    """When ui_step is False (default), no screenshot is captured."""
    mock_page = MagicMock()
    mock_page.screenshot.return_value = b"fake-screenshot-data"
    logger = StepLogger(page=mock_page)

    async with logger.step("non-ui step"):
        pass

    assert len(logger.steps) == 1
    assert logger.steps[0].screenshot is None
    mock_page.screenshot.assert_not_called()


async def test_step_screenshot_when_ui_step():
    """When ui_step=True, a screenshot IS captured."""
    mock_page = MagicMock()
    mock_page.screenshot.return_value = b"fake-screenshot-data"
    logger = StepLogger(page=mock_page)

    async with logger.step("ui step", ui_step=True):
        pass

    assert len(logger.steps) == 1
    assert logger.steps[0].screenshot is not None
    mock_page.screenshot.assert_called_once()


async def test_step_settle_ms():
    """settle_ms parameter causes a delay before the before-snapshot.

    The settle delay only fires when diagnostics is provided (so snapshots
    can see settled writes).
    """
    import time
    from unittest.mock import AsyncMock, MagicMock

    mock_diag = MagicMock()
    mock_diag.snapshot = AsyncMock(return_value=MagicMock(
        kafka_topics={}, db_tables={}, module_status={}, metrics={},
        timestamp=0,
    ))
    logger = StepLogger(diagnostics=mock_diag)

    start = time.monotonic()
    async with logger.step("settle step", settle_ms=200):
        pass
    elapsed = time.monotonic() - start

    # Should have waited at least ~200ms (allow some slack)
    assert elapsed >= 0.15  # 150ms to account for timing imprecision


async def test_step_settle_ms_zero():
    """settle_ms=0 (default) adds no delay even with diagnostics."""
    import time
    from unittest.mock import AsyncMock, MagicMock

    mock_diag = MagicMock()
    mock_diag.snapshot = AsyncMock(return_value=MagicMock(
        kafka_topics={}, db_tables={}, module_status={}, metrics={},
        timestamp=0,
    ))
    logger = StepLogger(diagnostics=mock_diag)

    start = time.monotonic()
    async with logger.step("no settle"):
        pass
    elapsed = time.monotonic() - start

    # Should complete quickly (no settle delay)
    assert elapsed < 1.0


def test_log_no_screenshot_by_default():
    """log() with ui_step=False (default) does not capture screenshot."""
    mock_page = MagicMock()
    mock_page.screenshot.return_value = b"fake-data"
    logger = StepLogger(page=mock_page)

    # Add an initial step without screenshot
    logger.log("step 1")
    logger.log("step 2")  # ui_step=False, so no deferred capture

    assert len(logger.steps) == 2
    mock_page.screenshot.assert_not_called()


def test_log_screenshot_when_ui_step():
    """log() with ui_step=True captures deferred screenshot."""
    mock_page = MagicMock()
    mock_page.screenshot.return_value = b"fake-data"
    logger = StepLogger(page=mock_page)

    logger.log("step 1")
    logger.log("step 2", ui_step=True)  # Captures screenshot for step 1

    assert len(logger.steps) == 2
    mock_page.screenshot.assert_called_once()


async def test_step_without_page_no_error():
    """step() with ui_step=True but no page does not raise."""
    logger = StepLogger()

    async with logger.step("no page step", ui_step=True):
        pass

    assert len(logger.steps) == 1
    assert logger.steps[0].screenshot is None
