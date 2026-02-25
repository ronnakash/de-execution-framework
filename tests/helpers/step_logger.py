"""Step logger for recording named pipeline steps within tests.

Each step captures a before/after ``PipelineSnapshot`` from
``TestDiagnostics`` and computes deltas for Kafka topics, DB row counts,
and metrics.  Results are serialized into the HTML test report for
per-test drill-down.
"""

from __future__ import annotations

import base64
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from tests.helpers.diagnostics import TestDiagnostics

# ── Data model ───────────────────────────────────────────────────────────────


@dataclass
class StepRecord:
    """A single recorded step."""

    name: str
    description: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0
    snapshot_before: dict | None = None
    snapshot_after: dict | None = None
    delta: dict | None = None
    error: str | None = None
    screenshot: str | None = None

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "description": self.description,
            "duration_seconds": round(self.duration_seconds, 3),
            "delta": self.delta,
            "error": self.error,
        }
        if self.screenshot:
            d["screenshot"] = self.screenshot
        return d


# ── Snapshot conversion ──────────────────────────────────────────────────────


def _snapshot_to_dict(snap: Any) -> dict | None:
    """Convert a PipelineSnapshot to a JSON-serializable dict.

    Delegates to ``snapshot_to_dict`` in ``pytest_pipeline_report``.
    """
    from tests.helpers.pytest_pipeline_report import snapshot_to_dict

    return snapshot_to_dict(snap)


# ── Delta computation ────────────────────────────────────────────────────────


def _compute_step_delta(before: dict, after: dict) -> dict:
    """Compute per-topic, per-table, and per-metric deltas between snapshots.

    Only non-zero deltas are included.
    """
    delta: dict[str, dict[str, Any]] = {"kafka": {}, "db": {}, "metrics": {}}

    # Kafka topic message count increases (from kafka_topics.*.high field)
    before_kafka = before.get("kafka_topics", {})
    after_kafka = after.get("kafka_topics", {})
    for topic in set(before_kafka) | set(after_kafka):
        b = before_kafka.get(topic, {}).get("high", 0)
        a = after_kafka.get(topic, {}).get("high", 0)
        diff = a - b
        if diff != 0:
            delta["kafka"][topic] = diff

    # DB row count changes
    before_db = before.get("db_tables", {})
    after_db = after.get("db_tables", {})
    for table in set(before_db) | set(after_db):
        b = before_db.get(table, 0)
        a = after_db.get(table, 0)
        diff = a - b
        if diff != 0:
            delta["db"][table] = diff

    # Metric value changes
    before_metrics = before.get("metrics", {})
    after_metrics = after.get("metrics", {})
    for name in set(before_metrics) | set(after_metrics):
        b = before_metrics.get(name, 0)
        a = after_metrics.get(name, 0)
        diff = a - b
        if diff != 0:
            delta["metrics"][name] = diff

    return delta


# ── StepLogger ───────────────────────────────────────────────────────────────


class StepLogger:
    """Records named steps within a test for report drill-down."""

    def __init__(
        self,
        diagnostics: TestDiagnostics | None = None,
        page: Any = None,
    ) -> None:
        self._diagnostics = diagnostics
        self._page = page
        self._steps: list[StepRecord] = []

    @asynccontextmanager
    async def step(
        self,
        name: str,
        description: str = "",
        ui_step: bool = False,
        settle_ms: int = 0,
    ):
        """Context manager to record a named step.

        Args:
            name: Human-readable step name.
            description: Optional longer description.
            ui_step: When ``True``, capture a UI screenshot at the end of the
                step.  Defaults to ``False`` so non-UI steps never produce
                irrelevant screenshots.
            settle_ms: Milliseconds to wait before taking the *before*
                snapshot.  Gives async backends (e.g. ClickHouse) time to
                flush writes from the previous step so that deltas align
                with the step that caused them.
        """
        import asyncio

        record = StepRecord(name=name, description=description)
        record.start_time = time.time()

        # Take before snapshot (with optional settle delay)
        if self._diagnostics:
            if settle_ms > 0:
                await asyncio.sleep(settle_ms / 1000)
            try:
                record.snapshot_before = _snapshot_to_dict(
                    await self._diagnostics.snapshot()
                )
            except Exception:
                pass

        try:
            yield record
        except Exception as e:
            record.error = str(e)
            raise
        finally:
            record.end_time = time.time()
            record.duration_seconds = record.end_time - record.start_time

            # Take after snapshot
            if self._diagnostics:
                try:
                    record.snapshot_after = _snapshot_to_dict(
                        await self._diagnostics.snapshot()
                    )
                except Exception:
                    pass

            # Compute delta between before and after
            if record.snapshot_before and record.snapshot_after:
                record.delta = _compute_step_delta(
                    record.snapshot_before, record.snapshot_after
                )

            # Capture UI screenshot only when this is a UI step
            if ui_step and self._page is not None:
                try:
                    screenshot_bytes = self._page.screenshot(
                        type="jpeg", quality=80, full_page=True,
                    )
                    record.screenshot = base64.b64encode(
                        screenshot_bytes
                    ).decode("ascii")
                except Exception:
                    pass  # Never fail the test due to screenshots

            self._steps.append(record)

    def _capture_deferred_screenshot(self) -> None:
        """Capture a screenshot for the most recent step (deferred).

        When ``log()`` is called for step N+1, the page shows the result of
        step N's actions.  This method attaches the current page state to the
        *previous* step, giving accurate post-action screenshots.
        """
        if self._page is None or not self._steps:
            return
        last = self._steps[-1]
        if last.screenshot is not None:
            return
        try:
            screenshot_bytes = self._page.screenshot(
                type="jpeg", quality=80, full_page=True,
            )
            last.screenshot = base64.b64encode(screenshot_bytes).decode("ascii")
        except Exception:
            pass  # Never fail the test due to screenshots

    def log(self, name: str, description: str = "", ui_step: bool = False) -> None:
        """Record a simple step without snapshots (sync-friendly).

        Screenshots are *deferred*: calling ``log()`` captures a screenshot
        for the **previous** step (the page now shows its result).  Call
        ``finalize()`` after the last step to capture its screenshot.

        Only captures screenshots when ``ui_step=True``.
        """
        # Capture screenshot for the PREVIOUS step (page now shows its result)
        if ui_step:
            self._capture_deferred_screenshot()

        record = StepRecord(name=name, description=description)
        record.start_time = time.time()
        record.end_time = record.start_time
        self._steps.append(record)

    def finalize(self) -> None:
        """Capture a screenshot for the very last step.

        Must be called after the final ``log()`` action has been performed
        (typically in fixture teardown via ``yield``).
        """
        self._capture_deferred_screenshot()

    @property
    def steps(self) -> list[StepRecord]:
        return list(self._steps)

    def to_dicts(self) -> list[dict]:
        return [s.to_dict() for s in self._steps]
