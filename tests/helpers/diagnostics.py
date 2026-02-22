"""TestDiagnostics — unified pipeline state inspection for test debugging.

Provides a ``PipelineSnapshot`` with Kafka topic watermarks, DB row counts,
module health, and metrics values.  Works with both in-process (MemoryMetrics,
asyncio.Task) and subprocess (Prometheus scraping, subprocess.Popen) setups.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any


# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class TopicState:
    """State of a single Kafka topic partition."""

    high_watermark: int
    low_watermark: int

    @property
    def messages_available(self) -> int:
        return self.high_watermark - self.low_watermark


@dataclass
class ModuleStatus:
    """Health of a running module (task or process)."""

    name: str
    alive: bool
    exit_code: int | None = None
    pid: int | None = None
    stderr_tail: str | None = None


@dataclass
class PipelineSnapshot:
    """Point-in-time snapshot of the full pipeline state."""

    timestamp: float
    kafka_topics: dict[str, TopicState] = field(default_factory=dict)
    db_tables: dict[str, int] = field(default_factory=dict)
    module_status: list[ModuleStatus] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)


# ── TestDiagnostics ──────────────────────────────────────────────────────────


class TestDiagnostics:
    """Collects pipeline state for test debugging.

    Supports both in-memory (MemoryHarness) and real-infra
    (SharedPipeline / RealInfraHarness) setups.
    """

    def __init__(
        self,
        *,
        kafka_bootstrap: str | None = None,
        clickhouse_db: Any | None = None,
        postgres_db: Any | None = None,
        memory_metrics: Any | None = None,
        prometheus_endpoints: dict[str, str] | None = None,
        processes: dict[str, subprocess.Popen] | None = None,
        tasks: list[asyncio.Task] | None = None,
        task_names: list[str] | None = None,
        memory_queue: Any | None = None,
    ) -> None:
        self._kafka_bootstrap = kafka_bootstrap
        self._clickhouse_db = clickhouse_db
        self._postgres_db = postgres_db
        self._memory_metrics = memory_metrics
        self._prometheus_endpoints = prometheus_endpoints or {}
        self._processes = processes or {}
        self._tasks = tasks or []
        self._task_names = task_names or []
        self._memory_queue = memory_queue

    def snapshot(self) -> PipelineSnapshot:
        """Take a point-in-time snapshot of all pipeline state."""
        snap = PipelineSnapshot(timestamp=time.time())

        # Kafka / message queue state
        if self._memory_queue is not None:
            snap.kafka_topics = self._memory_queue_state()
        elif self._kafka_bootstrap:
            snap.kafka_topics = self._kafka_watermarks()

        # DB row counts
        snap.db_tables = self._db_row_counts()

        # Module health
        snap.module_status = self._module_health()

        # Metrics
        if self._memory_metrics is not None:
            snap.metrics = self._read_memory_metrics()
        elif self._prometheus_endpoints:
            for name, endpoint in self._prometheus_endpoints.items():
                for k, v in self._scrape_prometheus(endpoint).items():
                    snap.metrics[f"{name}.{k}"] = v

        return snap

    # ── Kafka / MemoryQueue ──────────────────────────────────────────────

    def _memory_queue_state(self) -> dict[str, TopicState]:
        """Read topic state from MemoryQueue."""
        result: dict[str, TopicState] = {}
        mq = self._memory_queue
        if hasattr(mq, "_topics"):
            for topic, messages in mq._topics.items():
                result[topic] = TopicState(
                    high_watermark=len(messages),
                    low_watermark=0,
                )
        return result

    def _kafka_watermarks(self) -> dict[str, TopicState]:
        """Query Kafka topic watermarks via AdminClient."""
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient({"bootstrap.servers": self._kafka_bootstrap})
            metadata = admin.list_topics(timeout=5)

            result: dict[str, TopicState] = {}
            for topic_name in metadata.topics:
                if topic_name.startswith("_"):
                    continue
                # Get watermarks for partition 0
                try:
                    from confluent_kafka import Consumer

                    c = Consumer({
                        "bootstrap.servers": self._kafka_bootstrap,
                        "group.id": "__diagnostics__",
                        "enable.auto.commit": "false",
                    })
                    from confluent_kafka import TopicPartition

                    tp = TopicPartition(topic_name, 0)
                    low, high = c.get_watermark_offsets(tp, timeout=2)
                    result[topic_name] = TopicState(
                        high_watermark=high, low_watermark=low
                    )
                    c.close()
                except Exception:
                    result[topic_name] = TopicState(
                        high_watermark=-1, low_watermark=-1
                    )
            return result
        except Exception:
            return {}

    # ── Database ─────────────────────────────────────────────────────────

    def _db_row_counts(self) -> dict[str, int]:
        """Query row counts from databases."""
        counts: dict[str, int] = {}

        tables = [
            "orders",
            "executions",
            "transactions",
            "duplicates",
            "normalization_errors",
            "alerts",
        ]

        if self._clickhouse_db is not None:
            for table in tables[:5]:  # event tables in ClickHouse
                try:
                    rows = self._clickhouse_db.fetch_all(
                        f"SELECT * FROM {table}"
                    )
                    counts[f"clickhouse.{table}"] = len(rows)
                except Exception:
                    counts[f"clickhouse.{table}"] = -1

        if self._postgres_db is not None:
            try:
                rows = self._postgres_db.fetch_all(
                    "SELECT * FROM alerts"
                )
                counts["postgres.alerts"] = len(rows)
            except Exception:
                counts["postgres.alerts"] = -1

        return counts

    # ── Module health ────────────────────────────────────────────────────

    def _module_health(self) -> list[ModuleStatus]:
        """Check if each module process/task is alive."""
        statuses: list[ModuleStatus] = []

        # asyncio.Task-based modules
        for task, name in zip(self._tasks, self._task_names):
            if task.done():
                exc = task.exception() if not task.cancelled() else None
                statuses.append(ModuleStatus(
                    name=name,
                    alive=False,
                    exit_code=1 if exc else 0,
                    stderr_tail=str(exc)[:500] if exc else None,
                ))
            else:
                statuses.append(ModuleStatus(name=name, alive=True))

        # subprocess.Popen-based modules
        for name, proc in self._processes.items():
            poll = proc.poll()
            if poll is not None:
                stderr_tail = None
                try:
                    _, stderr = proc.communicate(timeout=1)
                    stderr_tail = stderr.decode()[-500:] if stderr else None
                except Exception:
                    pass
                statuses.append(ModuleStatus(
                    name=name,
                    alive=False,
                    exit_code=poll,
                    pid=proc.pid,
                    stderr_tail=stderr_tail,
                ))
            else:
                statuses.append(ModuleStatus(
                    name=name, alive=True, pid=proc.pid
                ))

        return statuses

    # ── Metrics ──────────────────────────────────────────────────────────

    def _read_memory_metrics(self) -> dict[str, float]:
        """Read all counters and gauges from MemoryMetrics."""
        m = self._memory_metrics
        result: dict[str, float] = {}
        if hasattr(m, "counters"):
            result.update(m.counters)
        if hasattr(m, "gauges"):
            result.update(m.gauges)
        return result

    def _scrape_prometheus(self, endpoint: str) -> dict[str, float]:
        """Scrape a Prometheus /metrics endpoint and parse into dict."""
        try:
            import urllib.request

            resp = urllib.request.urlopen(endpoint, timeout=3)
            body = resp.read().decode()
            result: dict[str, float] = {}
            for line in body.splitlines():
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        result[parts[0]] = float(parts[1])
                    except ValueError:
                        pass
            return result
        except Exception:
            return {}

    # ── Formatting ───────────────────────────────────────────────────────

    @staticmethod
    def format_snapshot(snap: PipelineSnapshot) -> str:
        """Format snapshot as human-readable text for terminal output."""
        lines: list[str] = []
        lines.append("Pipeline state snapshot:")

        if snap.kafka_topics:
            lines.append("  Kafka/MQ topics:")
            for topic, state in sorted(snap.kafka_topics.items()):
                lines.append(
                    f"    {topic:40s} {state.messages_available:>4d} msgs "
                    f"(high={state.high_watermark}, low={state.low_watermark})"
                )

        if snap.db_tables:
            lines.append("  DB tables:")
            for table, count in sorted(snap.db_tables.items()):
                lines.append(f"    {table:40s} {count:>6d} rows")

        if snap.module_status:
            lines.append("  Module status:")
            for m in snap.module_status:
                status = "alive" if m.alive else f"DEAD (exit={m.exit_code})"
                pid = f" pid={m.pid}" if m.pid else ""
                lines.append(f"    {m.name:20s} {status}{pid}")
                if m.stderr_tail:
                    for stderr_line in m.stderr_tail.splitlines()[:3]:
                        lines.append(f"      stderr: {stderr_line}")

        if snap.metrics:
            lines.append("  Metrics:")
            for name, value in sorted(snap.metrics.items()):
                lines.append(f"    {name:40s} {value:>10.1f}")

        return "\n".join(lines)
