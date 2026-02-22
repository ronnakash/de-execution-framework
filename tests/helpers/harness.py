"""PipelineHarness protocol and implementations.

Each harness provides a uniform interface for test scenarios to:
  - ingest events (REST / Kafka / Files)
  - wait for rows in data tables
  - wait for alerts
  - query the Data API
  - publish directly to the normalizer topic (for dedup tests)

Implementations:
  - MemoryHarness:      in-memory stubs, manual step-by-step pipeline driving
  - SharedPipeline:     session-scoped, starts 8 module subprocesses once
  - RealInfraHarness:   per-test harness backed by SharedPipeline
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import uuid
from typing import Any, Callable, Protocol

import aiohttp
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.algos.main import AlgosModule
from de_platform.modules.data_api.main import DataApiModule
from de_platform.modules.normalizer.main import NormalizerModule
from de_platform.modules.persistence.buffer import BufferKey
from de_platform.modules.persistence.main import PersistenceModule
from de_platform.pipeline.topics import (
    ALERTS,
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE,
)
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.memory_metrics import MemoryMetrics
from de_platform.services.metrics.noop_metrics import NoopMetrics

from tests.helpers.diagnostics import TestDiagnostics

from tests.helpers.ingestion import (
    ingest_files_memory,
    ingest_files_subprocess,
    ingest_kafka_publish,
    ingest_kafka_starter,
    ingest_rest_http,
    ingest_rest_inline,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def poll_until(
    predicate: Callable[[], bool],
    timeout: float = 30.0,
    interval: float = 0.1,
    on_timeout: Callable[[], str] | None = None,
) -> None:
    """Repeatedly call predicate() until it returns truthy or timeout expires."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            detail = on_timeout() if on_timeout else ""
            msg = f"poll_until: condition not met within {timeout}s"
            if detail:
                msg = f"{msg}\n\n{detail}"
            raise TimeoutError(msg)
        await asyncio.sleep(interval)


# ── Protocol ─────────────────────────────────────────────────────────────────


class PipelineHarness(Protocol):
    tenant_id: str

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None: ...

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 30.0
    ) -> list[dict]: ...

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 3.0
    ) -> None: ...

    async def fetch_alerts(self) -> list[dict]: ...

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 30.0
    ) -> list[dict]: ...

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]: ...

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None: ...


# ══════════════════════════════════════════════════════════════════════════════
# MemoryHarness — in-memory stubs, manual step-by-step
# ══════════════════════════════════════════════════════════════════════════════


class MemoryHarness:
    """In-memory harness with manual pipeline stepping.

    Each wait_for_* call internally drives the normalizer, persistence, and/or
    algos modules to process any pending messages.
    """

    def __init__(self, algos_config: dict[str, Any] | None = None) -> None:
        from de_platform.services.cache.memory_cache import MemoryCache
        from de_platform.services.database.memory_database import MemoryDatabase
        from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
        from de_platform.services.message_queue.memory_queue import MemoryQueue

        self.tenant_id: str = "acme"
        self.mq = MemoryQueue()
        self.cache = MemoryCache()
        self.fs = MemoryFileSystem()
        self.clickhouse_db = MemoryDatabase()
        self.alerts_db = MemoryDatabase()
        self._currency_db = MemoryDatabase()

        self.clickhouse_db.connect()
        self.alerts_db.connect()
        self._currency_db.connect()

        # Pre-seed currency rates in cache
        self.cache.set("currency_rate:EUR_USD", 1.10)
        self.cache.set("currency_rate:GBP_USD", 1.25)

        self._algos_config = algos_config or {}
        self._metrics = MemoryMetrics()
        self.normalizer: NormalizerModule | None = None
        self.persistence: PersistenceModule | None = None
        self.algos: AlgosModule | None = None
        self.diagnostics: TestDiagnostics | None = None

    async def _ensure_modules(self) -> None:
        if self.normalizer is not None:
            return

        logger = LoggerFactory(default_impl="memory")

        self.normalizer = NormalizerModule(
            config=ModuleConfig({}),
            logger=logger,
            mq=self.mq,
            cache=self.cache,
            db=self._currency_db,
            lifecycle=LifecycleManager(),
            metrics=self._metrics,
        )
        await self.normalizer.initialize()

        self.persistence = PersistenceModule(
            config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
            logger=logger,
            mq=self.mq,
            db=self.clickhouse_db,
            fs=self.fs,
            lifecycle=LifecycleManager(),
            metrics=self._metrics,
        )
        await self.persistence.initialize()

        self.algos = AlgosModule(
            config=ModuleConfig(self._algos_config),
            logger=logger,
            mq=self.mq,
            db=self.alerts_db,
            cache=self.cache,
            lifecycle=LifecycleManager(),
            metrics=self._metrics,
        )
        await self.algos.initialize()

        self.diagnostics = TestDiagnostics(
            memory_metrics=self._metrics,
            memory_queue=self.mq,
            clickhouse_db=self.clickhouse_db,
            postgres_db=self.alerts_db,
        )

    # ── Pipeline step helpers ────────────────────────────────────────────

    def _drain_normalizer(self) -> None:
        """Poll all normalizer topics until empty."""
        assert self.normalizer is not None
        from de_platform.pipeline.topics import (
            TRADE_NORMALIZATION,
            TX_NORMALIZATION,
        )

        for topic, category in [
            (TRADE_NORMALIZATION, "trade"),
            (TX_NORMALIZATION, "transaction"),
        ]:
            while self.mq._topics.get(topic):
                self.normalizer._poll_and_process(topic, category)

    def _drain_all_into_persistence(self) -> None:
        assert self.persistence is not None
        assert self.persistence.buffer is not None
        all_topic_tables = [
            (ORDERS_PERSISTENCE, "orders"),
            (EXECUTIONS_PERSISTENCE, "executions"),
            (TRANSACTIONS_PERSISTENCE, "transactions"),
            (DUPLICATES, "duplicates"),
            (NORMALIZATION_ERRORS, "normalization_errors"),
        ]
        for topic, table in all_topic_tables:
            while True:
                msg = self.mq.consume_one(topic)
                if msg is None:
                    break
                key = BufferKey(
                    tenant_id=msg.get("tenant_id", "unknown"),
                    table=table,
                )
                self.persistence.buffer.append(key, msg)
        self.persistence._flush_all()

    async def _drain_all_algos_topics(self) -> None:
        """Consume all messages from all algos topics and evaluate them."""
        assert self.algos is not None
        from de_platform.pipeline.topics import TRADES_ALGOS, TRANSACTIONS_ALGOS

        for topic in [TRADES_ALGOS, TRANSACTIONS_ALGOS]:
            while True:
                msg = self.mq.consume_one(topic)
                if msg is None:
                    break
                await self.algos._evaluate(msg)

    # ── Harness interface ────────────────────────────────────────────────

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None:
        await self._ensure_modules()

        if method == "rest":
            await ingest_rest_inline(self.mq, event_type, events)
        elif method == "kafka":
            await ingest_kafka_starter(self.mq, event_type, events)
        elif method == "files":
            await ingest_files_memory(self.fs, self.mq, event_type, events)
        else:
            raise ValueError(f"Unknown method: {method!r}")

    def _filter_by_tenant(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("tenant_id") == self.tenant_id]

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 30.0
    ) -> list[dict]:
        # Drive normalizer → persistence for all topics
        self._drain_normalizer()
        self._drain_all_into_persistence()
        return self._filter_by_tenant(
            self.clickhouse_db.fetch_all(f"SELECT * FROM {table}")
        )

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 3.0
    ) -> None:
        # Drive one more normalizer cycle to make sure nothing slips through
        self._drain_normalizer()
        self._drain_all_into_persistence()
        rows = self._filter_by_tenant(
            self.clickhouse_db.fetch_all(f"SELECT * FROM {table}")
        )
        if len(rows) != known:
            raise AssertionError(
                f"Expected {known} rows in {table}, got {len(rows)}"
            )

    async def fetch_alerts(self) -> list[dict]:
        # Drive pipeline first to make sure algos have processed everything
        self._drain_normalizer()
        await self._drain_all_algos_topics()
        return self._filter_by_tenant(
            self.alerts_db.fetch_all("SELECT * FROM alerts")
        )

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 30.0
    ) -> list[dict]:
        # Drive normalizer → algos
        self._drain_normalizer()
        await self._drain_all_algos_topics()
        return self._filter_by_tenant(
            self.alerts_db.fetch_all("SELECT * FROM alerts")
        )

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        db_factory = DatabaseFactory({})
        db_factory.register_instance("events", self.clickhouse_db)
        db_factory.register_instance("alerts", self.alerts_db)
        from de_platform.services.secrets.env_secrets import EnvSecrets

        api = DataApiModule(
            config=ModuleConfig({}),
            logger=LoggerFactory(default_impl="memory"),
            mq=self.mq,
            db_factory=db_factory,
            lifecycle=LifecycleManager(),
            metrics=NoopMetrics(),
            secrets=EnvSecrets(overrides={}),
        )
        await api.initialize()
        app = api._create_app()
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/v1/{endpoint}?{qs}")
            body = await resp.json()
            return resp.status, body

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None:
        await self._ensure_modules()
        self.mq.publish(topic, msg)

    async def consume_alert_topic(self) -> dict | None:
        return self.mq.consume_one(ALERTS)

    async def __aenter__(self) -> MemoryHarness:
        await self._ensure_modules()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        pass


def _launch_module(
    module_name: str,
    infra: Any,
    db_flags: list[str],
    extra_flags: list[str] | None = None,
    group_id: str = "test",
    health_port: int = 0,
    extra_env: dict[str, str] | None = None,
    log_dir: Path | None = None,
) -> subprocess.Popen:
    """Launch a module as an OS subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "de_platform",
        "run",
        module_name,
        *db_flags,
        "--mq",
        "kafka",
        "--cache",
        "redis",
        "--fs",
        "minio",
        "--log",
        "pretty",
        "--health-port",
        str(health_port),
        *(extra_flags or []),
    ]
    env = {**os.environ, **infra.to_env_overrides(group_id=group_id), **(extra_env or {})}
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        out = open(log_dir / f"{module_name}.log", "w")
        return subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT)
    return subprocess.Popen(
        cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


# ══════════════════════════════════════════════════════════════════════════════
# SharedPipeline — session-scoped, one pipeline instance for ALL tests
# ══════════════════════════════════════════════════════════════════════════════


import fcntl
import json as _json
import time
import urllib.error
import urllib.request
from pathlib import Path


def _wait_for_http_sync(url: str, timeout: float = 45.0) -> None:
    """Synchronous HTTP poll — used by SharedPipeline (no event loop)."""
    deadline = time.monotonic() + timeout
    last_error = "no connection attempted"
    while True:
        try:
            req = urllib.request.urlopen(url, timeout=5)
            if req.status < 500:
                return
            last_error = f"status={req.status}"
        except (urllib.error.URLError, OSError) as e:
            last_error = str(e)
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"HTTP service at {url} not ready within {timeout}s (last: {last_error})"
            )
        time.sleep(0.2)


class SharedPipeline:
    """Session-scoped pipeline that starts 8 module subprocesses once.

    Uses a file lock so only ONE xdist worker starts the pipeline;
    other workers connect to the same subprocesses.  The last worker
    to close kills the subprocesses.

    Fully synchronous — no event loop required during setup/teardown.
    ClickHouse and Kafka connections are sync and shared across tests.
    PostgresDatabase (asyncpg) is NOT managed here — each RealInfraHarness
    creates its own connection in the test's event loop.
    """

    def __init__(self, infra: Any, lock_dir: Path) -> None:
        self._infra = infra
        self._lock_dir = lock_dir
        self._lock_file = lock_dir / "pipeline.lock"
        self._info_file = lock_dir / "pipeline.json"
        self._counter_file = lock_dir / "pipeline_workers.count"
        self._procs: dict[str, subprocess.Popen] = {}
        self._is_owner = False

        self.rest_port: int = 0
        self.api_port: int = 0
        self.config_port: int = 0
        self.auth_port: int = 0
        self._health_ports: dict[str, int] = {}
        self._clickhouse_db: Any = None
        self._kafka_producer: Any = None
        self._minio_fs: Any = None
        self._bootstrap_servers: str = ""

    def start(self) -> None:
        """Start or connect to the shared pipeline (file-lock coordinated)."""
        with open(self._lock_file, "w") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                if self._info_file.exists():
                    info = _json.loads(self._info_file.read_text())
                    self.rest_port = info["rest_port"]
                    self.api_port = info["api_port"]
                    self.config_port = info.get("config_port", 0)
                    self.auth_port = info.get("auth_port", 0)
                    self._health_ports = info.get("health_ports", {})
                else:
                    self._start_subprocesses()
                    self._info_file.write_text(_json.dumps({
                        "rest_port": self.rest_port,
                        "api_port": self.api_port,
                        "config_port": self.config_port,
                        "auth_port": self.auth_port,
                        "health_ports": self._health_ports,
                        "pids": {n: p.pid for n, p in self._procs.items()},
                    }))
                    self._is_owner = True

                # Increment worker counter
                count = int(self._counter_file.read_text()) if self._counter_file.exists() else 0
                self._counter_file.write_text(str(count + 1))
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

        # All workers: connect sync services (ClickHouse, Kafka, MinIO)
        self._connect_services()

        # Wait for ALL module health endpoints (ensures modules have
        # initialized and connected to services before we start producing).
        for name, hp in self._health_ports.items():
            if hp:
                try:
                    _wait_for_http_sync(
                        f"http://127.0.0.1:{hp}/health/startup",
                        timeout=60.0,
                    )
                except TimeoutError:
                    crash = self._check_procs_alive() if self._procs else None
                    raise TimeoutError(
                        f"Module '{name}' health endpoint not ready within 60s. "
                        f"Subprocess status: {crash or 'running'}"
                    )

        if self._is_owner:
            self._wait_for_consumer_readiness()

    def _start_subprocesses(self) -> None:
        from de_platform.pipeline.topics import (
            DUPLICATES,
            EXECUTIONS_PERSISTENCE,
            NORMALIZATION_ERRORS,
            ORDERS_PERSISTENCE,
            TRADE_NORMALIZATION,
            TRADES_ALGOS,
            TRANSACTIONS_ALGOS,
            TRANSACTIONS_PERSISTENCE,
            TX_NORMALIZATION,
        )

        infra = self._infra
        self.rest_port = _free_port()
        self.api_port = _free_port()
        self.config_port = _free_port()
        self.auth_port = _free_port()

        # Per-module Kafka topic subscriptions: pre-subscribe all topics at
        # connect time to avoid incremental rebalance storms.
        _subscribe = {
            "kafka_starter": "client_orders,client_executions,client_transactions",
            "normalizer": f"{TRADE_NORMALIZATION},{TX_NORMALIZATION}",
            "persistence": ",".join([
                ORDERS_PERSISTENCE, EXECUTIONS_PERSISTENCE,
                TRANSACTIONS_PERSISTENCE, DUPLICATES, NORMALIZATION_ERRORS,
            ]),
            "algos": f"{TRADES_ALGOS},{TRANSACTIONS_ALGOS}",
        }

        _jwt_secret = "e2e-test-jwt-secret-at-least-32-bytes-long"

        module_specs: list[tuple[str, list[str], list[str]]] = [
            ("rest_starter", ["--mq", "kafka"], ["--port", str(self.rest_port)]),
            ("kafka_starter", ["--mq", "kafka"], []),
            ("normalizer", ["--db", "warehouse=postgres", "--cache", "redis", "--mq", "kafka"], []),
            ("persistence", ["--db", "clickhouse=clickhouse", "--fs", "minio", "--mq", "kafka"],
             ["--flush-threshold", "1", "--flush-interval", "0"]),
            ("algos", ["--db", "alerts=postgres", "--cache", "redis", "--mq", "kafka"],
             ["--suspicious-counterparty-ids", "bad-cp-1"]),
            ("data_api", ["--db", "events=clickhouse", "--db", "alerts=postgres", "--mq", "kafka"],
             ["--port", str(self.api_port)]),
            ("client_config", ["--db", "client_config=postgres", "--cache", "redis"],
             ["--port", str(self.config_port)]),
            ("auth", ["--db", "auth=postgres"],
             ["--port", str(self.auth_port)]),
        ]

        # Per-module extra env vars (JWT_SECRET only for auth module)
        _module_env_overrides: dict[str, dict[str, str]] = {
            "auth": {"JWT_SECRET": _jwt_secret},
        }

        for module_name, db_flags, extra_flags in module_specs:
            hp = _free_port()
            self._health_ports[module_name] = hp
            all_flags = db_flags + extra_flags
            # Each module gets its own consumer group to avoid cross-module
            # rebalance storms (each module subscribes to different topics;
            # sharing a group causes cascading rebalances as topics are added).
            module_env = dict(_module_env_overrides.get(module_name, {}))
            if module_name in _subscribe:
                module_env["MQ_KAFKA_SUBSCRIBE_TOPICS"] = _subscribe[module_name]
            self._procs[module_name] = _launch_module(
                module_name=module_name,
                infra=infra,
                db_flags=[],
                extra_flags=all_flags,
                group_id=f"e2e-{module_name}",
                health_port=hp,
                extra_env=module_env,
                log_dir=self._lock_dir / "logs",
            )

    def _connect_services(self) -> None:
        """Connect sync-only services (ClickHouse, Kafka, MinIO).

        PostgresDatabase (async) is NOT connected here — each RealInfraHarness
        creates its own asyncpg connection in the test's event loop.
        """
        from de_platform.services.database.clickhouse_database import ClickHouseDatabase
        from de_platform.services.filesystem.minio_filesystem import MinioFileSystem
        from de_platform.services.message_queue.kafka_queue import KafkaQueue
        from de_platform.services.secrets.env_secrets import EnvSecrets

        infra = self._infra
        secrets = EnvSecrets(
            overrides=infra.to_env_overrides(group_id=f"assert-{uuid.uuid4().hex[:8]}")
        )

        ch_db = ClickHouseDatabase(secrets=secrets)
        ch_db.connect()
        self._clickhouse_db = ch_db

        producer = KafkaQueue(secrets)
        producer.connect()
        self._kafka_producer = producer

        self._minio_fs = MinioFileSystem(secrets=secrets)
        self._bootstrap_servers = infra.kafka_bootstrap_servers

    def _check_procs_alive(self) -> str | None:
        """Return error string if any subprocess has exited, else None."""
        for name, proc in self._procs.items():
            if proc.poll() is not None:
                stderr = ""
                try:
                    _, stderr_bytes = proc.communicate(timeout=1)
                    stderr = stderr_bytes.decode()[:1000]
                except Exception:
                    pass
                return f"Module '{name}' exited with code {proc.returncode}: {stderr}"
        return None

    def _wait_for_consumer_readiness(self) -> None:
        """Produce sentinel events until one appears in ClickHouse (sync)."""
        from de_platform.pipeline.topics import TRADE_NORMALIZATION

        deadline = time.monotonic() + 45.0
        seen_ids: list[str] = []

        while time.monotonic() < deadline:
            # Check for crashed subprocesses
            crash = self._check_procs_alive()
            if crash:
                raise RuntimeError(f"Pipeline subprocess crashed during readiness check: {crash}")

            sentinel_id = f"sentinel-{uuid.uuid4().hex[:8]}"
            seen_ids.append(sentinel_id)
            sentinel = {
                "id": sentinel_id,
                "tenant_id": "PIPELINE_SENTINEL",
                "status": "new",
                "transact_time": "2026-01-01T00:00:00+00:00",
                "symbol": "SENTINEL",
                "side": "buy",
                "quantity": 1.0,
                "price": 1.0,
                "order_type": "limit",
                "currency": "USD",
                "event_type": "order",
                "message_id": uuid.uuid4().hex,
                "ingested_at": "2026-01-01T00:00:00+00:00",
            }
            self._kafka_producer.publish(TRADE_NORMALIZATION, sentinel)

            inner_deadline = time.monotonic() + 3.0
            while time.monotonic() < inner_deadline:
                rows = self._clickhouse_db.fetch_all("SELECT * FROM orders")
                if any(r.get("id") in seen_ids for r in rows):
                    return
                time.sleep(0.3)

        # Timeout — gather diagnostic info
        crash = self._check_procs_alive()
        proc_info = crash or "all subprocesses still running"
        raise TimeoutError(
            f"Consumer readiness check failed: sentinel events did not appear "
            f"in ClickHouse within 45s. Subprocess status: {proc_info}"
        )

    def close(self) -> None:
        """Close connections and (if last worker) kill subprocesses."""
        self._kafka_producer.disconnect()
        self._clickhouse_db.disconnect()

        should_kill = False
        with open(self._lock_file, "w") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                count = int(self._counter_file.read_text()) if self._counter_file.exists() else 1
                count -= 1
                self._counter_file.write_text(str(count))
                if count <= 0:
                    should_kill = True
                    self._info_file.unlink(missing_ok=True)
                    self._counter_file.unlink(missing_ok=True)
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

        if should_kill:
            if self._procs:
                for proc in self._procs.values():
                    try:
                        proc.send_signal(signal.SIGTERM)
                    except OSError:
                        pass
                for proc in self._procs.values():
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
            else:
                # Non-owner worker: kill by stored PIDs
                try:
                    info = _json.loads((self._lock_dir / "pipeline.json").read_text())
                    for pid in info.get("pids", {}).values():
                        try:
                            os.kill(pid, signal.SIGTERM)
                        except OSError:
                            pass
                except Exception:
                    pass


# ══════════════════════════════════════════════════════════════════════════════
# RealInfraHarness — lightweight per-test harness using SharedPipeline
# ══════════════════════════════════════════════════════════════════════════════


class RealInfraHarness:
    """Per-test harness backed by a SharedPipeline.

    Each test gets a unique tenant_id.  All ingestion and assertion
    methods filter by this tenant_id so tests are data-isolated.

    Manages its own async PostgresDatabase connection (created in __aenter__,
    closed in __aexit__) since asyncpg connections are tied to event loops.
    Shares the pipeline's sync ClickHouse and Kafka connections.
    """

    def __init__(self, pipeline: SharedPipeline) -> None:
        self._pipeline = pipeline
        self.tenant_id: str = f"INTEGRATION_CLIENT_{uuid.uuid4().hex[:12]}"
        self._alerts_db: Any = None
        self.diagnostics: TestDiagnostics | None = None

    async def __aenter__(self) -> RealInfraHarness:
        from de_platform.services.database.postgres_database import PostgresDatabase
        from de_platform.services.secrets.env_secrets import EnvSecrets

        secrets = EnvSecrets(
            overrides=self._pipeline._infra.to_env_overrides(
                group_id=f"assert-{uuid.uuid4().hex[:8]}"
            )
        )
        alerts_db = PostgresDatabase(secrets=secrets, prefix="DB_ALERTS")
        await alerts_db.connect_async()
        self._alerts_db = alerts_db

        self.diagnostics = TestDiagnostics(
            kafka_bootstrap=self._pipeline._bootstrap_servers,
            clickhouse_db=self._pipeline._clickhouse_db,
            postgres_db=self._alerts_db,
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._alerts_db:
            await self._alerts_db.disconnect_async()

    def _filter_by_tenant(self, rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("tenant_id") == self.tenant_id]

    def _snapshot_text(self) -> str:
        if self.diagnostics is None:
            return ""
        try:
            snap = self.diagnostics.snapshot()
            return TestDiagnostics.format_snapshot(snap)
        except Exception as e:
            return f"(diagnostics failed: {e})"

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None:
        p = self._pipeline
        if method == "rest":
            await ingest_rest_http(p.rest_port, event_type, events)
        elif method == "kafka":
            ingest_kafka_publish(p._kafka_producer, event_type, events)
        elif method == "files":
            ingest_files_subprocess(
                p._minio_fs, p._infra, event_type, events, _launch_module,
            )
        else:
            raise ValueError(f"Unknown method: {method!r}")

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 60.0
    ) -> list[dict]:
        ch = self._pipeline._clickhouse_db

        def _check() -> bool:
            rows = ch.fetch_all(f"SELECT * FROM {table}")
            return len(self._filter_by_tenant(rows)) >= expected

        def _on_timeout() -> str:
            rows = ch.fetch_all(f"SELECT * FROM {table}")
            filtered = self._filter_by_tenant(rows)
            return (
                f"wait_for_rows('{table}', expected={expected}) "
                f"timed out after {timeout}s (got {len(filtered)} "
                f"for tenant {self.tenant_id})\n\n{self._snapshot_text()}"
            )

        await poll_until(_check, timeout=timeout, on_timeout=_on_timeout)
        return self._filter_by_tenant(
            ch.fetch_all(f"SELECT * FROM {table}")
        )

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 5.0
    ) -> None:
        await asyncio.sleep(timeout)
        rows = self._filter_by_tenant(
            self._pipeline._clickhouse_db.fetch_all(f"SELECT * FROM {table}")
        )
        if len(rows) != known:
            raise AssertionError(
                f"Expected {known} rows in {table} to remain unchanged, "
                f"got {len(rows)}\n\n{self._snapshot_text()}"
            )

    async def fetch_alerts(self) -> list[dict]:
        rows = await self._alerts_db.fetch_all_async("SELECT * FROM alerts")
        return self._filter_by_tenant(rows)

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 60.0
    ) -> list[dict]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            rows = await self._alerts_db.fetch_all_async("SELECT * FROM alerts")
            rows = self._filter_by_tenant(rows)
            if any(predicate(r) for r in rows):
                return rows
            if loop.time() > deadline:
                raise TimeoutError(
                    f"wait_for_alert: condition not met within {timeout}s"
                    f"\n\n{self._snapshot_text()}"
                )
            await asyncio.sleep(0.1)

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"http://127.0.0.1:{self._pipeline.api_port}/api/v1/{endpoint}?{qs}"
        async with aiohttp.ClientSession() as session:
            resp = await session.get(url)
            body = await resp.json()
            return resp.status, body

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None:
        self._pipeline._kafka_producer.publish(topic, msg)
