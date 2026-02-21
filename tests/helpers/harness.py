"""PipelineHarness protocol and three implementations.

Each harness provides a uniform interface for test scenarios to:
  - ingest events (REST / Kafka / Files)
  - wait for rows in data tables
  - wait for alerts
  - query the Data API
  - publish directly to the normalizer topic (for dedup tests)

Three implementations:
  - MemoryHarness:     in-memory stubs, manual step-by-step pipeline driving
  - ContainerHarness:  real infra (docker-compose), 6 asyncio tasks in-process
  - SubprocessHarness: real infra, 6 OS subprocesses
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

from tests.helpers.events import (
    ALGOS_TOPIC,
    CATEGORY,
    CLIENT_TOPIC,
    EVENT_TABLE,
    NORM_TOPIC,
    REST_ENDPOINT,
)
from tests.helpers.ingestion import (
    ingest_files_memory,
    ingest_files_minio_with_kafka,
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
) -> None:
    """Repeatedly call predicate() until it returns truthy or timeout expires."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise TimeoutError("poll_until: condition not met within timeout")
        await asyncio.sleep(interval)


async def _wait_for_http(url: str, timeout: float = 15.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    last_error: str = "no connection attempted"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status < 500:
                        return
                    body = await resp.text()
                    last_error = f"status={resp.status} body={body[:200]}"
        except (aiohttp.ClientConnectorError, aiohttp.ServerConnectionError, OSError) as e:
            last_error = f"connection error: {e}"
        if loop.time() > deadline:
            raise TimeoutError(
                f"HTTP service at {url} not ready within {timeout}s (last: {last_error})"
            )
        await asyncio.sleep(0.1)


# ── Protocol ─────────────────────────────────────────────────────────────────


class PipelineHarness(Protocol):
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
        self.normalizer: NormalizerModule | None = None
        self.persistence: PersistenceModule | None = None
        self.algos: AlgosModule | None = None

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
        )
        await self.normalizer.initialize()

        self.persistence = PersistenceModule(
            config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
            logger=logger,
            mq=self.mq,
            db=self.clickhouse_db,
            fs=self.fs,
            lifecycle=LifecycleManager(),
        )
        await self.persistence.initialize()

        self.algos = AlgosModule(
            config=ModuleConfig(self._algos_config),
            logger=logger,
            mq=self.mq,
            db=self.alerts_db,
            cache=self.cache,
            lifecycle=LifecycleManager(),
        )
        await self.algos.initialize()

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
            ingest_files_memory(self.fs, self.mq, event_type, events)
        else:
            raise ValueError(f"Unknown method: {method!r}")

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 30.0
    ) -> list[dict]:
        # Drive normalizer → persistence for all topics
        self._drain_normalizer()
        self._drain_all_into_persistence()
        return self.clickhouse_db.fetch_all(f"SELECT * FROM {table}")

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 3.0
    ) -> None:
        # Drive one more normalizer cycle to make sure nothing slips through
        self._drain_normalizer()
        self._drain_all_into_persistence()
        rows = self.clickhouse_db.fetch_all(f"SELECT * FROM {table}")
        if len(rows) != known:
            raise AssertionError(
                f"Expected {known} rows in {table}, got {len(rows)}"
            )

    async def fetch_alerts(self) -> list[dict]:
        # Drive pipeline first to make sure algos have processed everything
        self._drain_normalizer()
        await self._drain_all_algos_topics()
        return self.alerts_db.fetch_all("SELECT * FROM alerts")

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 30.0
    ) -> list[dict]:
        # Drive normalizer → algos
        self._drain_normalizer()
        await self._drain_all_algos_topics()
        return self.alerts_db.fetch_all("SELECT * FROM alerts")

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        db_factory = DatabaseFactory({})
        db_factory.register_instance("events", self.clickhouse_db)
        db_factory.register_instance("alerts", self.alerts_db)
        api = DataApiModule(
            config=ModuleConfig({}),
            logger=LoggerFactory(default_impl="memory"),
            mq=self.mq,
            db_factory=db_factory,
            lifecycle=LifecycleManager(),
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


# ══════════════════════════════════════════════════════════════════════════════
# ContainerHarness — real infra (docker-compose), 6 asyncio tasks in-process
# ══════════════════════════════════════════════════════════════════════════════


class ContainerHarness:
    """Real-infra harness with all 6 modules running as asyncio tasks."""

    def __init__(
        self,
        infra: Any,
        algos_config: dict[str, Any] | None = None,
    ) -> None:
        self._infra = infra
        self._algos_config = algos_config or {}
        # Populated in __aenter__
        self._clickhouse_db: Any = None
        self._alerts_db: Any = None
        self._kafka_producer: Any = None
        self._minio_fs: Any = None
        self._bootstrap_servers: str = ""
        self.rest_port: int = 0
        self.api_port: int = 0
        self._tasks: list[asyncio.Task] = []
        self._lifecycles: list[LifecycleManager] = []
        self._mqs: list[Any] = []

    async def _setup(self) -> None:
        from de_platform.modules.kafka_starter.main import KafkaStarterModule
        from de_platform.modules.rest_starter.main import RestStarterModule
        from de_platform.services.cache.redis_cache import RedisCache
        from de_platform.services.database.clickhouse_database import ClickHouseDatabase
        from de_platform.services.database.postgres_database import PostgresDatabase
        from de_platform.services.filesystem.minio_filesystem import MinioFileSystem
        from de_platform.services.message_queue.kafka_queue import KafkaQueue
        from de_platform.services.secrets.env_secrets import EnvSecrets

        infra = self._infra
        self.rest_port = _free_port()
        self.api_port = _free_port()
        group_base = uuid.uuid4().hex[:8]

        def _make_secrets(suffix: str) -> EnvSecrets:
            overrides = infra.to_env_overrides(
                group_id=f"inproc-{group_base}-{suffix}"
            )
            # Very short poll timeout: (a) prevents blocking consume_one()
            # from starving the event loop when modules share one asyncio
            # loop, and (b) reduces idle wait when polling topics with no
            # messages (4 empty polls per persistence iteration at 50ms
            # each = 200ms overhead; at 10ms = 40ms overhead).
            overrides["MQ_KAFKA_POLL_TIMEOUT"] = "0.01"
            return EnvSecrets(overrides=overrides)

        logger = LoggerFactory(default_impl="memory")
        self._lifecycles = [LifecycleManager() for _ in range(6)]
        lc_rest, lc_kafka, lc_norm, lc_pers, lc_algos, lc_api = self._lifecycles

        # Per-module KafkaQueue instances
        mq_names = ["rest", "kafka", "norm", "pers", "algos", "api"]
        self._mqs = []
        for name in mq_names:
            mq = KafkaQueue(_make_secrets(name))
            mq.connect()
            self._mqs.append(mq)
        mq_rest, mq_kafka, mq_norm, mq_pers, mq_algos, mq_api = self._mqs

        # Shared service connections
        warehouse_db = PostgresDatabase(
            secrets=_make_secrets("warehouse"), prefix="DB_WAREHOUSE"
        )
        await warehouse_db.connect_async()
        self._warehouse_db = warehouse_db

        alerts_db = PostgresDatabase(
            secrets=_make_secrets("alerts"), prefix="DB_ALERTS"
        )
        await alerts_db.connect_async()
        self._alerts_db = alerts_db

        ch_db = ClickHouseDatabase(secrets=_make_secrets("ch"))
        ch_db.connect()
        self._clickhouse_db = ch_db

        cache = RedisCache(secrets=_make_secrets("cache"))
        cache.connect()
        self._cache = cache

        fs = MinioFileSystem(secrets=_make_secrets("fs"))
        self._minio_fs = fs

        self._bootstrap_servers = infra.kafka_bootstrap_servers

        # DataApi DatabaseFactory
        api_db_factory = DatabaseFactory({})
        api_db_factory.register_instance("events", ch_db)
        api_db_factory.register_instance("alerts", alerts_db)

        modules: list[Any] = [
            RestStarterModule(
                config=ModuleConfig({"port": self.rest_port}),
                logger=logger,
                mq=mq_rest,
                lifecycle=lc_rest,
            ),
            KafkaStarterModule(
                config=ModuleConfig({}),
                logger=logger,
                mq=mq_kafka,
                lifecycle=lc_kafka,
            ),
            NormalizerModule(
                config=ModuleConfig({}),
                logger=logger,
                mq=mq_norm,
                cache=cache,
                db=warehouse_db,
                lifecycle=lc_norm,
            ),
            PersistenceModule(
                config=ModuleConfig({"flush-threshold": 1, "flush-interval": 0}),
                logger=logger,
                mq=mq_pers,
                db=ch_db,
                fs=fs,
                lifecycle=lc_pers,
            ),
            AlgosModule(
                config=ModuleConfig(self._algos_config),
                logger=logger,
                mq=mq_algos,
                db=alerts_db,
                cache=cache,
                lifecycle=lc_algos,
            ),
            DataApiModule(
                config=ModuleConfig({"port": self.api_port}),
                logger=logger,
                mq=mq_api,
                db_factory=api_db_factory,
                lifecycle=lc_api,
            ),
        ]

        names = ["rest_starter", "kafka_starter", "normalizer", "persistence", "algos", "data_api"]
        self._tasks = [
            asyncio.create_task(m.run(), name=n) for m, n in zip(modules, names)
        ]

        for url in [
            f"http://127.0.0.1:{self.rest_port}/api/v1/health",
            f"http://127.0.0.1:{self.api_port}/api/v1/alerts",
        ]:
            try:
                await _wait_for_http(url, timeout=30.0)
            except TimeoutError:
                # Surface errors from crashed tasks
                for t in self._tasks:
                    if t.done() and t.exception():
                        raise RuntimeError(
                            f"Module task '{t.get_name()}' crashed: {t.exception()}"
                        ) from t.exception()
                raise

        # Producer for Kafka ingestion
        producer_mq = KafkaQueue(_make_secrets("producer"))
        producer_mq.connect()
        self._kafka_producer = producer_mq
        self._mqs.append(producer_mq)

    async def _teardown(self) -> None:
        for lc in self._lifecycles:
            await lc.shutdown()
        done, pending = await asyncio.wait(self._tasks, timeout=10.0)
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        for mq in self._mqs:
            mq.disconnect()
        await self._warehouse_db.disconnect_async()
        await self._alerts_db.disconnect_async()
        self._clickhouse_db.disconnect()
        self._cache.disconnect()

    # ── Harness interface ────────────────────────────────────────────────

    def _check_tasks_alive(self) -> None:
        """Raise immediately if any module task has crashed."""
        for t in self._tasks:
            if t.done() and t.exception():
                raise RuntimeError(
                    f"Module task '{t.get_name()}' crashed: {t.exception()}"
                ) from t.exception()

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None:
        if method == "rest":
            await ingest_rest_http(self.rest_port, event_type, events)
        elif method == "kafka":
            ingest_kafka_publish(self._kafka_producer, event_type, events)
        elif method == "files":
            ingest_files_minio_with_kafka(
                self._minio_fs,
                self._kafka_producer,
                event_type,
                events,
                self._bootstrap_servers,
            )
        else:
            raise ValueError(f"Unknown method: {method!r}")

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 60.0
    ) -> list[dict]:
        def _check() -> bool:
            self._check_tasks_alive()
            return (
                len(self._clickhouse_db.fetch_all(f"SELECT * FROM {table}"))
                >= expected
            )

        await poll_until(_check, timeout=timeout)
        return self._clickhouse_db.fetch_all(f"SELECT * FROM {table}")

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 5.0
    ) -> None:
        await asyncio.sleep(timeout)
        self._check_tasks_alive()
        rows = self._clickhouse_db.fetch_all(f"SELECT * FROM {table}")
        if len(rows) != known:
            raise AssertionError(
                f"Expected {known} rows in {table} to remain unchanged, got {len(rows)}"
            )

    async def fetch_alerts(self) -> list[dict]:
        self._check_tasks_alive()
        return await self._alerts_db.fetch_all_async("SELECT * FROM alerts")

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 60.0
    ) -> list[dict]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            self._check_tasks_alive()
            rows = await self._alerts_db.fetch_all_async("SELECT * FROM alerts")
            if any(predicate(r) for r in rows):
                return rows
            if loop.time() > deadline:
                raise TimeoutError(
                    "wait_for_alert: condition not met within timeout"
                )
            await asyncio.sleep(0.1)

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"http://127.0.0.1:{self.api_port}/api/v1/{endpoint}?{qs}"
        async with aiohttp.ClientSession() as session:
            resp = await session.get(url)
            body = await resp.json()
            return resp.status, body

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None:
        self._kafka_producer.publish(topic, msg)

    async def __aenter__(self) -> ContainerHarness:
        await self._setup()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._teardown()


# ══════════════════════════════════════════════════════════════════════════════
# SubprocessHarness — real infra, 6 OS subprocesses
# ══════════════════════════════════════════════════════════════════════════════


def _launch_module(
    module_name: str,
    infra: Any,
    db_flags: list[str],
    extra_flags: list[str] | None = None,
    group_id: str = "test",
    health_port: int = 0,
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
    env = {**os.environ, **infra.to_env_overrides(group_id=group_id)}
    return subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


class SubprocessHarness:
    """Real-infra harness with 6 OS subprocesses."""

    def __init__(
        self,
        infra: Any,
        algos_extra: list[str] | None = None,
    ) -> None:
        self._infra = infra
        self._algos_extra = algos_extra or []
        self._procs: dict[str, subprocess.Popen] = {}
        self.rest_port: int = 0
        self.api_port: int = 0
        self._clickhouse_db: Any = None
        self._alerts_db: Any = None
        self._kafka_producer: Any = None
        self._minio_fs: Any = None

    async def _setup(self) -> None:
        from de_platform.services.database.clickhouse_database import ClickHouseDatabase
        from de_platform.services.database.postgres_database import PostgresDatabase
        from de_platform.services.filesystem.minio_filesystem import MinioFileSystem
        from de_platform.services.message_queue.kafka_queue import KafkaQueue
        from de_platform.services.secrets.env_secrets import EnvSecrets

        infra = self._infra
        self.rest_port = _free_port()
        self.api_port = _free_port()
        group_base = uuid.uuid4().hex[:8]

        module_specs: list[tuple[str, list[str], list[str]]] = [
            ("rest_starter", ["--mq", "kafka"], ["--port", str(self.rest_port)]),
            ("kafka_starter", ["--mq", "kafka"], []),
            (
                "normalizer",
                [
                    "--db",
                    "warehouse=postgres",
                    "--cache",
                    "redis",
                    "--mq",
                    "kafka",
                ],
                [],
            ),
            (
                "persistence",
                [
                    "--db",
                    "default=clickhouse",
                    "--fs",
                    "minio",
                    "--mq",
                    "kafka",
                ],
                ["--flush-threshold", "1", "--flush-interval", "0"],
            ),
            (
                "algos",
                [
                    "--db",
                    "default=postgres",
                    "--cache",
                    "redis",
                    "--mq",
                    "kafka",
                ],
                self._algos_extra,
            ),
            (
                "data_api",
                [
                    "--db",
                    "events=clickhouse",
                    "--db",
                    "alerts=postgres",
                    "--mq",
                    "kafka",
                ],
                ["--port", str(self.api_port)],
            ),
        ]

        health_ports: dict[str, int] = {}
        for module_name, db_flags, extra_flags in module_specs:
            hp = _free_port()
            health_ports[module_name] = hp
            gid = f"sub-{group_base}-{module_name}"
            all_flags = db_flags + extra_flags
            self._procs[module_name] = _launch_module(
                module_name=module_name,
                infra=infra,
                db_flags=[],
                extra_flags=all_flags,
                group_id=gid,
                health_port=hp,
            )

        # Wait for all health endpoints
        for name, hp in health_ports.items():
            try:
                await _wait_for_http(
                    f"http://127.0.0.1:{hp}/health/startup", timeout=45
                )
            except TimeoutError:
                for n, p in self._procs.items():
                    if p.poll() is not None:
                        _, stderr = p.communicate(timeout=1)
                        print(
                            f"[{n}] exited with code {p.returncode}: "
                            f"{stderr.decode()[:500]}"
                        )
                raise

        # Assertion connections
        secrets = EnvSecrets(
            overrides=infra.to_env_overrides(group_id=f"assert-{group_base}")
        )
        ch_db = ClickHouseDatabase(secrets=secrets)
        ch_db.connect()
        self._clickhouse_db = ch_db

        alerts_db = PostgresDatabase(secrets=secrets, prefix="DB_ALERTS")
        await alerts_db.connect_async()
        self._alerts_db = alerts_db

        producer = KafkaQueue(secrets)
        producer.connect()
        self._kafka_producer = producer

        self._minio_fs = MinioFileSystem(secrets=secrets)

    async def _teardown(self) -> None:
        # SIGTERM all subprocesses
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

        self._kafka_producer.disconnect()
        self._clickhouse_db.disconnect()
        await self._alerts_db.disconnect_async()

    # ── Harness interface ────────────────────────────────────────────────

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None:
        if method == "rest":
            await ingest_rest_http(self.rest_port, event_type, events)
        elif method == "kafka":
            ingest_kafka_publish(self._kafka_producer, event_type, events)
        elif method == "files":
            ingest_files_subprocess(
                self._minio_fs,
                self._infra,
                event_type,
                events,
                _launch_module,
            )
        else:
            raise ValueError(f"Unknown method: {method!r}")

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 60.0
    ) -> list[dict]:
        await poll_until(
            lambda: len(
                self._clickhouse_db.fetch_all(f"SELECT * FROM {table}")
            )
            >= expected,
            timeout=timeout,
        )
        return self._clickhouse_db.fetch_all(f"SELECT * FROM {table}")

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 5.0
    ) -> None:
        await asyncio.sleep(timeout)
        rows = self._clickhouse_db.fetch_all(f"SELECT * FROM {table}")
        if len(rows) != known:
            raise AssertionError(
                f"Expected {known} rows in {table} to remain unchanged, got {len(rows)}"
            )

    async def fetch_alerts(self) -> list[dict]:
        return await self._alerts_db.fetch_all_async("SELECT * FROM alerts")

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 60.0
    ) -> list[dict]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            rows = await self._alerts_db.fetch_all_async("SELECT * FROM alerts")
            if any(predicate(r) for r in rows):
                return rows
            if loop.time() > deadline:
                raise TimeoutError(
                    "wait_for_alert: condition not met within timeout"
                )
            await asyncio.sleep(0.1)

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"http://127.0.0.1:{self.api_port}/api/v1/{endpoint}?{qs}"
        async with aiohttp.ClientSession() as session:
            resp = await session.get(url)
            body = await resp.json()
            return resp.status, body

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None:
        self._kafka_producer.publish(topic, msg)

    async def __aenter__(self) -> SubprocessHarness:
        await self._setup()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._teardown()
