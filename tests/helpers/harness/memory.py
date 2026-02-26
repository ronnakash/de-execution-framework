"""MemoryHarness -- in-memory stubs, manual step-by-step pipeline driving."""

from __future__ import annotations

from typing import Any, Callable

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
    ingest_kafka_starter,
    ingest_rest_inline,
)
from tests.helpers.step_logger import StepLogger


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
        self._currency_db = MemoryDatabase()

        self.clickhouse_db.connect()
        self._currency_db.connect()

        # Pre-seed currency rates in cache
        self.cache.set("currency_rate:EUR_USD", 1.10)
        self.cache.set("currency_rate:GBP_USD", 1.25)

        # Default window config: window_size=0 means immediate per-event
        # evaluation (LargeNotional, SuspiciousCounterparty).  Scenarios that
        # need sliding windows (velocity) override this before ingesting.
        self.cache.set(f"client_config:{self.tenant_id}", {
            "mode": "realtime",
            "window_size_minutes": 0,
            "window_slide_minutes": 0,
        })

        self._algos_config = algos_config or {}
        self._metrics = MemoryMetrics()
        self._collected_alerts: list[dict] = []
        self.normalizer: NormalizerModule | None = None
        self.persistence: PersistenceModule | None = None
        self.algos: AlgosModule | None = None
        self.diagnostics: TestDiagnostics | None = None
        self.step_logger: StepLogger = StepLogger()

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
            cache=self.cache,
            lifecycle=LifecycleManager(),
            metrics=self._metrics,
        )
        await self.algos.initialize()

        self.diagnostics = TestDiagnostics(
            memory_metrics=self._metrics,
            memory_queue=self.mq,
            clickhouse_db=self.clickhouse_db,
        )
        self.step_logger = StepLogger(diagnostics=self.diagnostics)

    # -- Pipeline step helpers ------------------------------------------------

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

        # Collect alerts published to the ALERTS Kafka topic
        while True:
            alert = self.mq.consume_one(ALERTS)
            if alert is None:
                break
            self._collected_alerts.append(alert)

    # -- Harness interface ----------------------------------------------------

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
        # Drive normalizer -> persistence for all topics
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
        return self._filter_by_tenant(self._collected_alerts)

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 30.0
    ) -> list[dict]:
        # Drive normalizer -> algos
        self._drain_normalizer()
        await self._drain_all_algos_topics()
        return self._filter_by_tenant(self._collected_alerts)

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]:
        db_factory = DatabaseFactory({})
        db_factory.register_instance("events", self.clickhouse_db)
        from de_platform.services.secrets.env_secrets import EnvSecrets

        api = DataApiModule(
            config=ModuleConfig({}),
            logger=LoggerFactory(default_impl="memory"),
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

    async def call_service(
        self,
        service: str,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, Any]:
        raise NotImplementedError("call_service is not supported in MemoryHarness")

    def step(self, name: str, description: str = ""):
        return self.step_logger.step(name, description)

    async def consume_alert_topic(self) -> dict | None:
        return self.mq.consume_one(ALERTS)

    async def __aenter__(self) -> MemoryHarness:
        await self._ensure_modules()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        pass
