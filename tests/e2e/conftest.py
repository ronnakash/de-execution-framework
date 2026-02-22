"""Shared infrastructure fixtures for integration and E2E tests.

Uses docker-compose services (started via ``make infra-up`` or the devcontainer).
When DEVCONTAINER=1, uses Docker DNS names (postgres, redis, kafka:29092, etc.);
otherwise defaults to localhost.
"""

from __future__ import annotations

import fcntl
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"


# ── Infrastructure config ────────────────────────────────────────────────────


@dataclass
class InfraConfig:
    postgres_url: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str
    redis_url: str
    kafka_bootstrap_servers: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str

    def to_env_overrides(self, group_id: str = "test") -> dict[str, str]:
        """Build env var dict for EnvSecrets / subprocess env.

        Includes env vars for all DB name prefixes used by modules:
        - warehouse (normalizer), alerts (algos, data_api), events (data_api)
        - clickhouse (persistence), currency (currency_loader)
        """
        ch_vars = {
            "HOST": self.clickhouse_host,
            "PORT": str(self.clickhouse_port),
            "DATABASE": self.clickhouse_database,
            "USER": self.clickhouse_user,
            "PASSWORD": self.clickhouse_password,
        }
        result = {
            # Postgres instances (all point to same server)
            "DB_WAREHOUSE_URL": self.postgres_url,
            "DB_ALERTS_URL": self.postgres_url,
            "DB_CURRENCY_URL": self.postgres_url,
            "DB_CLIENT_CONFIG_URL": self.postgres_url,
            # ClickHouse instances (all point to same server)
            **{f"DB_CLICKHOUSE_{k}": v for k, v in ch_vars.items()},
            **{f"DB_EVENTS_{k}": v for k, v in ch_vars.items()},
            # Redis, Kafka, MinIO
            "CACHE_REDIS_URL": self.redis_url,
            "MQ_KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers,
            "MQ_KAFKA_GROUP_ID": group_id,
            "MQ_KAFKA_POLL_TIMEOUT": "0.01",
            "MQ_KAFKA_AUTO_OFFSET_RESET": "latest",
            "FS_MINIO_ENDPOINT": self.minio_endpoint,
            "FS_MINIO_ACCESS_KEY": self.minio_access_key,
            "FS_MINIO_SECRET_KEY": self.minio_secret_key,
            "FS_MINIO_BUCKET": self.minio_bucket,
            "FS_MINIO_SECURE": "false",
        }
        return result


def _infra_config() -> InfraConfig:
    """Return InfraConfig pointing to docker-compose services.

    When DEVCONTAINER=1, uses Docker DNS names; otherwise localhost.
    Individual values can be overridden via environment variables.
    """
    in_devcontainer = os.environ.get("DEVCONTAINER", "") == "1"
    pg_host = "postgres" if in_devcontainer else "localhost"
    redis_host = "redis" if in_devcontainer else "localhost"
    kafka_bootstrap = "kafka:29092" if in_devcontainer else "localhost:9092"
    minio_host = "minio" if in_devcontainer else "localhost"
    ch_host = "clickhouse" if in_devcontainer else "localhost"

    return InfraConfig(
        postgres_url=os.environ.get(
            "DB_WAREHOUSE_URL", f"postgresql://platform:platform@{pg_host}:5432/platform"
        ),
        clickhouse_host=os.environ.get("DB_CLICKHOUSE_HOST", ch_host),
        clickhouse_port=int(os.environ.get("DB_CLICKHOUSE_PORT", "8123")),
        clickhouse_database=os.environ.get("DB_CLICKHOUSE_DATABASE", "fraud_pipeline"),
        clickhouse_user=os.environ.get("DB_CLICKHOUSE_USER", "default"),
        clickhouse_password=os.environ.get("DB_CLICKHOUSE_PASSWORD", "clickhouse"),
        redis_url=os.environ.get("CACHE_REDIS_URL", f"redis://{redis_host}:6379/0"),
        kafka_bootstrap_servers=os.environ.get(
            "MQ_KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap
        ),
        minio_endpoint=os.environ.get("FS_MINIO_ENDPOINT", f"{minio_host}:9000"),
        minio_access_key=os.environ.get("FS_MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("FS_MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.environ.get("FS_MINIO_BUCKET", "de-platform-test"),
    )


# ── Session-scoped infra fixture ─────────────────────────────────────────────


@pytest.fixture(scope="session")
def infra() -> InfraConfig:
    """Provide infrastructure connection details from docker-compose services."""
    return _infra_config()


# ── Session-scoped schema initialization ─────────────────────────────────────


@pytest.fixture(scope="session", autouse=True)
def _init_schemas(request, tmp_path_factory) -> None:
    """Create Postgres tables (via migrations) and ClickHouse tables once per session.

    Only runs when at least one test in the session is marked with ``integration``
    or ``e2e``.  Uses a file lock to ensure only one xdist worker performs
    initialization; other workers wait for completion.
    """
    has_infra_tests = any(
        item.get_closest_marker("integration") or item.get_closest_marker("e2e")
        for item in request.session.items
    )
    if not has_infra_tests:
        return

    pytest.importorskip("asyncpg")
    pytest.importorskip("clickhouse_connect")

    # Use a file lock to ensure schema init runs only once across xdist workers.
    # tmp_path_factory.getbasetemp() returns a shared base when using xdist.
    lock_dir = tmp_path_factory.getbasetemp().parent if hasattr(tmp_path_factory, "getbasetemp") else Path(os.environ.get("TMPDIR", "/tmp"))
    lock_file = lock_dir / "de_platform_schema_init.lock"
    done_file = lock_dir / "de_platform_schema_init.done"

    with open(lock_file, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            if done_file.exists():
                # Another worker already completed init
                return
            _run_schema_init(request)
            done_file.write_text("done")
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def _run_schema_init(request: Any) -> None:
    """Perform the actual schema initialization (called under file lock)."""
    infra: InfraConfig = request.getfixturevalue("infra")

    from de_platform.migrations.runner import MigrationRunner
    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.database.clickhouse_database import ClickHouseDatabase
    from de_platform.services.secrets.env_secrets import EnvSecrets

    # Postgres: run warehouse + alerts migrations
    pg_secrets = EnvSecrets(overrides={"DB_WAREHOUSE_URL": infra.postgres_url})
    pg_db = PostgresDatabase(secrets=pg_secrets, prefix="DB_WAREHOUSE")
    import asyncio

    asyncio.get_event_loop().run_until_complete(pg_db.connect_async())
    MigrationRunner(pg_db, db_name="warehouse").up()
    MigrationRunner(pg_db, db_name="alerts").up()
    MigrationRunner(pg_db, db_name="client_config").up()
    asyncio.get_event_loop().run_until_complete(pg_db.disconnect_async())

    # ClickHouse: execute init SQL
    ch_secrets = EnvSecrets(overrides={
        "DB_CLICKHOUSE_HOST": infra.clickhouse_host,
        "DB_CLICKHOUSE_PORT": str(infra.clickhouse_port),
        "DB_CLICKHOUSE_DATABASE": infra.clickhouse_database,
        "DB_CLICKHOUSE_USER": infra.clickhouse_user,
        "DB_CLICKHOUSE_PASSWORD": infra.clickhouse_password,
    })
    ch_db = ClickHouseDatabase(secrets=ch_secrets)
    ch_db.connect()
    init_sql = (SCRIPTS_DIR / "clickhouse_init.sql").read_text()
    for stmt in init_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            ch_db.execute(stmt)
    ch_db.disconnect()

    # Seed currency rates in Postgres
    pg_db2 = PostgresDatabase(secrets=pg_secrets, prefix="DB_WAREHOUSE")
    asyncio.get_event_loop().run_until_complete(pg_db2.connect_async())
    _seed_currency_rates(pg_db2)
    asyncio.get_event_loop().run_until_complete(pg_db2.disconnect_async())

    # Kafka: ensure topics exist with enough partitions for parallel tests
    _ensure_kafka_topics(infra)


def _ensure_kafka_topics(infra: InfraConfig) -> None:
    """Pre-create pipeline Kafka topics with enough partitions for parallel tests.

    With shared consumer groups across test instances, we need enough partitions
    so that each consumer gets at least one partition. Messages keyed by
    tenant_id:symbol hash to partitions, giving each test's data its own partition.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    from de_platform.pipeline.topics import (
        ALERTS,
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

    admin = AdminClient({"bootstrap.servers": infra.kafka_bootstrap_servers})
    num_partitions = 3

    # Get existing topics
    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())

    all_topics = [
        TRADE_NORMALIZATION, TX_NORMALIZATION,
        NORMALIZATION_ERRORS, DUPLICATES,
        ORDERS_PERSISTENCE, EXECUTIONS_PERSISTENCE, TRANSACTIONS_PERSISTENCE,
        TRADES_ALGOS, TRANSACTIONS_ALGOS,
        ALERTS,
    ]

    # Create missing topics
    to_create = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=1)
        for t in all_topics
        if t not in existing
    ]
    if to_create:
        futures = admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()
            except Exception:
                pass  # topic may already exist from a concurrent worker

    # Increase partitions on existing topics that have fewer than needed
    from confluent_kafka.admin import NewPartitions

    to_increase = []
    for t in all_topics:
        if t in existing:
            topic_meta = metadata.topics[t]
            if len(topic_meta.partitions) < num_partitions:
                to_increase.append(NewPartitions(t, num_partitions))
    if to_increase:
        futures = admin.create_partitions(to_increase)
        for topic, future in futures.items():
            try:
                future.result()
            except Exception:
                pass  # may already have enough partitions


def _seed_currency_rates(db: Any) -> None:
    """Insert standard currency rates for tests."""
    rates = [
        {"from_currency": "EUR", "to_currency": "USD", "rate": 1.10, "effective_at": "2026-01-01T00:00:00"},
        {"from_currency": "GBP", "to_currency": "USD", "rate": 1.25, "effective_at": "2026-01-01T00:00:00"},
        {"from_currency": "USD", "to_currency": "USD", "rate": 1.00, "effective_at": "2026-01-01T00:00:00"},
    ]
    try:
        db.bulk_insert("currency_rates", rates)
    except Exception:
        pass  # rates may already exist


# ── Session-scoped shared pipeline ──────────────────────────────────────────


@pytest.fixture(scope="session")
def shared_pipeline(request, infra, tmp_path_factory):
    """Session-scoped shared pipeline for E2E tests.

    Starts 6 module subprocesses once and shares them across all tests.
    Only activates when at least one test is marked with ``e2e``.
    """
    has_e2e = any(
        item.get_closest_marker("e2e") for item in request.session.items
    )
    if not has_e2e:
        yield None
        return

    from tests.helpers.harness import SharedPipeline

    # Under xdist, getbasetemp() is per-worker (e.g. .../gw0/);
    # use .parent so all workers share the same lock/info files
    # and only one pipeline is started.
    lock_dir = tmp_path_factory.getbasetemp()
    if os.environ.get("PYTEST_XDIST_WORKER"):
        lock_dir = lock_dir.parent
    pipeline = SharedPipeline(infra, lock_dir)
    pipeline.start()
    yield pipeline
    pipeline.close()


# ── Per-test Kafka isolation ─────────────────────────────────────────────────


@pytest.fixture
def test_group_id() -> str:
    """Unique consumer group ID per test to isolate Kafka consumption."""
    return f"test-{uuid.uuid4().hex[:12]}"


# ── Service factory fixtures ─────────────────────────────────────────────────


@pytest.fixture
def secrets(infra: InfraConfig, test_group_id: str):
    """EnvSecrets with infra connection details and unique group ID."""
    from de_platform.services.secrets.env_secrets import EnvSecrets

    return EnvSecrets(overrides=infra.to_env_overrides(group_id=test_group_id))


@pytest.fixture
async def warehouse_db(secrets):
    """Connected PostgresDatabase for warehouse (currency_rates, etc)."""
    from de_platform.services.database.postgres_database import PostgresDatabase

    db = PostgresDatabase(secrets=secrets, prefix="DB_WAREHOUSE")
    await db.connect_async()
    yield db
    await db.disconnect_async()


@pytest.fixture
async def alerts_db(secrets):
    """Connected PostgresDatabase for alerts."""
    from de_platform.services.database.postgres_database import PostgresDatabase

    db = PostgresDatabase(secrets=secrets, prefix="DB_ALERTS")
    await db.connect_async()
    yield db
    await db.disconnect_async()


@pytest.fixture
def clickhouse_db(secrets):
    """Connected ClickHouseDatabase."""
    from de_platform.services.database.clickhouse_database import ClickHouseDatabase

    db = ClickHouseDatabase(secrets=secrets)
    db.connect()
    yield db
    db.disconnect()


@pytest.fixture
def redis_cache(secrets):
    """Connected RedisCache."""
    from de_platform.services.cache.redis_cache import RedisCache

    cache = RedisCache(secrets=secrets)
    cache.connect()
    yield cache
    cache.disconnect()


@pytest.fixture
def kafka_mq(secrets):
    """Connected KafkaQueue."""
    from de_platform.services.message_queue.kafka_queue import KafkaQueue

    mq = KafkaQueue(secrets=secrets)
    mq.connect()
    yield mq
    mq.disconnect()


@pytest.fixture
def minio_fs(secrets):
    """MinioFileSystem instance."""
    from de_platform.services.filesystem.minio_filesystem import MinioFileSystem

    return MinioFileSystem(secrets=secrets)
