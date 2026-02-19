"""Shared infrastructure fixtures for real-infra E2E tests.

Supports two modes:
  - Testcontainers (default): starts Postgres, ClickHouse, Redis, Kafka, MinIO in Docker.
  - Dev infra (USE_DEV_INFRA=1): reuses existing containers from docker-compose.

All fixtures that produce real service instances use session scope for containers
and function scope for cleanup, giving each test a clean slate.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
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
    # Keep container references alive to prevent GC
    _containers: list[Any] = field(default_factory=list, repr=False)

    def to_env_overrides(self, group_id: str = "test") -> dict[str, str]:
        """Build env var dict for EnvSecrets / subprocess env."""
        return {
            "DB_WAREHOUSE_URL": self.postgres_url,
            "DB_ALERTS_URL": self.postgres_url,
            "DB_CURRENCY_URL": self.postgres_url,
            "DB_EVENTS_URL": self.postgres_url,
            "DB_CLICKHOUSE_HOST": self.clickhouse_host,
            "DB_CLICKHOUSE_PORT": str(self.clickhouse_port),
            "DB_CLICKHOUSE_DATABASE": self.clickhouse_database,
            "DB_CLICKHOUSE_USER": self.clickhouse_user,
            "DB_CLICKHOUSE_PASSWORD": self.clickhouse_password,
            "CACHE_REDIS_URL": self.redis_url,
            "MQ_KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers,
            "MQ_KAFKA_GROUP_ID": group_id,
            "FS_MINIO_ENDPOINT": self.minio_endpoint,
            "FS_MINIO_ACCESS_KEY": self.minio_access_key,
            "FS_MINIO_SECRET_KEY": self.minio_secret_key,
            "FS_MINIO_BUCKET": self.minio_bucket,
            "FS_MINIO_SECURE": "false",
        }


# ── Testcontainer startup ───────────────────────────────────────────────────


def _start_testcontainers() -> InfraConfig:
    """Start all 5 infrastructure containers via testcontainers-python."""
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer
    from testcontainers.kafka import KafkaContainer
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    containers: list[Any] = []

    # Postgres
    pg = PostgresContainer("postgres:16")
    pg.start()
    containers.append(pg)
    pg_url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

    # Redis
    redis_c = RedisContainer("redis:7-alpine")
    redis_c.start()
    containers.append(redis_c)
    redis_url = f"redis://{redis_c.get_container_host_ip()}:{redis_c.get_exposed_port(6379)}/0"

    # Kafka
    kafka_c = KafkaContainer("confluentinc/cp-kafka:7.6.0")
    kafka_c.start()
    containers.append(kafka_c)
    kafka_bootstrap = kafka_c.get_bootstrap_server()

    # ClickHouse
    ch = (
        DockerContainer("clickhouse/clickhouse-server:latest")
        .with_exposed_ports(8123)
        .with_env("CLICKHOUSE_DB", "fraud_pipeline")
        .with_env("CLICKHOUSE_USER", "default")
        .with_env("CLICKHOUSE_PASSWORD", "")
    )
    ch.start()
    wait_for_logs(ch, "Ready for connections", timeout=30)
    containers.append(ch)
    ch_host = ch.get_container_host_ip()
    ch_port = int(ch.get_exposed_port(8123))

    # MinIO
    minio_c = (
        DockerContainer("minio/minio:latest")
        .with_exposed_ports(9000)
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_command("server /data")
    )
    minio_c.start()
    wait_for_logs(minio_c, "API:", timeout=30)
    containers.append(minio_c)
    minio_host = minio_c.get_container_host_ip()
    minio_port = minio_c.get_exposed_port(9000)

    return InfraConfig(
        postgres_url=pg_url,
        clickhouse_host=ch_host,
        clickhouse_port=ch_port,
        clickhouse_database="fraud_pipeline",
        clickhouse_user="default",
        clickhouse_password="",
        redis_url=redis_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        minio_endpoint=f"{minio_host}:{minio_port}",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="de-platform-test",
        _containers=containers,
    )


def _dev_infra_config() -> InfraConfig:
    """Return InfraConfig pointing to devcontainer services on localhost."""
    return InfraConfig(
        postgres_url="postgresql://platform:platform@localhost:5432/platform",
        clickhouse_host="localhost",
        clickhouse_port=8123,
        clickhouse_database="fraud_pipeline",
        clickhouse_user="default",
        clickhouse_password="",
        redis_url="redis://localhost:6379/0",
        kafka_bootstrap_servers="localhost:9092",
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="de-platform-test",
    )


# ── Session-scoped infra fixture ─────────────────────────────────────────────


@pytest.fixture(scope="session")
def infra() -> InfraConfig:
    """Provide infrastructure connection details (testcontainers or devcontainer)."""
    if os.environ.get("USE_DEV_INFRA", "").strip() in ("1", "true", "yes"):
        return _dev_infra_config()
    return _start_testcontainers()


# ── Session-scoped schema initialization ─────────────────────────────────────


@pytest.fixture(scope="session", autouse=True)
def _init_schemas(request) -> None:
    """Create Postgres tables (via migrations) and ClickHouse tables once per session.

    Only runs when at least one test in the session is marked with ``real_infra``.
    """
    has_real_infra = any(
        item.get_closest_marker("real_infra") for item in request.session.items
    )
    if not has_real_infra:
        return

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


# ── Function-scoped cleanup ──────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _cleanup_between_tests(request) -> None:
    """Truncate all data tables and flush Redis before each test.

    Only activates for tests marked with ``real_infra``.
    """
    if not request.node.get_closest_marker("real_infra"):
        return

    infra: InfraConfig = request.getfixturevalue("infra")

    import asyncio
    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.database.clickhouse_database import ClickHouseDatabase
    from de_platform.services.cache.redis_cache import RedisCache
    from de_platform.services.secrets.env_secrets import EnvSecrets

    secrets = EnvSecrets(overrides=infra.to_env_overrides())

    # Truncate Postgres tables
    pg = PostgresDatabase(secrets=secrets, prefix="DB_WAREHOUSE")
    asyncio.get_event_loop().run_until_complete(pg.connect_async())
    for table in ["alerts", "currency_rates"]:
        try:
            pg.execute(f"TRUNCATE TABLE {table} CASCADE")
        except Exception:
            pass
    # Re-seed currency rates
    _seed_currency_rates(pg)
    asyncio.get_event_loop().run_until_complete(pg.disconnect_async())

    # Truncate ClickHouse tables
    ch = ClickHouseDatabase(secrets=secrets)
    ch.connect()
    for table in ["orders", "executions", "transactions", "duplicates", "normalization_errors", "alerts"]:
        try:
            ch.execute(f"TRUNCATE TABLE {table}")
        except Exception:
            pass
    ch.disconnect()

    # Flush Redis
    cache = RedisCache(secrets=secrets)
    cache.connect()
    cache.flush()
    # Re-seed currency rates in Redis cache
    cache.set("currency_rate:EUR_USD", 1.10)
    cache.set("currency_rate:GBP_USD", 1.25)
    cache.set("currency_rate:USD_USD", 1.00)
    cache.disconnect()


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
