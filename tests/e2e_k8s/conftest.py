"""Fixtures for K8s E2E tests.

Connects to services already running in a local kind cluster (via ``make dev-k8s``).
No subprocesses are started — ExternalPipeline just connects to port-forwarded
services and waits for health endpoints.

Migrations and dev user seeding are handled by dev-k8s.sh.  We only seed
currency rates and auth data needed by test tenants.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from tests.e2e.conftest import InfraConfig, _seed_auth_data, _seed_currency_rates

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"


# ── Infrastructure config ────────────────────────────────────────────────────


def _k8s_infra_config() -> InfraConfig:
    """Return InfraConfig pointing to K8s services via port-forwarded localhost."""
    return InfraConfig(
        postgres_url=os.environ.get(
            "DB_WAREHOUSE_URL", "postgresql://platform:platform@localhost:5432/platform"
        ),
        clickhouse_host=os.environ.get("DB_CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.environ.get("DB_CLICKHOUSE_PORT", "8123")),
        clickhouse_database=os.environ.get("DB_CLICKHOUSE_DATABASE", "fraud_pipeline"),
        clickhouse_user=os.environ.get("DB_CLICKHOUSE_USER", "default"),
        clickhouse_password=os.environ.get("DB_CLICKHOUSE_PASSWORD", "clickhouse"),
        redis_url=os.environ.get("CACHE_REDIS_URL", "redis://localhost:6379/0"),
        kafka_bootstrap_servers=os.environ.get(
            "MQ_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        minio_endpoint=os.environ.get("FS_MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.environ.get("FS_MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("FS_MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.environ.get("FS_MINIO_BUCKET", "de-platform-test"),
    )


# ── Session-scoped fixtures ──────────────────────────────────────────────────


@pytest.fixture(scope="session")
def infra() -> InfraConfig:
    """Provide infrastructure connection details for K8s port-forwarded services."""
    return _k8s_infra_config()


@pytest.fixture(scope="session", autouse=True)
def _init_schemas(request, infra) -> None:
    """Seed currency rates and auth data for test tenants.

    Migrations and ClickHouse schema are already handled by dev-k8s.sh.
    We only need to ensure test-specific seed data exists.
    """
    has_k8s_tests = any(
        item.get_closest_marker("e2e_k8s")
        for item in request.session.items
    )
    if not has_k8s_tests:
        return

    pytest.importorskip("asyncpg")

    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.secrets.env_secrets import EnvSecrets

    pg_secrets = EnvSecrets(overrides={"DB_WAREHOUSE_URL": infra.postgres_url})
    pg_db = PostgresDatabase(secrets=pg_secrets, prefix="DB_WAREHOUSE")

    asyncio.get_event_loop().run_until_complete(pg_db.connect_async())
    _seed_currency_rates(pg_db)
    _seed_auth_data(pg_db)
    asyncio.get_event_loop().run_until_complete(pg_db.disconnect_async())


@pytest.fixture(scope="session")
def shared_pipeline(infra):
    """Session-scoped ExternalPipeline for K8s E2E tests.

    Connects to the already-running K8s services (no subprocess management).
    """
    from tests.helpers.harness import ExternalPipeline

    pipeline = ExternalPipeline(infra)
    pipeline.start()
    yield pipeline
    pipeline.close()
