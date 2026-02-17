"""Root-level pytest fixtures: testcontainer-backed databases with auto-migration."""

from __future__ import annotations

import asyncio

import pytest


@pytest.fixture(scope="session")
def postgres_container():
    """Single Postgres container for the test session."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture
def warehouse_db(postgres_container):
    """Connected + migrated Postgres for 'warehouse' DB."""
    from de_platform.migrations.runner import MigrationRunner
    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.secrets.env_secrets import EnvSecrets

    # testcontainers gives a psycopg2-style URL; convert to asyncpg format
    url = postgres_container.get_connection_url()
    asyncpg_url = url.replace("postgresql+psycopg2://", "postgresql://")

    secrets = EnvSecrets(overrides={"DB_WAREHOUSE_URL": asyncpg_url})
    db = PostgresDatabase(secrets=secrets, prefix="DB_WAREHOUSE")

    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.run_until_complete(db.connect_async())

    # Run migrations
    runner = MigrationRunner(db, db_name="warehouse")
    runner.up()

    yield db

    # Teardown: rollback all migrations then disconnect
    runner.down(count=999)
    loop.run_until_complete(db.disconnect_async())
    loop.close()
