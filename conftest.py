"""Root-level pytest fixtures: docker-compose-backed databases with auto-migration."""

from __future__ import annotations

import os

import pytest


@pytest.fixture
async def warehouse_db():
    """Connected + migrated Postgres for 'warehouse' DB.

    Connects to the docker-compose Postgres (localhost or Docker DNS depending
    on DEVCONTAINER env var).  Skipped cleanly when asyncpg is not installed.
    """
    pytest.importorskip("asyncpg")

    from de_platform.migrations.runner import MigrationRunner
    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.secrets.env_secrets import EnvSecrets

    if os.environ.get("DEVCONTAINER", "") == "1":
        default_url = "postgresql://platform:platform@postgres:5432/platform"
    else:
        default_url = "postgresql://platform:platform@localhost:5432/platform"

    url = os.environ.get("DB_WAREHOUSE_URL", default_url)
    secrets = EnvSecrets(overrides={"DB_WAREHOUSE_URL": url})
    db = PostgresDatabase(secrets=secrets, prefix="DB_WAREHOUSE")

    await db.connect_async()

    # Run migrations
    runner = MigrationRunner(db, db_name="warehouse")
    runner.up()

    yield db

    # Teardown: rollback all migrations then disconnect
    runner.down(count=999)
    await db.disconnect_async()
