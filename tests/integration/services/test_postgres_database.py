import os

import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("asyncpg")

from de_platform.services.database.postgres_database import PostgresDatabase  # noqa: E402
from de_platform.services.secrets.env_secrets import EnvSecrets  # noqa: E402


@pytest.fixture
async def db():
    """Connected PostgresDatabase backed by docker-compose Postgres.

    Uses an async fixture so it runs on the same event loop as pytest-asyncio tests.
    """
    if os.environ.get("DEVCONTAINER", "") == "1":
        default_url = "postgresql://platform:platform@postgres:5432/platform"
    else:
        default_url = "postgresql://platform:platform@localhost:5432/platform"
    url = os.environ.get("DB_TEST_URL", default_url)
    secrets = EnvSecrets(overrides={"DB_TEST_URL": url})
    database = PostgresDatabase(secrets=secrets, prefix="DB_TEST")

    await database.connect_async()
    await database.execute_async("DROP TABLE IF EXISTS _test_pg")

    yield database

    await database.execute_async("DROP TABLE IF EXISTS _test_pg")
    await database.disconnect_async()


async def test_connect_disconnect():
    if os.environ.get("DEVCONTAINER", "") == "1":
        default_url = "postgresql://platform:platform@postgres:5432/platform"
    else:
        default_url = "postgresql://platform:platform@localhost:5432/platform"
    url = os.environ.get("DB_TEST_URL", default_url)
    secrets = EnvSecrets(overrides={"DB_TEST_URL": url})
    database = PostgresDatabase(secrets=secrets, prefix="DB_TEST")
    await database.connect_async()
    assert database.is_connected()
    await database.disconnect_async()
    assert not database.is_connected()


async def test_execute_create_table(db):
    await db.execute_async(
        "CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"
    )
    row = await db.fetch_one_async(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '_test_pg')"
    )
    assert row is not None


async def test_fetch_one(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    await db.execute_async("INSERT INTO _test_pg (name) VALUES ($1)", ["alice"])
    row = await db.fetch_one_async("SELECT name FROM _test_pg WHERE name = $1", ["alice"])
    assert row is not None
    assert row["name"] == "alice"


async def test_fetch_one_no_results(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    row = await db.fetch_one_async("SELECT * FROM _test_pg WHERE name = $1", ["nobody"])
    assert row is None


async def test_fetch_all(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    await db.execute_async("INSERT INTO _test_pg (name) VALUES ($1)", ["a"])
    await db.execute_async("INSERT INTO _test_pg (name) VALUES ($1)", ["b"])
    rows = await db.fetch_all_async("SELECT name FROM _test_pg ORDER BY name")
    assert [r["name"] for r in rows] == ["a", "b"]


async def test_transaction_commit(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    async with db.transaction():
        await db.execute_async("INSERT INTO _test_pg (name) VALUES ($1)", ["committed"])
    row = await db.fetch_one_async("SELECT name FROM _test_pg")
    assert row is not None and row["name"] == "committed"


async def test_transaction_rollback(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    with pytest.raises(RuntimeError):
        async with db.transaction():
            await db.execute_async("INSERT INTO _test_pg (name) VALUES ($1)", ["rolled_back"])
            raise RuntimeError("force rollback")
    rows = await db.fetch_all_async("SELECT * FROM _test_pg")
    assert len(rows) == 0


async def test_bulk_insert(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    rows = [{"name": f"row_{i}"} for i in range(100)]
    count = await db.bulk_insert_async("_test_pg", rows)
    assert count == 100
    result = await db.fetch_all_async("SELECT * FROM _test_pg")
    assert len(result) == 100


async def test_health_check(db):
    assert await db.health_check_async() is True


async def test_parameterized_queries(db):
    await db.execute_async("CREATE TABLE _test_pg (id SERIAL PRIMARY KEY, name TEXT)")
    await db.execute_async("INSERT INTO _test_pg (name) VALUES ($1)", ["safe"])
    # Attempting SQL injection via parameter — should not execute the DROP
    rows = await db.fetch_all_async(
        "SELECT * FROM _test_pg WHERE name = $1",
        ["'; DROP TABLE _test_pg; --"],
    )
    assert len(rows) == 0
    # Table should still exist
    row = await db.fetch_one_async("SELECT count(*) as cnt FROM _test_pg")
    assert row is not None and row["cnt"] == 1
