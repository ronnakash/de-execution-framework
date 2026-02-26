"""Integration tests for ClickHouseDatabase service."""

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("clickhouse_connect")

from de_platform.services.database.clickhouse_database import ClickHouseDatabase  # noqa: E402
from de_platform.services.secrets.env_secrets import EnvSecrets  # noqa: E402


def _make_secrets() -> EnvSecrets:
    in_dc = os.environ.get("DEVCONTAINER", "") == "1"
    ch_host = "clickhouse" if in_dc else "localhost"
    return EnvSecrets(overrides={
        "DB_CLICKHOUSE_HOST": os.environ.get("DB_CLICKHOUSE_HOST", ch_host),
        "DB_CLICKHOUSE_PORT": os.environ.get("DB_CLICKHOUSE_PORT", "8123"),
        "DB_CLICKHOUSE_DATABASE": os.environ.get("DB_CLICKHOUSE_DATABASE", "fraud_pipeline"),
        "DB_CLICKHOUSE_USER": os.environ.get("DB_CLICKHOUSE_USER", "default"),
        "DB_CLICKHOUSE_PASSWORD": os.environ.get("DB_CLICKHOUSE_PASSWORD", "clickhouse"),
    })


@pytest.fixture
def ch():
    secrets = _make_secrets()
    db = ClickHouseDatabase(secrets=secrets)
    db.connect()
    # Create a unique test table using ReplacingMergeTree (supports FINAL keyword)
    db._test_table = f"_test_ch_{uuid.uuid4().hex[:8]}"
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {db._test_table} "
        f"(id String, name String, value Float64) "
        f"ENGINE = ReplacingMergeTree() ORDER BY id"
    )
    yield db
    db.execute(f"DROP TABLE IF EXISTS {db._test_table}")
    db.disconnect()


def test_connect_disconnect():
    secrets = _make_secrets()
    db = ClickHouseDatabase(secrets=secrets)
    db.connect()
    assert db.is_connected()
    db.disconnect()
    assert not db.is_connected()


def test_insert_and_fetch(ch):
    table = ch._test_table
    ch.insert_one(table, {"id": "1", "name": "alice", "value": 42.0})
    rows = ch.fetch_all(f"SELECT * FROM {table} FINAL WHERE id = {{p1:String}}", ["1"])
    assert len(rows) == 1
    assert rows[0]["name"] == "alice"


def test_fetch_all_multiple(ch):
    table = ch._test_table
    ch.insert_one(table, {"id": "a", "name": "alpha", "value": 1.0})
    ch.insert_one(table, {"id": "b", "name": "beta", "value": 2.0})
    rows = ch.fetch_all(f"SELECT * FROM {table} FINAL")
    names = {r["name"] for r in rows}
    assert "alpha" in names
    assert "beta" in names


def test_bulk_insert(ch):
    table = ch._test_table
    rows = [{"id": str(i), "name": f"row_{i}", "value": float(i)} for i in range(50)]
    ch.bulk_insert(table, rows)
    result = ch.fetch_all(f"SELECT * FROM {table} FINAL")
    assert len(result) >= 50


def test_final_keyword_dedup(ch):
    """FINAL keyword deduplicates rows with the same primary key in ReplacingMergeTree."""
    table = ch._test_table
    ch.insert_one(table, {"id": "dup", "name": "v1", "value": 1.0})
    ch.insert_one(table, {"id": "dup", "name": "v2", "value": 2.0})
    rows_final = ch.fetch_all(f"SELECT * FROM {table} FINAL WHERE id = {{p1:String}}", ["dup"])
    # FINAL returns the latest version
    assert len(rows_final) == 1
    assert rows_final[0]["name"] == "v2"
