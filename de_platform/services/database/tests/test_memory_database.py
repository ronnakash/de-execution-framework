import pytest

from de_platform.services.database.memory_database import MemoryDatabase


def test_connect_disconnect():
    db = MemoryDatabase()
    assert not db.is_connected()
    db.connect()
    assert db.is_connected()
    db.disconnect()
    assert not db.is_connected()


def test_bulk_insert_and_fetch_all():
    db = MemoryDatabase()
    db.connect()
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    count = db.bulk_insert("users", rows)
    assert count == 2
    result = db.fetch_all("SELECT * FROM users")
    assert len(result) == 2
    assert result[0]["name"] == "Alice"


def test_fetch_one_returns_first():
    db = MemoryDatabase()
    db.connect()
    db.bulk_insert("users", [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    result = db.fetch_one("SELECT * FROM users")
    assert result is not None
    assert result["name"] == "Alice"


def test_fetch_one_returns_none_when_empty():
    db = MemoryDatabase()
    db.connect()
    result = db.fetch_one("SELECT * FROM users")
    assert result is None


def test_fetch_all_with_where():
    db = MemoryDatabase()
    db.connect()
    db.bulk_insert("users", [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    result = db.fetch_all("SELECT * FROM users WHERE id = $1", [1])
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_health_check_mirrors_connection():
    db = MemoryDatabase()
    assert not db.health_check()
    db.connect()
    assert db.health_check()
    db.disconnect()
    assert not db.health_check()


def test_raises_when_not_connected():
    db = MemoryDatabase()
    with pytest.raises(RuntimeError, match="not connected"):
        db.fetch_all("SELECT * FROM users")
