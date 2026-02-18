from de_platform.migrations.runner import MigrationRunner, discover_sql_migrations
from de_platform.services.database.memory_database import MemoryDatabase


def _make_db():
    db = MemoryDatabase()
    db.connect()
    return db


def test_discover_sql_migrations():
    migrations = discover_sql_migrations("warehouse")
    assert len(migrations) >= 2
    assert migrations[0].name == "001_create_events_table"
    assert migrations[1].name == "002_create_cleaned_events_table"


def test_discover_sql_migrations_has_content():
    migrations = discover_sql_migrations("warehouse")
    assert "CREATE TABLE" in migrations[0].up_sql
    assert "DROP TABLE" in migrations[0].down_sql


def test_discover_nonexistent_db():
    migrations = discover_sql_migrations("nonexistent")
    assert migrations == []


def test_up_applies_all():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    applied = runner.up()
    assert len(applied) >= 2
    assert "001_create_events_table" in applied
    assert "002_create_cleaned_events_table" in applied


def test_up_idempotent():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    first = runner.up()
    second = runner.up()
    assert len(first) >= 2
    assert len(second) == 0


def test_up_with_target():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    applied = runner.up(target=1)
    assert len(applied) == 1
    assert applied[0] == "001_create_events_table"


def test_down_rolls_back():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    runner.up()
    rolled_back = runner.down(count=1)
    assert len(rolled_back) == 1
    assert rolled_back[0] == "004_create_currency_rates_table"


def test_down_then_up():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    runner.up()
    runner.down(count=2)
    # All should be pending again
    applied = runner.up()
    assert len(applied) >= 2


def test_status():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    runner.up()
    statuses = runner.status()
    assert len(statuses) >= 2
    assert all(s.applied for s in statuses)
    assert all(s.applied_at is not None for s in statuses)


def test_status_mixed():
    db = _make_db()
    runner = MigrationRunner(db, db_name="warehouse")
    runner.up(target=1)
    statuses = runner.status()
    assert statuses[0].applied is True
    assert statuses[1].applied is False
