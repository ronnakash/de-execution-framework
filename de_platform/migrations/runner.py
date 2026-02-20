"""Migration runner: discovers, applies, and rolls back SQL-based database migrations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from de_platform.services.database.interface import DatabaseInterface

MIGRATIONS_DIR = Path(__file__).resolve().parent

TRACKING_TABLE = "_migrations"

CREATE_TRACKING = f"""
CREATE TABLE IF NOT EXISTS {TRACKING_TABLE} (
    name TEXT NOT NULL PRIMARY KEY,
    applied_at TEXT NOT NULL
)
"""


@dataclass
class MigrationStatus:
    name: str
    applied: bool
    applied_at: str | None = None


@dataclass
class SqlMigration:
    name: str
    number: int
    up_sql: str
    down_sql: str


def discover_sql_migrations(db_name: str) -> list[SqlMigration]:
    """Scan de_platform/migrations/<db_name>/ for NNN_name.up.sql / .down.sql pairs."""
    migrations_dir = MIGRATIONS_DIR / db_name
    if not migrations_dir.exists():
        return []

    pattern = re.compile(r"^(\d{3})_(.+)\.up\.sql$")
    migrations: list[SqlMigration] = []

    for path in sorted(migrations_dir.glob("*.up.sql")):
        match = pattern.match(path.name)
        if not match:
            continue
        number = int(match.group(1))
        base_name = f"{match.group(1)}_{match.group(2)}"
        down_path = migrations_dir / f"{base_name}.down.sql"

        if not down_path.exists():
            raise FileNotFoundError(
                f"Migration {path.name} is missing its .down.sql counterpart"
            )

        migrations.append(SqlMigration(
            name=base_name,
            number=number,
            up_sql=path.read_text(),
            down_sql=down_path.read_text(),
        ))

    migrations.sort(key=lambda m: m.number)
    return migrations


class MigrationRunner:
    def __init__(self, db: DatabaseInterface, db_name: str = "warehouse") -> None:
        self._db = db
        self._db_name = db_name

    def ensure_tracking_table(self) -> None:
        self._db.execute(CREATE_TRACKING)

    def get_applied(self) -> set[str]:
        rows = self._db.fetch_all(f"SELECT * FROM {TRACKING_TABLE}")
        return {r["name"] for r in rows}

    def up(self, target: int | None = None) -> list[str]:
        """Apply pending migrations. Returns list of applied migration names."""
        self.ensure_tracking_table()
        applied = self.get_applied()
        all_migrations = discover_sql_migrations(self._db_name)
        applied_names: list[str] = []

        for migration in all_migrations:
            if target is not None and migration.number > target:
                break
            if migration.name in applied:
                continue
            # Execute each statement in the up SQL
            for statement in _split_statements(migration.up_sql):
                self._db.execute(statement)
            now = datetime.utcnow().isoformat()
            self._db.insert_one(TRACKING_TABLE, {"name": migration.name, "applied_at": now})
            applied_names.append(migration.name)

        return applied_names

    def down(self, count: int = 1) -> list[str]:
        """Roll back the last `count` migrations. Returns list of rolled-back names."""
        self.ensure_tracking_table()
        applied = self.get_applied()
        all_migrations = discover_sql_migrations(self._db_name)

        # Find applied migrations in reverse order
        to_rollback = [m for m in reversed(all_migrations) if m.name in applied][:count]
        rolled_back: list[str] = []

        for migration in to_rollback:
            for statement in _split_statements(migration.down_sql):
                self._db.execute(statement)
            self._db.execute(
                f"DELETE FROM {TRACKING_TABLE} WHERE name = $1", [migration.name]
            )
            rolled_back.append(migration.name)

        return rolled_back

    def status(self) -> list[MigrationStatus]:
        """Return status of all known migrations."""
        self.ensure_tracking_table()
        applied_rows = self._db.fetch_all(f"SELECT * FROM {TRACKING_TABLE}")
        applied_map: dict[str, str] = {r["name"]: r["applied_at"] for r in applied_rows}
        all_migrations = discover_sql_migrations(self._db_name)

        return [
            MigrationStatus(
                name=m.name,
                applied=m.name in applied_map,
                applied_at=applied_map.get(m.name),
            )
            for m in all_migrations
        ]


def _split_statements(sql: str) -> list[str]:
    """Split SQL text into individual statements, filtering empty ones."""
    return [s.strip() for s in sql.split(";") if s.strip()]
