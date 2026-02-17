"""Migration runner: discovers, applies, and rolls back database migrations."""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from de_platform.services.database.interface import DatabaseInterface

VERSIONS_DIR = Path(__file__).resolve().parent / "versions"

TRACKING_TABLE = "_migrations"

CREATE_TRACKING = f"""
CREATE TABLE IF NOT EXISTS {TRACKING_TABLE} (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    applied_at TEXT NOT NULL
)
"""


@dataclass
class MigrationStatus:
    name: str
    applied: bool
    applied_at: str | None = None


@dataclass
class Migration:
    name: str
    number: int
    up: Callable[[DatabaseInterface], None]
    down: Callable[[DatabaseInterface], None]


def discover_migrations() -> list[Migration]:
    """Scan the versions directory for NNN_*.py files and load up/down functions."""
    pattern = re.compile(r"^(\d{3})_.+\.py$")
    migrations: list[Migration] = []

    for path in sorted(VERSIONS_DIR.glob("*.py")):
        match = pattern.match(path.name)
        if not match:
            continue
        number = int(match.group(1))
        module_name = path.stem
        spec_name = f"de_platform.migrations.versions.{module_name}"
        mod = importlib.import_module(spec_name)

        if not hasattr(mod, "up") or not hasattr(mod, "down"):
            raise ImportError(
                f"Migration {path.name} must define both 'up(db)' and 'down(db)' functions"
            )

        migrations.append(Migration(
            name=module_name,
            number=number,
            up=mod.up,
            down=mod.down,
        ))

    migrations.sort(key=lambda m: m.number)
    return migrations


class MigrationRunner:
    def __init__(self, db: DatabaseInterface) -> None:
        self._db = db

    def ensure_tracking_table(self) -> None:
        self._db.execute(CREATE_TRACKING)

    def get_applied(self) -> set[str]:
        rows = self._db.fetch_all(f"SELECT * FROM {TRACKING_TABLE}")
        return {r["name"] for r in rows}

    def up(self, target: int | None = None) -> list[str]:
        """Apply pending migrations. Returns list of applied migration names."""
        self.ensure_tracking_table()
        applied = self.get_applied()
        all_migrations = discover_migrations()
        applied_names: list[str] = []

        for migration in all_migrations:
            if target is not None and migration.number > target:
                break
            if migration.name in applied:
                continue
            migration.up(self._db)
            now = datetime.utcnow().isoformat()
            self._db.bulk_insert(TRACKING_TABLE, [{"name": migration.name, "applied_at": now}])
            applied_names.append(migration.name)

        return applied_names

    def down(self, count: int = 1) -> list[str]:
        """Roll back the last `count` migrations. Returns list of rolled-back names."""
        self.ensure_tracking_table()
        applied = self.get_applied()
        all_migrations = discover_migrations()

        # Find applied migrations in reverse order
        to_rollback = [m for m in reversed(all_migrations) if m.name in applied][:count]
        rolled_back: list[str] = []

        for migration in to_rollback:
            migration.down(self._db)
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
        all_migrations = discover_migrations()

        return [
            MigrationStatus(
                name=m.name,
                applied=m.name in applied_map,
                applied_at=applied_map.get(m.name),
            )
            for m in all_migrations
        ]
