"""Migration runner module.

Applies or rolls back SQL migrations for a named database.

Usage examples::

    # Apply all pending migrations to the warehouse database
    python -m de_platform run run_migrations --db warehouse=postgres --env-file devcontainer

    # Roll back the last 2 migrations (requires --allow-down safety flag)
    python -m de_platform run run_migrations \\
        --direction down --count 2 --allow-down \\
        --db warehouse=postgres

    # Check migration status
    python -m de_platform run run_migrations --direction status --db warehouse=postgres

    # Create a new migration template
    python -m de_platform run run_migrations --create add_users_table --db-name warehouse

    # Apply only up to migration #003
    python -m de_platform run run_migrations --target 3 --db warehouse=postgres
"""

from __future__ import annotations

import sys

from de_platform.config.context import ModuleConfig
from de_platform.migrations.runner import MIGRATIONS_DIR, MigrationRunner
from de_platform.modules.base import Module
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface


class RunMigrationsModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.db.connect()

    async def teardown(self) -> None:
        self.db.disconnect()

    async def execute(self) -> int:
        direction = self.config.get("direction", "up")
        db_name = self.config.get("db-name", "warehouse")
        count = self.config.get("count", 1)
        target = self.config.get("target", None)
        allow_down = self.config.get("allow-down", False)
        create_name = self.config.get("create", None)

        # Create a new migration template
        if create_name:
            return self._create_migration(db_name, create_name)

        runner = MigrationRunner(self.db, db_name=db_name)

        if direction == "up":
            return self._run_up(runner, db_name, target)

        if direction == "down":
            return self._run_down(runner, db_name, count, allow_down)

        if direction == "status":
            return self._run_status(runner)

        self.log.error("Unknown direction", direction=direction)
        print(
            f"Unknown direction: {direction!r}. Use 'up', 'down', or 'status'.",
            file=sys.stderr,
        )
        return 1

    def _run_up(self, runner: MigrationRunner, db_name: str, target: int | None) -> int:
        self.log.info("Applying migrations", db_name=db_name, target=target)
        applied = runner.up(target=target)
        if applied:
            for name in applied:
                self.log.info("Applied migration", name=name)
                print(f"  Applied: {name}")
            print(f"\n{len(applied)} migration(s) applied.")
        else:
            print("No pending migrations.")
        return 0

    def _run_down(
        self, runner: MigrationRunner, db_name: str, count: int, allow_down: bool
    ) -> int:
        if not allow_down:
            self.log.error(
                "Down migrations blocked",
                hint="Pass --allow-down true to enable rollbacks",
            )
            print(
                "Error: Down migrations are disabled by default.\n"
                "Pass --allow-down true to enable rolling back migrations.",
                file=sys.stderr,
            )
            return 1

        self.log.info("Rolling back migrations", db_name=db_name, count=count)
        rolled_back = runner.down(count=count)
        if rolled_back:
            for name in rolled_back:
                self.log.info("Rolled back migration", name=name)
                print(f"  Rolled back: {name}")
            print(f"\n{len(rolled_back)} migration(s) rolled back.")
        else:
            print("No migrations to roll back.")
        return 0

    def _run_status(self, runner: MigrationRunner) -> int:
        statuses = runner.status()
        if not statuses:
            print("No migrations found.")
            return 0
        print(f"{'Migration':<45} {'Status':<10} {'Applied At'}")
        print("-" * 80)
        for s in statuses:
            status = "applied" if s.applied else "pending"
            at = s.applied_at or ""
            print(f"{s.name:<45} {status:<10} {at}")
        return 0

    def _create_migration(self, db_name: str, name: str) -> int:
        migrations_dir = MIGRATIONS_DIR / db_name
        migrations_dir.mkdir(parents=True, exist_ok=True)

        existing = sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.up.sql"))
        next_num = int(existing[-1].name[:3]) + 1 if existing else 1

        base = f"{next_num:03d}_{name}"
        up_path = migrations_dir / f"{base}.up.sql"
        down_path = migrations_dir / f"{base}.down.sql"

        up_path.write_text(f"-- {name.replace('_', ' ').title()} (up)\n")
        down_path.write_text(f"-- {name.replace('_', ' ').title()} (down)\n")

        self.log.info("Created migration", up=str(up_path), down=str(down_path))
        print(f"Created: {up_path}")
        print(f"Created: {down_path}")
        return 0


module_class = RunMigrationsModule
