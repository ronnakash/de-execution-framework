"""CLI subcommand for database migrations.

Usage:
    python -m de_platform migrate up warehouse --db postgres
    python -m de_platform migrate down warehouse --count 1 --db postgres
    python -m de_platform migrate status warehouse --db postgres
    python -m de_platform migrate create warehouse add_users_table
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from de_platform.config.env_loader import load_env_file
from de_platform.migrations.runner import MIGRATIONS_DIR, MigrationRunner
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.registry import resolve_implementation
from de_platform.services.secrets.env_secrets import EnvSecrets


def _parse_migrate_args(argv: list[str]) -> dict[str, Any]:
    """Parse migrate subcommand arguments.

    Format: <command> <db_name> [migration_name] [--db impl] [--count N] [--target N] [--env-file F]
    """
    args: dict[str, Any] = {
        "command": None,
        "db_name": None,
        "target": None,
        "count": 1,
        "name": None,
    }
    impl_flags: dict[str, str] = {}
    env_file: str | None = None

    if not argv:
        raise ValueError(
            "Usage: python -m de_platform migrate <up|down|status|create> <db_name> [options]"
        )

    args["command"] = argv[0]

    # Second positional arg = database name
    positionals: list[str] = []
    i = 1
    while i < len(argv):
        flag = argv[i]
        if flag == "--target" and i + 1 < len(argv):
            args["target"] = int(argv[i + 1])
            i += 2
        elif flag == "--count" and i + 1 < len(argv):
            args["count"] = int(argv[i + 1])
            i += 2
        elif flag == "--db" and i + 1 < len(argv):
            impl_flags["db"] = argv[i + 1]
            i += 2
        elif flag == "--env-file" and i + 1 < len(argv):
            env_file = argv[i + 1]
            i += 2
        else:
            positionals.append(argv[i])
            i += 1

    if positionals:
        args["db_name"] = positionals[0]
    if len(positionals) > 1:
        args["name"] = positionals[1]

    args["impl_flags"] = impl_flags
    args["env_file"] = env_file
    return args


def _build_db(impl_name: str, db_name: str, env_file: str | None) -> DatabaseInterface:
    """Build a DatabaseInterface instance from flags."""
    overrides: dict[str, str] = {}
    if env_file:
        overrides = load_env_file(env_file)

    secrets = EnvSecrets(overrides=overrides)
    impl_cls = resolve_implementation("db", impl_name)

    # Check if constructor needs secrets
    from typing import get_type_hints

    try:
        hints = get_type_hints(impl_cls.__init__)
        hints.pop("return", None)
    except Exception:
        hints = {}

    if hints:
        prefix = f"DB_{db_name.upper()}"
        return impl_cls(secrets=secrets, prefix=prefix)
    return impl_cls()


def run_migrate(argv: list[str]) -> int:
    """Entry point for the migrate subcommand."""
    args = _parse_migrate_args(argv)
    command = args["command"]
    db_name = args["db_name"]

    if not db_name:
        print(
            "Usage: python -m de_platform migrate <up|down|status|create> <db_name> [options]",
            file=sys.stderr,
        )
        return 1

    if command == "create":
        return _create_migration(db_name, args["name"])

    # All other commands need a database
    db_impl = args["impl_flags"].get("db", "memory")
    db = _build_db(db_impl, db_name, args["env_file"])
    db.connect()

    try:
        runner = MigrationRunner(db, db_name=db_name)

        if command == "up":
            applied = runner.up(target=args["target"])
            if applied:
                for name in applied:
                    print(f"  Applied: {name}")
                print(f"\n{len(applied)} migration(s) applied.")
            else:
                print("No pending migrations.")
            return 0

        elif command == "down":
            rolled_back = runner.down(count=args["count"])
            if rolled_back:
                for name in rolled_back:
                    print(f"  Rolled back: {name}")
                print(f"\n{len(rolled_back)} migration(s) rolled back.")
            else:
                print("No migrations to roll back.")
            return 0

        elif command == "status":
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

        else:
            print(f"Unknown migrate command: {command}", file=sys.stderr)
            return 1
    finally:
        db.disconnect()


def _create_migration(db_name: str, name: str | None) -> int:
    """Create new .up.sql and .down.sql migration templates."""
    if not name:
        print(
            "Usage: python -m de_platform migrate create <db_name> <migration_name>",
            file=sys.stderr,
        )
        return 1

    migrations_dir = MIGRATIONS_DIR / db_name
    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Find next number
    existing = sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.up.sql"))
    if existing:
        last_num = int(existing[-1].name[:3])
        next_num = last_num + 1
    else:
        next_num = 1

    base = f"{next_num:03d}_{name}"
    up_path = migrations_dir / f"{base}.up.sql"
    down_path = migrations_dir / f"{base}.down.sql"

    up_path.write_text(f"-- {name.replace('_', ' ').title()} (up)\n")
    down_path.write_text(f"-- {name.replace('_', ' ').title()} (down)\n")

    print(f"Created: {up_path}")
    print(f"Created: {down_path}")
    return 0
