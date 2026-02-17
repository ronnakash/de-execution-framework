"""CLI subcommand for database migrations."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from de_platform.config.env_loader import load_env_file
from de_platform.migrations.runner import MigrationRunner, VERSIONS_DIR
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.registry import resolve_implementation
from de_platform.services.secrets.env_secrets import EnvSecrets


def _parse_migrate_args(argv: list[str]) -> dict[str, Any]:
    """Parse migrate subcommand arguments."""
    args: dict[str, Any] = {"command": None, "target": None, "count": 1, "name": None}
    impl_flags: dict[str, str] = {}
    env_file: str | None = None

    if not argv:
        raise ValueError("Usage: python -m de_platform migrate <up|down|status|create> [options]")

    args["command"] = argv[0]
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
            # Positional arg (e.g. migration name for 'create')
            if args["name"] is None:
                args["name"] = argv[i]
            i += 1

    args["impl_flags"] = impl_flags
    args["env_file"] = env_file
    return args


def _build_db(impl_name: str, env_file: str | None) -> DatabaseInterface:
    """Build a DatabaseInterface instance from flags."""
    overrides: dict[str, str] = {}
    if env_file:
        overrides = load_env_file(env_file)

    secrets = EnvSecrets(overrides=overrides)
    impl_cls = resolve_implementation("db", impl_name)

    # Check if constructor needs secrets
    import inspect
    from typing import get_type_hints

    try:
        hints = get_type_hints(impl_cls.__init__)
        hints.pop("return", None)
    except Exception:
        hints = {}

    if hints:
        from de_platform.config.container import Container
        from de_platform.services.secrets.interface import SecretsInterface

        container = Container()
        container.register_instance(SecretsInterface, secrets)
        return container.resolve(impl_cls)  # type: ignore[return-value]
    return impl_cls()  # type: ignore[return-value]


def run_migrate(argv: list[str]) -> int:
    """Entry point for the migrate subcommand."""
    args = _parse_migrate_args(argv)
    command = args["command"]

    if command == "create":
        return _create_migration(args["name"])

    # All other commands need a database
    db_impl = args["impl_flags"].get("db", "memory")
    db = _build_db(db_impl, args["env_file"])
    db.connect()

    try:
        runner = MigrationRunner(db)

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


def _create_migration(name: str | None) -> int:
    """Create a new migration file from template."""
    if not name:
        print("Usage: python -m de_platform migrate create <name>", file=sys.stderr)
        return 1

    # Find next number
    existing = sorted(VERSIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))
    if existing:
        last_num = int(existing[-1].name[:3])
        next_num = last_num + 1
    else:
        next_num = 1

    filename = f"{next_num:03d}_{name}.py"
    filepath = VERSIONS_DIR / filename

    filepath.write_text(f'''"""{name.replace("_", " ").title()}."""

from de_platform.services.database.interface import DatabaseInterface


def up(db: DatabaseInterface) -> None:
    pass


def down(db: DatabaseInterface) -> None:
    pass
''')

    print(f"Created: {filepath}")
    return 0
