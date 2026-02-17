"""Integration tests for the Batch ETL module using memory implementations."""

from __future__ import annotations

import json
from pathlib import Path

from de_platform.cli.runner import run_module
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.logger.memory_logger import MemoryLogger

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _run_batch_etl(
    date: str = "2026-01-15",
    source_path: str = "raw/events",
    extra_flags: list[str] | None = None,
    fs_data: dict[str, bytes] | None = None,
) -> tuple[int, object]:
    """Helper to run batch_etl with memory implementations.

    Writes fs_data into the MemoryFileSystem before running.
    Returns (exit_code, module_instance).
    """
    args = [
        "run", "batch_etl",
        "--db", "memory",
        "--fs", "memory",
        "--log", "memory",
        "--date", date,
        "--source-path", source_path,
    ]
    if extra_flags:
        args.extend(extra_flags)

    # We need to pre-populate the MemoryFileSystem. Since run_module() creates
    # the container internally, we run it and rely on the module reading from fs.
    # To seed data, we write files into the memory filesystem via the container
    # before execution. We'll do this by running run_module, but we need the fs
    # to already have data.
    #
    # Approach: patch run_module flow by calling the internals ourselves.
    from de_platform.cli.runner import (
        _build_container,
        _extract_global_flags,
        load_module_descriptor,
        parse_module_args,
    )
    import importlib
    import inspect

    descriptor = load_module_descriptor("batch_etl")
    impl_flags, env_overrides, filtered_args = _extract_global_flags(args[2:])
    module_args = parse_module_args(descriptor, filtered_args)
    container = _build_container(impl_flags, env_overrides, module_args)

    # Seed filesystem data
    if fs_data:
        from de_platform.services.filesystem.interface import FileSystemInterface

        fs = container._registry[FileSystemInterface]
        for path, data in fs_data.items():
            fs.write(path, data)

    # Import and resolve module
    mod = importlib.import_module("de_platform.modules.batch_etl.main")
    module_instance = container.resolve(mod.module_class)
    result = module_instance.run()
    if inspect.iscoroutine(result):
        import asyncio
        exit_code = asyncio.run(result)
    else:
        exit_code = result

    return exit_code, module_instance


def test_run_success():
    data = _load_fixture("valid_events.jsonl")
    exit_code, module = _run_batch_etl(
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data}
    )
    assert exit_code == 0
    # Reconnect to query (teardown disconnects)
    module.db.connect()
    rows = module.db.fetch_all("SELECT * FROM cleaned_events")
    assert len(rows) == 10
    # Check log has summary
    assert isinstance(module.log, MemoryLogger)
    assert any("Batch ETL complete" in m for m in module.log.messages)


def test_run_no_data():
    exit_code, module = _run_batch_etl()
    assert exit_code == 0
    assert isinstance(module.log, MemoryLogger)
    assert any("No raw data found" in m for m in module.log.messages)


def test_run_dry_run():
    data = _load_fixture("valid_events.jsonl")
    exit_code, module = _run_batch_etl(
        extra_flags=["--dry-run", "true"],
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data},
    )
    assert exit_code == 0
    module.db.connect()
    rows = module.db.fetch_all("SELECT * FROM cleaned_events")
    assert len(rows) == 0


def test_run_deduplication():
    data = _load_fixture("duplicate_events.jsonl")
    exit_code, module = _run_batch_etl(
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data}
    )
    assert exit_code == 0
    module.db.connect()
    rows = module.db.fetch_all("SELECT * FROM cleaned_events")
    # 5 events, 2 duplicates -> 3 unique
    assert len(rows) == 3


def test_run_invalid_data():
    data = _load_fixture("mixed_events.jsonl")
    exit_code, module = _run_batch_etl(
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data}
    )
    assert exit_code == 0
    module.db.connect()
    # 10 events: 7 valid, 3 invalid -> 7 unique written
    rows = module.db.fetch_all("SELECT * FROM cleaned_events")
    assert len(rows) == 7
    # Should have logged validation warnings
    assert isinstance(module.log, MemoryLogger)
    assert any("Validation complete" in m for m in module.log.messages)
