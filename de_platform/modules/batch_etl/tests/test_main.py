"""Tests for the rewritten generic Batch ETL module."""

from __future__ import annotations

import importlib
import asyncio
import json
from pathlib import Path

from de_platform.cli.runner import (
    _build_container,
    _extract_global_flags,
    load_module_descriptor,
    parse_module_args,
)
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.database.interface import DatabaseInterface

FIXTURES = Path(__file__).parent / "fixtures"

_EXTRACTOR = "de_platform.modules.batch_etl.extractors.filesystem.FileSystemExtractor"
_TRANSFORMER = (
    "de_platform.modules.batch_etl.transformers.event_normalizer.EventNormalizerTransformer"
)
_LOADER = "de_platform.modules.batch_etl.loaders.database.DatabaseLoader"


def _run_etl(
    extractor_params: dict | None = None,
    transformer_params: dict | None = None,
    loader_params: dict | None = None,
    extra_flags: list[str] | None = None,
    fs_data: dict[str, bytes] | None = None,
) -> tuple[int, object]:
    """Run batch_etl with memory implementations, returning (exit_code, module)."""
    ep = json.dumps(extractor_params or {})
    tp = json.dumps(transformer_params or {})
    lp = json.dumps(loader_params or {"table": "cleaned_events"})

    args = [
        "run", "batch_etl",
        "--db", "memory",
        "--fs", "memory",
        "--log", "memory",
        "--extractor", _EXTRACTOR,
        "--transformers", _TRANSFORMER,
        "--loader", _LOADER,
        "--extractor-params", ep,
        "--transformer-params", tp,
        "--loader-params", lp,
    ]
    if extra_flags:
        args.extend(extra_flags)

    descriptor = load_module_descriptor("batch_etl")
    impl_flags, env_overrides, filtered_args, db_entries, _hp = _extract_global_flags(args[2:])
    module_args = parse_module_args(descriptor, filtered_args)
    container = _build_container(impl_flags, env_overrides, module_args, db_entries)

    # Seed filesystem data
    if fs_data:
        fs = container._registry[FileSystemInterface]
        for path, data in fs_data.items():
            fs.write(path, data)

    mod = importlib.import_module("de_platform.modules.batch_etl.main")
    module_instance = container.resolve(mod.module_class)
    exit_code = asyncio.run(module_instance.run())

    return exit_code, module_instance


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_run_success():
    """Valid events are extracted, normalized, and inserted into the database."""
    data = _fixture("valid_events.jsonl")
    ext_params = {"path": "raw/events", "date": "2026-01-15"}
    t_params = {"date": "2026-01-15"}

    exit_code, module = _run_etl(
        extractor_params=ext_params,
        transformer_params=t_params,
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data},
    )
    assert exit_code == 0

    # Verify rows were inserted via the container's DB
    from de_platform.config.container import Container
    db: DatabaseInterface = module.container._registry[DatabaseInterface]
    db.connect()
    rows = db.fetch_all("SELECT * FROM cleaned_events")
    assert len(rows) == 10


def test_run_no_data():
    """When no files exist for the given prefix, the module exits cleanly."""
    exit_code, _ = _run_etl(
        extractor_params={"path": "raw/events", "date": "2026-01-15"},
        transformer_params={"date": "2026-01-15"},
    )
    assert exit_code == 0


def test_run_dry_run():
    """In dry-run mode, items are processed but the loader is skipped."""
    data = _fixture("valid_events.jsonl")
    ext_params = {"path": "raw/events", "date": "2026-01-15"}
    t_params = {"date": "2026-01-15"}

    exit_code, module = _run_etl(
        extractor_params=ext_params,
        transformer_params=t_params,
        extra_flags=["--dry-run", "true"],
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data},
    )
    assert exit_code == 0

    from de_platform.services.database.interface import DatabaseInterface
    db: DatabaseInterface = module.container._registry[DatabaseInterface]
    db.connect()
    rows = db.fetch_all("SELECT * FROM cleaned_events")
    assert len(rows) == 0


def test_run_batched():
    """Events are batched before reaching the loader."""
    data = _fixture("valid_events.jsonl")
    ext_params = {"path": "raw/events", "date": "2026-01-15"}
    t_params = {"date": "2026-01-15"}

    exit_code, _ = _run_etl(
        extractor_params=ext_params,
        transformer_params=t_params,
        extra_flags=["--batch-size", "5"],
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data},
    )
    assert exit_code == 0


def test_run_invalid_events_dropped():
    """Events missing required fields are dropped by the normalizer."""
    data = _fixture("mixed_events.jsonl")
    ext_params = {"path": "raw/events", "date": "2026-01-15"}
    t_params = {"date": "2026-01-15"}

    exit_code, module = _run_etl(
        extractor_params=ext_params,
        transformer_params=t_params,
        fs_data={"raw/events/2026-01-15/batch_0.jsonl": data},
    )
    assert exit_code == 0

    from de_platform.services.database.interface import DatabaseInterface
    db: DatabaseInterface = module.container._registry[DatabaseInterface]
    db.connect()
    rows = db.fetch_all("SELECT * FROM cleaned_events")
    # mixed_events has 10 records of which 7 are valid
    assert len(rows) == 7
