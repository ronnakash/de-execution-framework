"""Tests for the Stream Ingest module using memory implementations."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json

import pytest

from de_platform.cli.runner import (
    _build_container,
    _extract_global_flags,
    load_module_descriptor,
    parse_module_args,
)
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.memory_logger import MemoryLogger
from de_platform.services.message_queue.interface import MessageQueueInterface


def _build_stream_ingest(
    topic: str = "events",
    output_prefix: str = "output/batches",
    batch_size: int = 5,
    flush_interval: int = 60,
    extra_flags: list[str] | None = None,
):
    """Build a StreamIngestModule with memory implementations and return (module, container)."""
    args = [
        "run", "stream_ingest",
        "--db", "memory",
        "--fs", "memory",
        "--mq", "memory",
        "--log", "memory",
        "--topic", topic,
        "--output-prefix", output_prefix,
        "--batch-size", str(batch_size),
        "--flush-interval", str(flush_interval),
    ]
    if extra_flags:
        args.extend(extra_flags)

    descriptor = load_module_descriptor("stream_ingest")
    impl_flags, env_overrides, filtered_args, db_entries, _hp = _extract_global_flags(args[2:])
    module_args = parse_module_args(descriptor, filtered_args)
    container = _build_container(impl_flags, env_overrides, module_args, db_entries)

    # Register lifecycle manager
    lifecycle = LifecycleManager()
    container.register_instance(LifecycleManager, lifecycle)

    mod = importlib.import_module("de_platform.modules.stream_ingest.main")
    module_instance = container.resolve(mod.module_class)
    return module_instance, container, lifecycle


@pytest.mark.asyncio
async def test_consumes_and_flushes_on_batch_size():
    module, container, lifecycle = _build_stream_ingest(batch_size=5)
    mq = container._registry[MessageQueueInterface]

    # Publish 5 events (equals batch_size)
    for i in range(5):
        mq.publish("events", {"event_type": "click", "id": i})

    await module.initialize()

    # Run execute in a task, and stop after a short delay
    async def run_and_stop():
        await asyncio.sleep(0.05)
        lifecycle._shutting_down = True

    await asyncio.gather(module.execute(), run_and_stop())

    # Check filesystem has the batch file
    fs = container._registry[FileSystemInterface]
    files = fs.list("output/batches")
    assert len(files) == 1
    data = fs.read(files[0]).decode("utf-8")
    events = [json.loads(line) for line in data.strip().splitlines()]
    assert len(events) == 5


@pytest.mark.asyncio
async def test_shutdown_flushes_remaining():
    module, container, lifecycle = _build_stream_ingest(batch_size=100)
    mq = container._registry[MessageQueueInterface]

    # Publish 3 events (less than batch_size)
    for i in range(3):
        mq.publish("events", {"event_type": "click", "id": i})

    await module.initialize()

    # Run briefly, then shutdown
    async def run_and_stop():
        await asyncio.sleep(0.05)
        await lifecycle.shutdown()

    await asyncio.gather(module.execute(), run_and_stop())

    # Shutdown hook should have flushed the buffer
    fs = container._registry[FileSystemInterface]
    files = fs.list("output/batches")
    assert len(files) == 1
    data = fs.read(files[0]).decode("utf-8")
    events = [json.loads(line) for line in data.strip().splitlines()]
    assert len(events) == 3


@pytest.mark.asyncio
async def test_metadata_recorded_in_db():
    module, container, lifecycle = _build_stream_ingest(batch_size=3)
    mq = container._registry[MessageQueueInterface]
    db = container._registry[DatabaseInterface]

    for i in range(3):
        mq.publish("events", {"event_type": "click", "id": i})

    await module.initialize()

    async def run_and_stop():
        await asyncio.sleep(0.05)
        lifecycle._shutting_down = True

    await asyncio.gather(module.execute(), run_and_stop())

    # Check ingest_batches table
    db.connect()
    rows = db.fetch_all("SELECT * FROM ingest_batches")
    assert len(rows) == 1
    assert rows[0]["event_count"] == 3


@pytest.mark.asyncio
async def test_empty_buffer_no_flush():
    module, container, lifecycle = _build_stream_ingest(batch_size=5)

    await module.initialize()

    async def run_and_stop():
        await asyncio.sleep(0.05)
        await lifecycle.shutdown()

    await asyncio.gather(module.execute(), run_and_stop())

    fs = container._registry[FileSystemInterface]
    files = fs.list("output/batches")
    assert len(files) == 0


@pytest.mark.asyncio
async def test_batch_id_uniqueness():
    module, container, lifecycle = _build_stream_ingest(batch_size=2)
    mq = container._registry[MessageQueueInterface]

    # Publish 4 events -> should produce 2 batches of 2
    for i in range(4):
        mq.publish("events", {"event_type": "click", "id": i})

    await module.initialize()

    async def run_and_stop():
        await asyncio.sleep(0.05)
        lifecycle._shutting_down = True

    await asyncio.gather(module.execute(), run_and_stop())

    fs = container._registry[FileSystemInterface]
    files = fs.list("output/batches")
    assert len(files) == 2
    assert files[0] != files[1]
