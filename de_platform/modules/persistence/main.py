"""Persistence Service.

Reads events from Kafka persistence, duplicates, and errors topics.
Buffers rows per (tenant, table) and flushes to ClickHouse + filesystem
when the size threshold or time interval is reached.

Topics consumed:
    orders_persistence / executions_persistence / transactions_persistence
    duplicates
    normalization_errors

Each flush writes:
  - ClickHouse bulk insert via DatabaseInterface.bulk_insert()
  - JSONL file to FileSystemInterface at path: {tenant_id}/{table}/{uuid}.jsonl
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import AsyncModule
from de_platform.modules.persistence.buffer import BufferKey, TenantBuffer
from de_platform.pipeline.topics import (
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE,
)
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface

_TOPIC_TABLE_MAP: list[tuple[str, str]] = [
    (ORDERS_PERSISTENCE, "orders"),
    (EXECUTIONS_PERSISTENCE, "executions"),
    (TRANSACTIONS_PERSISTENCE, "transactions"),
    (DUPLICATES, "duplicates"),
    (NORMALIZATION_ERRORS, "normalization_errors"),
]


class PersistenceModule(AsyncModule):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        db: DatabaseInterface,
        fs: FileSystemInterface,
        lifecycle: LifecycleManager,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.db = db
        self.fs = fs
        self.lifecycle = lifecycle
        self.flush_interval: int = 60
        self.flush_threshold: int = 100_000
        self.buffer: TenantBuffer | None = None

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.flush_interval = int(self.config.get("flush-interval", 60))
        self.flush_threshold = int(self.config.get("flush-threshold", 100_000))
        self.db.connect()
        self.buffer = TenantBuffer(self.flush_interval, self.flush_threshold)
        self.lifecycle.on_shutdown(self._flush_all)
        self.lifecycle.on_shutdown(self.db.disconnect)
        self.log.info(
            "Persistence initialized",
            flush_interval=self.flush_interval,
            flush_threshold=self.flush_threshold,
        )

    async def execute(self) -> int:
        assert self.buffer is not None
        self.log.info("Persistence running")
        while not self.lifecycle.is_shutting_down:
            for topic, table in _TOPIC_TABLE_MAP:
                msg = self.mq.consume_one(topic)
                if msg:
                    tenant_id = msg.get("tenant_id", "unknown")
                    key = BufferKey(tenant_id=tenant_id, table=table)
                    self.buffer.append(key, msg)
                    if self.buffer.should_flush(key):
                        self._flush(key)
            # Time-based flush check for all buffered keys
            for key in self.buffer.all_keys():
                if self.buffer.should_flush(key):
                    self._flush(key)
            await asyncio.sleep(0.01)
        return 0

    def _flush(self, key: BufferKey) -> None:
        assert self.buffer is not None
        rows = self.buffer.drain(key)
        if rows:
            self._flush_rows(key, rows)

    def _flush_rows(self, key: BufferKey, rows: list[dict[str, Any]]) -> None:
        # Write to database (ClickHouse in production)
        self.db.bulk_insert(key.table, rows)
        # Write to filesystem as JSONL, one file per flush
        path = f"{key.tenant_id}/{key.table}/{uuid.uuid4().hex[:12]}.jsonl"
        data = "\n".join(json.dumps(r) for r in rows).encode()
        self.fs.write(path, data)
        self.log.info(
            "Flushed",
            tenant=key.tenant_id,
            table=key.table,
            count=len(rows),
            path=path,
        )

    def _flush_all(self) -> None:
        """Drain all remaining buffers — called on shutdown."""
        if self.buffer is None:
            return
        for key, rows in self.buffer.drain_all().items():
            if rows:
                self._flush_rows(key, rows)
        self.log.info("All buffers flushed on shutdown")


module_class = PersistenceModule
