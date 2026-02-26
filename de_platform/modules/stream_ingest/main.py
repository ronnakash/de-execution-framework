"""Stream Ingest module: consumes from a message queue, batches to filesystem, records metadata."""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface


class StreamIngestModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        fs: FileSystemInterface,
        db: DatabaseInterface,
        lifecycle: LifecycleManager,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.fs = fs
        self.db = db
        self.lifecycle = lifecycle
        self._buffer: list[dict] = []
        self._last_flush_time = time.monotonic()

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.topic = self.config.get("topic")
        self.output_prefix = self.config.get("output-prefix")
        self.batch_size = self.config.get("batch-size", 1000)
        self.flush_interval = self.config.get("flush-interval", 60)

        self.db.connect()
        self.mq.subscribe(self.topic, self._on_message)

        self.lifecycle.on_shutdown(self._flush_buffer)
        self.lifecycle.on_shutdown(self.db.disconnect)

        self.log.info(
            "Stream Ingest initialized",
            topic=self.topic,
            output_prefix=self.output_prefix,
            batch_size=self.batch_size,
            flush_interval=self.flush_interval,
        )

    async def execute(self) -> int:
        """Main loop: poll for messages, buffer, flush on size/time threshold."""
        while not self.lifecycle.is_shutting_down:
            try:
                msg = self.mq.consume_one(self.topic)
                if msg is not None:
                    self._buffer.append(msg)
                else:
                    # Yield to event loop when no messages available
                    await asyncio.sleep(0.01)

                if len(self._buffer) >= self.batch_size:
                    self._flush_buffer()
                elif self._buffer and self._time_to_flush():
                    self._flush_buffer()
            except Exception as exc:
                self.log.error("Processing error", error=str(exc))
                await asyncio.sleep(0.01)

        return 0

    async def teardown(self) -> None:
        # Final flush handled by lifecycle shutdown hooks
        pass

    def _on_message(self, message: dict) -> None:
        """Handler called by subscribe — not used for consume_one polling pattern."""
        pass

    def _time_to_flush(self) -> bool:
        return (time.monotonic() - self._last_flush_time) >= self.flush_interval

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer[:]
        self._buffer.clear()
        self._last_flush_time = time.monotonic()

        batch_id = uuid.uuid4().hex[:12]
        filename = f"{self.output_prefix}/{batch_id}.jsonl"
        data = "\n".join(json.dumps(e) for e in batch).encode("utf-8")
        self.fs.write(filename, data)

        self.db.bulk_insert("ingest_batches", [
            {"filename": filename, "event_count": len(batch)},
        ])

        self.log.info("Flushed batch", filename=filename, event_count=len(batch))


module_class = StreamIngestModule
