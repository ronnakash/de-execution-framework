"""File Processor job module.

Reads a JSON array or JSONL file of events, validates them, and publishes
valid events to the appropriate normalization topic. Invalid events are
published to the ``normalization_errors`` topic.

Supported file formats:
    JSON array  — ``[{...}, {...}]``
    JSONL       — one JSON object per line

Args:
    file-path:  Path to the file (read via FileSystemInterface)
    event-type: ``"order"`` | ``"execution"`` | ``"transaction"``
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.serialization import _now_iso, error_to_dict
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.pipeline.validation import group_errors_by_event, validate_events
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface

_TOPIC_MAP: dict[str, str] = {
    "order": TRADE_NORMALIZATION,
    "execution": TRADE_NORMALIZATION,
    "transaction": TX_NORMALIZATION,
}


class FileProcessorModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        fs: FileSystemInterface,
        mq: MessageQueueInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.fs = fs
        self.mq = mq

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.file_path: str = self.config.get("file-path", "")
        self.event_type: str = self.config.get("event-type", "")

    async def validate(self) -> None:
        if not self.file_path:
            raise ValueError("file-path is required")
        if self.event_type not in _TOPIC_MAP:
            raise ValueError(
                f"event-type must be one of {list(_TOPIC_MAP)}, got {self.event_type!r}"
            )

    async def execute(self) -> int:
        raw_bytes = self.fs.read(self.file_path)
        events = _parse_events(raw_bytes)

        if not events:
            self.log.info("No events found in file", path=self.file_path)
            return 0

        valid, errors = validate_events(self.event_type, events)

        topic = _TOPIC_MAP[self.event_type]
        for raw in valid:
            msg = dict(raw)
            msg["message_id"] = uuid.uuid4().hex
            msg["ingested_at"] = _now_iso()
            msg["event_type"] = self.event_type
            self.mq.publish(topic, msg)

        for event_index, event_errors in group_errors_by_event(errors).items():
            raw_event = events[event_index] if event_index < len(events) else {}
            err_msg = error_to_dict(raw_event, self.event_type, event_errors)
            self.mq.publish(NORMALIZATION_ERRORS, err_msg)

        self.log.info(
            "File processed",
            path=self.file_path,
            event_type=self.event_type,
            accepted=len(valid),
            rejected=len(errors),
        )
        return 0


def _parse_events(data: bytes) -> list[dict[str, Any]]:
    """Parse JSON array or JSONL bytes into a list of dicts."""
    text = data.decode("utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        return json.loads(text)
    # JSONL — one JSON object per line
    return [json.loads(line) for line in text.splitlines() if line.strip()]


module_class = FileProcessorModule
