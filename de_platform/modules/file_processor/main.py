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
from datetime import datetime
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.audit_accumulator import AuditAccumulator
from de_platform.pipeline.serialization import _now_iso, error_to_dict
from de_platform.pipeline.topics import (
    AUDIT_FILE_UPLOADS,
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.pipeline.validation import group_errors_by_event, validate_events
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface

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
        metrics: MetricsInterface,
        db: DatabaseInterface | None = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.fs = fs
        self.mq = mq
        self.metrics = metrics
        self.db = db

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.file_path: str = self.config.get("file-path", "")
        self.event_type: str = self.config.get("event-type", "")
        self._audit = AuditAccumulator(self.mq, source="file")

    async def validate(self) -> None:
        if not self.file_path:
            self.log.error("Validation failed: file-path is required", module="file_processor")
            raise ValueError("file-path is required")
        if self.event_type not in _TOPIC_MAP:
            self.log.error(
                "Validation failed: invalid event-type",
                module="file_processor",
                event_type=self.event_type,
            )
            raise ValueError(
                f"event-type must be one of {list(_TOPIC_MAP)}, got {self.event_type!r}"
            )

    async def execute(self) -> int:
        file_id = uuid.uuid4().hex

        # Write initial file_audit record
        if self.db is not None:
            await self.db.insert_one_async("file_audit", {
                "file_id": file_id,
                "tenant_id": "unknown",
                "file_name": self.file_path,
                "event_type": self.event_type,
                "total_count": 0,
                "processed_count": 0,
                "error_count": 0,
                "duplicate_count": 0,
                "status": "processing",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })

        try:
            raw_bytes = self.fs.read(self.file_path)
            events = _parse_events(raw_bytes)

            if not events:
                self.log.info(
                    "No events found in file",
                    file_path=self.file_path,
                    event_type=self.event_type,
                )
                if self.db is not None:
                    await self._update_file_audit(
                        file_id, "unknown", 0, 0, 0, "completed",
                    )
                return 0

            valid, errors = validate_events(self.event_type, events)

            topic = _TOPIC_MAP[self.event_type]
            for raw in valid:
                msg = dict(raw)
                msg["message_id"] = uuid.uuid4().hex
                msg["ingested_at"] = _now_iso()
                msg["event_type"] = self.event_type
                msg["ingestion_method"] = "file"
                tenant_id = msg.get("tenant_id", "")
                symbol = msg.get("symbol", "")
                msg_key = f"{tenant_id}:{symbol}" if tenant_id else None
                self.mq.publish(topic, msg, key=msg_key)

            for event_index, event_errors in group_errors_by_event(errors).items():
                raw_event = events[event_index] if event_index < len(events) else {}
                err_msg = error_to_dict(raw_event, self.event_type, event_errors)
                tenant_id = raw_event.get("tenant_id", "")
                msg_key = f"{tenant_id}:" if tenant_id else None
                self.mq.publish(NORMALIZATION_ERRORS, err_msg, key=msg_key)

            # Audit counting
            tenant_counts: dict[str, int] = {}
            for raw in valid:
                tid = raw.get("tenant_id", "unknown")
                tenant_counts[tid] = tenant_counts.get(tid, 0) + 1
            for tid, count in tenant_counts.items():
                self._audit.count(tid, self.event_type, count)
            self._audit.flush()

            # Update file_audit record
            if self.db is not None:
                tenant_ids = {e.get("tenant_id", "unknown") for e in events}
                file_tenant = tenant_ids.pop() if len(tenant_ids) == 1 else "mixed"
                await self._update_file_audit(
                    file_id, file_tenant, len(events), len(valid),
                    len({e.event_index for e in errors}), "completed",
                )

            self.metrics.counter("events_ingested_total", value=float(len(valid)), tags={"service": "file_processor", "event_type": self.event_type, "method": "file"})
            if errors:
                self.metrics.counter("events_errors_total", value=float(len(errors)), tags={"service": "file_processor", "event_type": self.event_type})

            # Publish file upload record to audit_file_uploads topic
            tenant_ids_for_audit = {e.get("tenant_id", "unknown") for e in events}
            file_tenant_audit = tenant_ids_for_audit.pop() if len(tenant_ids_for_audit) == 1 else "mixed"
            self.mq.publish(AUDIT_FILE_UPLOADS, {
                "tenant_id": file_tenant_audit,
                "event_type": self.event_type,
                "ingestion_method": "file",
                "file_name": self.file_path,
                "event_count": len(valid),
                "uploaded_at": _now_iso(),
            })

            self.log.info(
                "File processed",
                file_path=self.file_path,
                event_type=self.event_type,
                valid_count=len(valid),
                error_count=len(errors),
            )
            return 0

        except Exception:
            if self.db is not None:
                await self._update_file_audit(
                    file_id, "unknown", 0, 0, 0, "failed",
                )
            raise

    async def _update_file_audit(
        self,
        file_id: str,
        tenant_id: str,
        total: int,
        processed: int,
        error_count: int,
        status: str,
    ) -> None:
        assert self.db is not None
        await self.db.execute_async(
            "DELETE FROM file_audit WHERE file_id = $1", [file_id],
        )
        await self.db.insert_one_async("file_audit", {
            "file_id": file_id,
            "tenant_id": tenant_id,
            "file_name": self.file_path,
            "event_type": self.event_type,
            "total_count": total,
            "processed_count": processed,
            "error_count": error_count,
            "duplicate_count": 0,
            "status": status,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        })


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
