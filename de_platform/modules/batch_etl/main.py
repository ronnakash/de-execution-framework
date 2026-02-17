"""Batch ETL module: reads raw data, validates, transforms, deduplicates, writes to DB."""

from __future__ import annotations

import json

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.modules.batch_etl.transformer import compute_dedup_key, transform_events
from de_platform.modules.batch_etl.validator import validate_events
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface


class BatchEtlModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        fs: FileSystemInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.fs = fs

    def initialize(self) -> None:
        self.log = self.logger.create()
        self.db.connect()

    def execute(self) -> int:
        date = self.config.get("date")
        source_path = self.config.get("source-path")
        target_table = self.config.get("target-table", "cleaned_events")
        batch_size = self.config.get("batch-size", 500)
        dry_run = self.config.get("dry-run", False)

        self.log.info(
            "Starting Batch ETL",
            date=date,
            source_path=source_path,
            target_table=target_table,
            dry_run=dry_run,
        )

        # Step 1: Read raw data
        raw_events = self._read_raw_data(source_path, date)
        if not raw_events:
            self.log.warn("No raw data found", date=date, source_path=source_path)
            return 0

        # Step 2: Validate
        valid_events, invalid_count = validate_events(raw_events, self.log)

        # Step 3: Transform
        transformed = transform_events(valid_events, date)

        # Step 4: Deduplicate
        unique_events, dup_count = self._deduplicate(transformed)
        self.log.info(
            "Deduplication complete",
            before=len(transformed),
            after=len(unique_events),
            duplicates=dup_count,
        )

        # Step 5: Write to database
        inserted = 0
        if not dry_run and unique_events:
            inserted = self._write_to_db(target_table, unique_events, batch_size)

        # Step 6: Summary
        self.log.info(
            "Batch ETL complete",
            raw=len(raw_events),
            valid=len(valid_events),
            unique=len(unique_events),
            inserted=inserted,
            invalid=invalid_count,
            duplicates=dup_count,
        )

        return 0

    def teardown(self) -> None:
        self.db.disconnect()

    def _read_raw_data(self, source_path: str, date: str) -> list[dict]:
        """Read JSONL files from {source_path}/{date}/ in the file system."""
        prefix = f"{source_path}/{date}"
        files = self.fs.list(prefix)
        if not files:
            return []

        self.log.info("Reading raw data", files=len(files), prefix=prefix)
        raw_events: list[dict] = []
        for file_path in files:
            data = self.fs.read(file_path)
            for line in data.decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        raw_events.append(json.loads(line))
                    except json.JSONDecodeError:
                        self.log.error("Malformed JSON line", file=file_path, line=line[:100])

        self.log.info("Raw data loaded", total_records=len(raw_events))
        return raw_events

    def _deduplicate(self, events: list[dict]) -> tuple[list[dict], int]:
        """Deduplicate events by content hash. Returns (unique_events, duplicate_count)."""
        seen: set[str] = set()
        unique: list[dict] = []
        for event in events:
            key = compute_dedup_key(event)
            if key not in seen:
                seen.add(key)
                unique.append(event)
        return unique, len(events) - len(unique)

    def _write_to_db(self, table: str, events: list[dict], batch_size: int) -> int:
        """Write events to database in batches. Returns total rows inserted."""
        total = 0
        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]
            count = self.db.bulk_insert(table, batch)
            total += count
        self.log.info("Batch write complete", table=table, total_inserted=total)
        return total


module_class = BatchEtlModule
