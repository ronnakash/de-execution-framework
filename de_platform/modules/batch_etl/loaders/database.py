"""Database loader: bulk-inserts items or batches into a database table."""

from __future__ import annotations

from typing import Any

from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.shared.etl.interfaces import Loader


class DatabaseLoader(Loader):
    """Loads dicts or lists of dicts into a database table using bulk insert.

    Expected params::

        {
            "table": "cleaned_events"   # target table name (required)
        }

    When used after a ``.batch(N)`` stage the loader receives a ``list[dict]``.
    When used without batching it receives individual ``dict`` items.

    Rows with ``_dedup_key`` field have that key stripped before insert
    (it's only used internally for deduplication logic).
    """

    processing_method = "inline"
    workers = 1

    def __init__(self, db: DatabaseInterface, logger: LoggerFactory) -> None:
        self.db = db
        self.log = logger.create()
        self.params: dict[str, Any] = {}
        self._inserted = 0
        self.db.connect()

    def load(self, item: Any) -> None:
        table = self.params.get("table", "cleaned_events")

        if isinstance(item, list):
            rows = [self._clean_row(r) for r in item if r is not None]
        elif item is not None:
            rows = [self._clean_row(item)]
        else:
            return

        if not rows:
            return

        count = self.db.bulk_insert(table, rows)
        self._inserted += count
        self.log.info("Inserted batch", table=table, count=count, total=self._inserted)

    @staticmethod
    def _clean_row(row: dict) -> dict:
        """Strip internal-only fields before persisting."""
        return {k: v for k, v in row.items() if not k.startswith("_")}
