import re
from typing import Any

from de_platform.services.database.interface import DatabaseInterface


class MemoryDatabase(DatabaseInterface):
    """In-memory database for unit testing. Supports bulk_insert/fetch_all/fetch_one
    operations on named tables stored as lists of dicts."""

    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._connected = False

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def execute(self, query: str, params: list[Any] | None = None) -> int:
        self._check_connected()
        # Support DELETE FROM <table> WHERE <col> = $1
        delete_match = re.match(
            r"(?i)DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*\$1", query.strip()
        )
        if delete_match and params:
            table = delete_match.group(1)
            col = delete_match.group(2)
            val = params[0]
            rows = self._tables.get(table, [])
            before = len(rows)
            self._tables[table] = [r for r in rows if r.get(col) != val]
            return before - len(self._tables[table])
        # For DDL and other statements, no-op
        return 0

    def fetch_one(self, query: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        self._check_connected()
        rows = self._query(query, params)
        return rows[0] if rows else None

    def fetch_all(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        self._check_connected()
        return self._query(query, params)

    def insert_one(self, table: str, row: dict[str, Any]) -> int:
        self._check_connected()
        if table not in self._tables:
            self._tables[table] = []
        self._tables[table].append(row)
        return 1

    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        self._check_connected()
        if table not in self._tables:
            self._tables[table] = []
        self._tables[table].extend(rows)
        return len(rows)

    def health_check(self) -> bool:
        return self._connected

    def _check_connected(self) -> None:
        if not self._connected:
            raise RuntimeError("Database is not connected. Call connect() first.")

    def _query(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Minimal query parser for SELECT * FROM <table> [WHERE <col> = $1]."""
        q = query.strip().upper()
        match = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)", q)
        if not match:
            return []
        table = match.group(1).lower()
        # Also try the original case from the query
        raw_match = re.match(r"(?i)SELECT\s+\*\s+FROM\s+(\w+)", query.strip())
        table_name = raw_match.group(1) if raw_match else table
        rows = self._tables.get(table_name, self._tables.get(table, []))

        # Simple WHERE col = $1 support
        where_match = re.search(r"(?i)WHERE\s+(\w+)\s*=\s*\$1", query)
        if where_match and params:
            col = where_match.group(1)
            val = params[0]
            rows = [r for r in rows if r.get(col) == val]

        return list(rows)
