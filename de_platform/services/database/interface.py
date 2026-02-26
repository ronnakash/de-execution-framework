from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DatabaseInterface(ABC):
    """Abstract interface for relational and document-style data access."""

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def is_connected(self) -> bool: ...

    @abstractmethod
    def execute(self, query: str, params: list[Any] | None = None) -> int:
        """Execute a query, return rows affected."""
        ...

    @abstractmethod
    def fetch_one(self, query: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        """Fetch a single row as a dict. Returns None if no rows."""
        ...

    @abstractmethod
    def fetch_all(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all rows as a list of dicts."""
        ...

    @abstractmethod
    def insert_one(self, table: str, row: dict[str, Any]) -> int:
        """Insert a single row (partial columns allowed). Return rows affected."""
        ...

    @abstractmethod
    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert multiple rows. Return count inserted."""
        ...

    @abstractmethod
    def health_check(self) -> bool: ...

    # -- Async wrappers -------------------------------------------------------
    # Concrete defaults so callers can always use the async API.
    # Subclasses with a native async implementation (e.g. PostgresDatabase)
    # should override these directly.

    async def execute_async(self, query: str, params: list[Any] | None = None) -> int:
        """Async execute — default delegates to the sync version."""
        return self.execute(query, params)

    async def fetch_one_async(
        self, query: str, params: list[Any] | None = None
    ) -> dict[str, Any] | None:
        """Async fetch_one — default delegates to the sync version."""
        return self.fetch_one(query, params)

    async def fetch_all_async(
        self, query: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Async fetch_all — default delegates to the sync version."""
        return self.fetch_all(query, params)

    async def insert_one_async(self, table: str, row: dict[str, Any]) -> int:
        """Async insert_one — default delegates to the sync version."""
        return self.insert_one(table, row)

    async def bulk_insert_async(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Async bulk_insert — default delegates to the sync version."""
        return self.bulk_insert(table, rows)

    async def connect_async(self) -> None:
        """Async connect — default delegates to the sync version."""
        self.connect()

    async def disconnect_async(self) -> None:
        """Async disconnect — default delegates to the sync version."""
        self.disconnect()
