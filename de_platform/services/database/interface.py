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
    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert multiple rows. Return count inserted."""
        ...

    @abstractmethod
    def health_check(self) -> bool: ...
