from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class CacheInterface(ABC):
    """Key-value caching with TTL support."""

    @abstractmethod
    def get(self, key: str) -> Any | None: ...

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int | None = None) -> None: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def flush(self) -> None: ...

    @abstractmethod
    def health_check(self) -> bool: ...
