import time
from typing import Any

from de_platform.services.cache.interface import CacheInterface


class MemoryCache(CacheInterface):
    """In-memory cache with TTL support for unit testing."""

    def __init__(self) -> None:
        # key -> (value, expiry_timestamp_or_none)
        self._data: dict[str, tuple[Any, float | None]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expiry = entry
        if expiry is not None and time.monotonic() >= expiry:
            del self._data[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        expiry = time.monotonic() + ttl if ttl is not None else None
        self._data[key] = (value, expiry)

    def delete(self, key: str) -> bool:
        if key in self._data:
            del self._data[key]
            return True
        return False

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def flush(self) -> None:
        self._data.clear()

    def health_check(self) -> bool:
        return True
