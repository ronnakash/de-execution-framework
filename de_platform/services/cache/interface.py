from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable


class CacheInterface(ABC):
    """Key-value caching with TTL support and optional pub-sub channels."""

    @abstractmethod
    def get(self, key: str) -> Any | None: ...

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int | None = None) -> None: ...

    def set_nx(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set key only if it does not exist. Returns True if set, False if key existed."""
        if self.get(key) is not None:
            return False
        self.set(key, value, ttl=ttl)
        return True

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def flush(self) -> None: ...

    @abstractmethod
    def health_check(self) -> bool: ...

    def publish_channel(self, channel: str, message: str) -> None:
        """Publish a message to a pub-sub channel."""

    def subscribe_channel(self, channel: str, callback: Callable[[str], None]) -> None:
        """Subscribe to a pub-sub channel with a callback."""

    def unsubscribe_channel(self, channel: str) -> None:
        """Unsubscribe from a pub-sub channel."""
