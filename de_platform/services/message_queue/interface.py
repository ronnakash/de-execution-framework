from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable


class MessageQueueInterface(ABC):
    """Publish/subscribe and work queue patterns for inter-module communication."""

    @abstractmethod
    def publish(self, topic: str, message: Any, key: str | None = None) -> None: ...

    @abstractmethod
    def subscribe(self, topic: str, handler: Callable[[Any], None]) -> None: ...

    @abstractmethod
    def consume_one(self, topic: str) -> Any | None:
        """Non-blocking: return next message or None."""
        ...

    @abstractmethod
    def health_check(self) -> bool: ...
