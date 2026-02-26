from __future__ import annotations

from collections import deque
from typing import Any, Callable

from de_platform.services.message_queue.interface import MessageQueueInterface


class MemoryQueue(MessageQueueInterface):
    """In-memory message queue for unit testing."""

    def __init__(self) -> None:
        self._topics: dict[str, deque[Any]] = {}
        self._handlers: dict[str, list[Callable[[Any], None]]] = {}

    def publish(self, topic: str, message: Any, key: str | None = None) -> None:
        if topic not in self._topics:
            self._topics[topic] = deque()
        self._topics[topic].append(message)
        for handler in self._handlers.get(topic, []):
            handler(message)

    def subscribe(self, topic: str, handler: Callable[[Any], None]) -> None:
        if topic not in self._handlers:
            self._handlers[topic] = []
        self._handlers[topic].append(handler)

    def consume_one(self, topic: str) -> Any | None:
        q = self._topics.get(topic)
        if q:
            return q.popleft()
        return None

    def health_check(self) -> bool:
        return True
