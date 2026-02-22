"""Redis-backed cache implementation using redis-py."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    import redis

from de_platform.services.cache.interface import CacheInterface
from de_platform.services.secrets.interface import SecretsInterface


class RedisCache(CacheInterface):
    def __init__(self, secrets: SecretsInterface) -> None:
        self._url = secrets.get_or_default("CACHE_REDIS_URL", "redis://localhost:6379/0")
        self._client: redis.Redis | None = None  # type: ignore[type-arg]
        self._pubsub: Any = None
        self._pubsub_thread: threading.Thread | None = None
        self._callbacks: dict[str, Callable[[str], None]] = {}

    def connect(self) -> None:
        import redis

        self._client = redis.Redis.from_url(self._url, decode_responses=False)

    def disconnect(self) -> None:
        self._stop_pubsub()
        if self._client:
            self._client.close()
            self._client = None

    def _ensure_connected(self) -> redis.Redis:  # type: ignore[type-arg]
        if self._client is None:
            self.connect()
        return self._client  # type: ignore[return-value]

    def get(self, key: str) -> Any | None:
        raw = self._ensure_connected().get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        raw = json.dumps(value).encode("utf-8")
        client = self._ensure_connected()
        if ttl is not None:
            client.setex(key, ttl, raw)
        else:
            client.set(key, raw)

    def set_nx(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Atomically set key only if it does not exist."""
        raw = json.dumps(value).encode("utf-8")
        client = self._ensure_connected()
        if ttl is not None:
            return bool(client.set(key, raw, nx=True, ex=ttl))
        return bool(client.set(key, raw, nx=True))

    def delete(self, key: str) -> bool:
        return self._ensure_connected().delete(key) > 0

    def exists(self, key: str) -> bool:
        return self._ensure_connected().exists(key) > 0

    def flush(self) -> None:
        self._ensure_connected().flushdb()

    def health_check(self) -> bool:
        try:
            return self._ensure_connected().ping()
        except Exception:
            return False

    def publish_channel(self, channel: str, message: str) -> None:
        self._ensure_connected().publish(channel, message.encode("utf-8"))

    def subscribe_channel(self, channel: str, callback: Callable[[str], None]) -> None:
        client = self._ensure_connected()
        self._callbacks[channel] = callback

        if self._pubsub is None:
            self._pubsub = client.pubsub()

        def _handler(msg: dict) -> None:
            if msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                cb = self._callbacks.get(msg["channel"].decode("utf-8") if isinstance(msg["channel"], bytes) else msg["channel"])
                if cb:
                    cb(data)

        self._pubsub.subscribe(**{channel: _handler})
        if self._pubsub_thread is None or not self._pubsub_thread.is_alive():
            self._pubsub_thread = self._pubsub.run_in_thread(sleep_time=0.1, daemon=True)

    def unsubscribe_channel(self, channel: str) -> None:
        self._callbacks.pop(channel, None)
        if self._pubsub is not None:
            self._pubsub.unsubscribe(channel)
            if not self._callbacks:
                self._stop_pubsub()

    def _stop_pubsub(self) -> None:
        if self._pubsub_thread is not None:
            self._pubsub_thread.stop()
            self._pubsub_thread = None
        if self._pubsub is not None:
            self._pubsub.close()
            self._pubsub = None
        self._callbacks.clear()
