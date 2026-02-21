"""Redis-backed cache implementation using redis-py."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import redis

from de_platform.services.cache.interface import CacheInterface
from de_platform.services.secrets.interface import SecretsInterface


class RedisCache(CacheInterface):
    def __init__(self, secrets: SecretsInterface) -> None:
        self._url = secrets.get_or_default("CACHE_REDIS_URL", "redis://localhost:6379/0")
        self._client: redis.Redis | None = None  # type: ignore[type-arg]

    def connect(self) -> None:
        import redis

        self._client = redis.Redis.from_url(self._url, decode_responses=False)

    def disconnect(self) -> None:
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
