"""Client configuration cache with pub-sub invalidation.

Reads per-tenant config from Redis cache keys written by the client_config
service.  Subscribes to a pub-sub channel for real-time invalidation when
config changes.

Cache keys (written by client_config service):
    client_config:{tenant_id}            → {"mode": "realtime", ...}
    algo_config:{tenant_id}:{algorithm}  → {"enabled": true, "thresholds": {...}}
"""

from __future__ import annotations

from typing import Any

from de_platform.services.cache.interface import CacheInterface

CHANNEL = "client_config_updates"


class ClientConfigCache:
    """Cache layer for per-tenant client configuration.

    Maintains a local in-memory cache backed by Redis lookups.  Subscribes
    to the ``client_config_updates`` pub-sub channel so the client_config
    service can push invalidation notifications in real time.
    """

    def __init__(self, cache: CacheInterface) -> None:
        self._cache = cache
        self._local: dict[str, Any] = {}

    def start(self) -> None:
        """Subscribe to config update notifications."""
        self._cache.subscribe_channel(CHANNEL, self._on_update)

    def stop(self) -> None:
        """Unsubscribe from notifications."""
        self._cache.unsubscribe_channel(CHANNEL)

    def _on_update(self, message: str) -> None:
        """Invalidate local cache entries for the updated tenant."""
        keys_to_remove = [k for k in self._local if message in k]
        for k in keys_to_remove:
            del self._local[k]

    # ── Public API ────────────────────────────────────────────────────────

    def get_client_mode(self, tenant_id: str) -> str:
        """Return ``'realtime'`` or ``'batch'``.  Defaults to ``'realtime'``."""
        config = self._get_client(tenant_id)
        return config.get("mode", "realtime") if config else "realtime"

    def is_algo_enabled(self, tenant_id: str, algorithm: str) -> bool:
        """Check if *algorithm* is enabled for *tenant_id*.  Default: ``True``."""
        config = self._get_algo_config(tenant_id, algorithm)
        return config.get("enabled", True) if config else True

    def get_algo_thresholds(self, tenant_id: str, algorithm: str) -> dict:
        """Get algo-specific thresholds.  Default: ``{}`` (use algo defaults)."""
        config = self._get_algo_config(tenant_id, algorithm)
        return config.get("thresholds", {}) if config else {}

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get_client(self, tenant_id: str) -> dict | None:
        key = f"client_config:{tenant_id}"
        if key in self._local:
            return self._local[key]
        val = self._cache.get(key)
        if val is not None:
            self._local[key] = val
        return val

    def _get_algo_config(self, tenant_id: str, algorithm: str) -> dict | None:
        key = f"algo_config:{tenant_id}:{algorithm}"
        if key in self._local:
            return self._local[key]
        val = self._cache.get(key)
        if val is not None:
            self._local[key] = val
        return val
