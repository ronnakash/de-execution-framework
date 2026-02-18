"""Redis-based event deduplication.

Tracks up to 10 message_ids per primary_key with a 24-hour TTL.
For each incoming event:
  - "new"                — first time this primary_key is seen
  - "internal_duplicate" — same message_id seen before (silently drop)
  - "external_duplicate" — new message_id for an existing primary_key (route to duplicates topic)
"""

from __future__ import annotations

from de_platform.services.cache.interface import CacheInterface

_DEDUP_PREFIX = "dedup:"
_DEDUP_TTL = 86400  # 24 hours
_MAX_IDS = 10


class EventDeduplicator:
    """Deduplication backed by the cache (Redis in production, MemoryCache in tests)."""

    def __init__(self, cache: CacheInterface) -> None:
        self.cache = cache

    def check(self, primary_key: str, message_id: str) -> str:
        """Check dedup status for (primary_key, message_id).

        Returns:
            "new"                — first time seeing this primary_key
            "internal_duplicate" — same message_id already processed (silently drop)
            "external_duplicate" — new message_id for existing primary_key (send to duplicates topic)
        """
        cache_key = f"{_DEDUP_PREFIX}{primary_key}"
        cached: list[str] | None = self.cache.get(cache_key)

        if cached is None:
            self.cache.set(cache_key, [message_id], ttl=_DEDUP_TTL)
            return "new"

        if message_id in cached:
            return "internal_duplicate"

        # External duplicate — add message_id and retain only the last _MAX_IDS
        updated = cached + [message_id]
        self.cache.set(cache_key, updated[-_MAX_IDS:], ttl=_DEDUP_TTL)
        return "external_duplicate"
