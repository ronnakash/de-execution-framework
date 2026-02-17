import time
from unittest.mock import patch

from de_platform.services.cache.memory_cache import MemoryCache


def test_get_set_roundtrip():
    cache = MemoryCache()
    cache.set("key", "value")
    assert cache.get("key") == "value"


def test_get_returns_none_for_missing():
    cache = MemoryCache()
    assert cache.get("nonexistent") is None


def test_ttl_expiry():
    cache = MemoryCache()
    start = time.monotonic()
    cache.set("key", "value", ttl=1)
    # Simulate time passing beyond TTL
    with patch("de_platform.services.cache.memory_cache.time") as mock_time:
        mock_time.monotonic.return_value = start + 2
        assert cache.get("key") is None


def test_ttl_not_expired():
    cache = MemoryCache()
    cache.set("key", "value", ttl=60)
    assert cache.get("key") == "value"


def test_delete_existing():
    cache = MemoryCache()
    cache.set("key", "value")
    assert cache.delete("key") is True
    assert cache.get("key") is None


def test_delete_nonexistent():
    cache = MemoryCache()
    assert cache.delete("nonexistent") is False


def test_exists():
    cache = MemoryCache()
    assert not cache.exists("key")
    cache.set("key", "value")
    assert cache.exists("key")


def test_flush():
    cache = MemoryCache()
    cache.set("a", 1)
    cache.set("b", 2)
    cache.flush()
    assert cache.get("a") is None
    assert cache.get("b") is None


def test_health_check():
    cache = MemoryCache()
    assert cache.health_check() is True
