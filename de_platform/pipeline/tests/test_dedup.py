"""Tests for EventDeduplicator."""

from __future__ import annotations

from de_platform.pipeline.dedup import EventDeduplicator
from de_platform.services.cache.memory_cache import MemoryCache


def _make_dedup() -> EventDeduplicator:
    return EventDeduplicator(MemoryCache())


def test_new_event_returns_new() -> None:
    d = _make_dedup()
    assert d.check("t1_order_o1_2026-01-01", "msg001") == "new"


def test_same_message_id_returns_internal_duplicate() -> None:
    d = _make_dedup()
    d.check("pk1", "msg1")
    assert d.check("pk1", "msg1") == "internal_duplicate"


def test_different_message_id_same_key_returns_external_duplicate() -> None:
    d = _make_dedup()
    d.check("pk1", "msg1")
    assert d.check("pk1", "msg2") == "external_duplicate"


def test_max_10_message_ids_retained() -> None:
    cache = MemoryCache()
    d = EventDeduplicator(cache)
    pk = "pk_many"

    # Fill with 10 unique IDs — all should be "new" or "external_duplicate"
    d.check(pk, "msg000")  # new
    for i in range(1, 10):
        d.check(pk, f"msg{i:03d}")  # external_duplicate each time

    # 11th new message_id: list must stay capped at 10
    d.check(pk, "msg_new")
    stored: list[str] = cache.get(f"dedup:{pk}")
    assert len(stored) == 10
    assert "msg_new" in stored
    # Oldest entries should have been dropped
    assert "msg000" not in stored


def test_independent_primary_keys_do_not_interfere() -> None:
    d = _make_dedup()
    assert d.check("pk_a", "msg1") == "new"
    assert d.check("pk_b", "msg1") == "new"  # same message_id, different key → new
    assert d.check("pk_a", "msg1") == "internal_duplicate"
