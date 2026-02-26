"""Tests for ClientConfigCache topic configuration (Phase 4).

Verifies get_topic_config() and get_all_client_ids() methods.
"""

from __future__ import annotations

from de_platform.pipeline.client_config_cache import ClientConfigCache
from de_platform.services.cache.memory_cache import MemoryCache


class TestTopicConfig:
    def test_default_topic_config(self) -> None:
        cache = MemoryCache()
        cc = ClientConfigCache(cache)
        config = cc.get_topic_config("unknown_tenant")
        assert config["inbound_topic_prefix"] == ""
        assert config["error_topic"] == ""

    def test_custom_topic_config(self) -> None:
        cache = MemoryCache()
        cache.set("client_config:acme", {
            "mode": "realtime",
            "inbound_topic_prefix": "acme_",
            "error_topic": "acme_errors",
        })
        cc = ClientConfigCache(cache)
        config = cc.get_topic_config("acme")
        assert config["inbound_topic_prefix"] == "acme_"
        assert config["error_topic"] == "acme_errors"

    def test_get_all_client_ids_empty(self) -> None:
        cache = MemoryCache()
        cc = ClientConfigCache(cache)
        assert cc.get_all_client_ids() == []

    def test_get_all_client_ids_after_lookups(self) -> None:
        cache = MemoryCache()
        cache.set("client_config:acme", {"mode": "realtime"})
        cache.set("client_config:globex", {"mode": "batch"})
        cc = ClientConfigCache(cache)

        # Trigger lookups so they enter local cache
        cc.get_client_mode("acme")
        cc.get_client_mode("globex")

        ids = cc.get_all_client_ids()
        assert set(ids) == {"acme", "globex"}
