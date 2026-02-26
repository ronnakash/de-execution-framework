"""Tests for ClientConfigCache."""

from __future__ import annotations

from de_platform.pipeline.client_config_cache import CHANNEL, ClientConfigCache
from de_platform.services.cache.memory_cache import MemoryCache


def _make_cache_with_config(
    tenant_id: str = "t1",
    mode: str = "realtime",
    algos: dict | None = None,
) -> MemoryCache:
    cache = MemoryCache()
    cache.set(f"client_config:{tenant_id}", {"mode": mode, "algo_run_hour": 14})
    for algo_name, config in (algos or {}).items():
        cache.set(f"algo_config:{tenant_id}:{algo_name}", config)
    return cache


class TestGetClientMode:
    def test_returns_configured_mode(self) -> None:
        cache = _make_cache_with_config(mode="batch")
        cc = ClientConfigCache(cache)
        assert cc.get_client_mode("t1") == "batch"

    def test_defaults_to_realtime_when_no_config(self) -> None:
        cc = ClientConfigCache(MemoryCache())
        assert cc.get_client_mode("unknown") == "realtime"

    def test_defaults_to_realtime_when_mode_missing(self) -> None:
        cache = MemoryCache()
        cache.set("client_config:t1", {"algo_run_hour": 10})
        cc = ClientConfigCache(cache)
        assert cc.get_client_mode("t1") == "realtime"


class TestIsAlgoEnabled:
    def test_returns_true_by_default(self) -> None:
        cc = ClientConfigCache(MemoryCache())
        assert cc.is_algo_enabled("t1", "large_notional") is True

    def test_returns_configured_value(self) -> None:
        cache = _make_cache_with_config(algos={
            "large_notional": {"enabled": False, "thresholds": {}},
        })
        cc = ClientConfigCache(cache)
        assert cc.is_algo_enabled("t1", "large_notional") is False

    def test_enabled_algo_returns_true(self) -> None:
        cache = _make_cache_with_config(algos={
            "velocity": {"enabled": True, "thresholds": {}},
        })
        cc = ClientConfigCache(cache)
        assert cc.is_algo_enabled("t1", "velocity") is True


class TestGetAlgoThresholds:
    def test_returns_empty_dict_by_default(self) -> None:
        cc = ClientConfigCache(MemoryCache())
        assert cc.get_algo_thresholds("t1", "large_notional") == {}

    def test_returns_configured_thresholds(self) -> None:
        cache = _make_cache_with_config(algos={
            "large_notional": {"enabled": True, "thresholds": {"threshold_usd": 500_000}},
        })
        cc = ClientConfigCache(cache)
        assert cc.get_algo_thresholds("t1", "large_notional") == {"threshold_usd": 500_000}


class TestPubSubInvalidation:
    def test_invalidates_local_cache_on_update(self) -> None:
        cache = _make_cache_with_config(mode="batch")
        cc = ClientConfigCache(cache)
        cc.start()

        # Populate local cache
        assert cc.get_client_mode("t1") == "batch"
        assert "client_config:t1" in cc._local

        # Simulate config change: update Redis + publish notification
        cache.set("client_config:t1", {"mode": "realtime"})
        cache.publish_channel(CHANNEL, "t1")

        # Local cache should be invalidated, next read gets new value
        assert "client_config:t1" not in cc._local
        assert cc.get_client_mode("t1") == "realtime"

        cc.stop()

    def test_only_invalidates_matching_tenant(self) -> None:
        cache = MemoryCache()
        cache.set("client_config:t1", {"mode": "batch"})
        cache.set("client_config:t2", {"mode": "realtime"})
        cc = ClientConfigCache(cache)
        cc.start()

        # Populate both in local cache
        cc.get_client_mode("t1")
        cc.get_client_mode("t2")
        assert "client_config:t1" in cc._local
        assert "client_config:t2" in cc._local

        # Invalidate only t1
        cache.publish_channel(CHANNEL, "t1")
        assert "client_config:t1" not in cc._local
        assert "client_config:t2" in cc._local

        cc.stop()


class TestLocalCaching:
    def test_caches_after_first_read(self) -> None:
        cache = _make_cache_with_config(mode="batch")
        cc = ClientConfigCache(cache)

        cc.get_client_mode("t1")
        assert "client_config:t1" in cc._local

        # Even if Redis changes, local cache returns stale value
        cache.set("client_config:t1", {"mode": "realtime"})
        assert cc.get_client_mode("t1") == "batch"
