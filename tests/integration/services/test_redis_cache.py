"""Integration tests for RedisCache service."""

import json
import os
import time
import uuid

import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("redis")

from de_platform.services.cache.redis_cache import RedisCache  # noqa: E402
from de_platform.services.secrets.env_secrets import EnvSecrets  # noqa: E402


def _make_secrets() -> EnvSecrets:
    in_dc = os.environ.get("DEVCONTAINER", "") == "1"
    redis_host = "redis" if in_dc else "localhost"
    return EnvSecrets(overrides={
        "CACHE_REDIS_URL": os.environ.get(
            "CACHE_REDIS_URL", f"redis://{redis_host}:6379/0"
        ),
    })


@pytest.fixture
def cache():
    secrets = _make_secrets()
    c = RedisCache(secrets=secrets)
    c.connect()
    # Use unique prefix to avoid collisions between tests
    c._test_prefix = f"_test_{uuid.uuid4().hex[:8]}"
    yield c
    # Cleanup test keys
    pattern = f"{c._test_prefix}:*"
    keys = c._client.keys(pattern)
    if keys:
        c._client.delete(*keys)
    c.disconnect()


def _key(cache, name: str) -> str:
    return f"{cache._test_prefix}:{name}"


def test_connect_disconnect():
    secrets = _make_secrets()
    c = RedisCache(secrets=secrets)
    c.connect()
    assert c._client is not None
    assert c.health_check()
    c.disconnect()
    assert c._client is None


def test_get_set(cache):
    k = _key(cache, "hello")
    cache.set(k, "world")
    assert cache.get(k) == "world"


def test_get_missing(cache):
    k = _key(cache, "missing")
    assert cache.get(k) is None


def test_delete(cache):
    k = _key(cache, "to_delete")
    cache.set(k, "value")
    cache.delete(k)
    assert cache.get(k) is None


def test_set_dict(cache):
    k = _key(cache, "config")
    cache.set(k, {"mode": "realtime", "window": 5})
    result = cache.get(k)
    assert result["mode"] == "realtime"
    assert result["window"] == 5


def test_ttl(cache):
    k = _key(cache, "ttl_key")
    cache.set(k, "value", ttl=1)
    assert cache.get(k) == "value"
    time.sleep(1.5)
    assert cache.get(k) is None


def test_pubsub(cache):
    """Test pub-sub via subscribe_channel and publish_channel."""
    channel = _key(cache, "channel")
    messages = []

    def handler(msg):
        messages.append(msg)

    cache.subscribe_channel(channel, handler)
    # publish_channel expects a string
    cache.publish_channel(channel, json.dumps({"action": "invalidate"}))

    # Give pub-sub a moment to deliver
    deadline = time.monotonic() + 5.0
    while not messages and time.monotonic() < deadline:
        time.sleep(0.1)

    assert len(messages) >= 1
