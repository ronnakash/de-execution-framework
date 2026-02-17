import pytest

from de_platform.services.registry import resolve_implementation, resolve_interface_type


def test_resolve_memory_database():
    cls = resolve_implementation("db", "memory")
    from de_platform.services.database.memory_database import MemoryDatabase

    assert cls is MemoryDatabase


def test_resolve_memory_filesystem():
    cls = resolve_implementation("fs", "memory")
    from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem

    assert cls is MemoryFileSystem


def test_resolve_memory_cache():
    cls = resolve_implementation("cache", "memory")
    from de_platform.services.cache.memory_cache import MemoryCache

    assert cls is MemoryCache


def test_resolve_memory_queue():
    cls = resolve_implementation("mq", "memory")
    from de_platform.services.message_queue.memory_queue import MemoryQueue

    assert cls is MemoryQueue


def test_resolve_noop_metrics():
    cls = resolve_implementation("metrics", "noop")
    from de_platform.services.metrics.noop_metrics import NoopMetrics

    assert cls is NoopMetrics


def test_resolve_env_secrets():
    cls = resolve_implementation("secrets", "env")
    from de_platform.services.secrets.env_secrets import EnvSecrets

    assert cls is EnvSecrets


def test_unknown_flag_raises():
    with pytest.raises(ValueError, match="Unknown interface flag"):
        resolve_implementation("nonexistent", "memory")


def test_unknown_impl_raises():
    with pytest.raises(ValueError, match="Unknown implementation"):
        resolve_implementation("db", "oracle")


def test_resolve_interface_type():
    iface = resolve_interface_type("db")
    from de_platform.services.database.interface import DatabaseInterface

    assert iface is DatabaseInterface
