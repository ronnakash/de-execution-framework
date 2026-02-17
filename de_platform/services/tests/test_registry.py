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


def test_resolve_redis_cache():
    pytest.importorskip("redis")
    cls = resolve_implementation("cache", "redis")
    from de_platform.services.cache.redis_cache import RedisCache

    assert cls is RedisCache


def test_resolve_kafka_queue():
    pytest.importorskip("confluent_kafka")
    cls = resolve_implementation("mq", "kafka")
    from de_platform.services.message_queue.kafka_queue import KafkaQueue

    assert cls is KafkaQueue


def test_resolve_postgres_database():
    cls = resolve_implementation("db", "postgres")
    from de_platform.services.database.postgres_database import PostgresDatabase

    assert cls is PostgresDatabase


def test_resolve_local_filesystem():
    cls = resolve_implementation("fs", "local")
    from de_platform.services.filesystem.local_filesystem import LocalFileSystem

    assert cls is LocalFileSystem


def test_resolve_memory_metrics():
    cls = resolve_implementation("metrics", "memory")
    from de_platform.services.metrics.memory_metrics import MemoryMetrics

    assert cls is MemoryMetrics


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
