"""Central registry mapping (interface_name, impl_name) to concrete class paths.

Uses string paths for lazy imports — importing the registry doesn't pull in
heavy libraries (asyncpg, boto3, etc.) unless that implementation is selected.
"""

import importlib
from typing import Any

REGISTRY: dict[str, dict[str, str]] = {
    "db": {
        "memory": "de_platform.services.database.memory_database.MemoryDatabase",
    },
    "fs": {
        "memory": "de_platform.services.filesystem.memory_filesystem.MemoryFileSystem",
    },
    "cache": {
        "memory": "de_platform.services.cache.memory_cache.MemoryCache",
    },
    "mq": {
        "memory": "de_platform.services.message_queue.memory_queue.MemoryQueue",
    },
    "metrics": {
        "noop": "de_platform.services.metrics.noop_metrics.NoopMetrics",
        "memory": "de_platform.services.metrics.memory_metrics.MemoryMetrics",
    },
    "secrets": {
        "env": "de_platform.services.secrets.env_secrets.EnvSecrets",
    },
}

# Maps flag name -> interface ABC for DI container registration
INTERFACE_TYPES: dict[str, str] = {
    "db": "de_platform.services.database.interface.DatabaseInterface",
    "fs": "de_platform.services.filesystem.interface.FileSystemInterface",
    "cache": "de_platform.services.cache.interface.CacheInterface",
    "mq": "de_platform.services.message_queue.interface.MessageQueueInterface",
    "metrics": "de_platform.services.metrics.interface.MetricsInterface",
    "secrets": "de_platform.services.secrets.interface.SecretsInterface",
}


def resolve_class(dotted_path: str) -> type[Any]:
    """Import and return a class from a dotted module.ClassName path."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def resolve_implementation(flag_name: str, impl_name: str) -> type[Any]:
    """Look up the concrete class for a given flag and implementation name."""
    impls = REGISTRY.get(flag_name)
    if impls is None:
        raise ValueError(f"Unknown interface flag: --{flag_name}")
    dotted = impls.get(impl_name)
    if dotted is None:
        available = ", ".join(impls.keys())
        raise ValueError(
            f"Unknown implementation '{impl_name}' for --{flag_name} "
            f"(available: {available})"
        )
    return resolve_class(dotted)


def resolve_interface_type(flag_name: str) -> type[Any]:
    """Return the ABC type for a given flag name, for DI container registration."""
    dotted = INTERFACE_TYPES.get(flag_name)
    if dotted is None:
        raise ValueError(f"Unknown interface flag: --{flag_name}")
    return resolve_class(dotted)
