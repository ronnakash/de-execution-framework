from __future__ import annotations

from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.logger.memory_logger import MemoryLogger
from de_platform.services.logger.pretty_logger import PrettyLogger


class LoggerFactory:
    """Factory that creates and caches logger instances by implementation name."""

    _registry: dict[str, type[LoggingInterface]] = {
        "pretty": PrettyLogger,
        "memory": MemoryLogger,
    }

    def __init__(self, default_impl: str = "pretty") -> None:
        if default_impl not in self._registry:
            raise ValueError(
                f"Unknown logger implementation: '{default_impl}' "
                f"(available: {', '.join(self._registry)})"
            )
        self._default_impl = default_impl
        self._instances: dict[str, LoggingInterface] = {}

    def create(self, impl_name: str | None = None) -> LoggingInterface:
        """Return a logger instance, creating one if not yet cached."""
        name = impl_name or self._default_impl
        if name not in self._instances:
            cls = self._registry.get(name)
            if cls is None:
                raise ValueError(
                    f"Unknown logger implementation: '{name}' "
                    f"(available: {', '.join(self._registry)})"
                )
            self._instances[name] = cls()
        return self._instances[name]
