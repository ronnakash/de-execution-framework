from dataclasses import dataclass
from typing import Any

from de_platform.interfaces.logging import LoggingInterface


class ModuleConfig:
    """Wrapper around parsed module arguments with typed access."""

    def __init__(self, args: dict[str, Any]) -> None:
        self._args = args

    def get(self, key: str, default: Any = None) -> Any:
        return self._args.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._args

    def __repr__(self) -> str:
        return f"ModuleConfig({self._args})"


@dataclass
class PlatformContext:
    """Container for all platform interfaces available to a module."""

    log: LoggingInterface
    config: ModuleConfig
