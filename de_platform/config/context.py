import os
from typing import Any


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


class PlatformConfig:
    """Environment-based configuration with optional overrides."""

    def __init__(self, overrides: dict[str, str] | None = None) -> None:
        self._env = dict(os.environ)
        if overrides:
            self._env.update(overrides)

    def get(self, key: str, default: str = "") -> str:
        return self._env.get(key, default)
