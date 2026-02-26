from __future__ import annotations

import os

from de_platform.services.secrets.interface import SecretsInterface


class EnvSecrets(SecretsInterface):
    """Reads secrets from environment variables with optional overrides."""

    def __init__(self, overrides: dict[str, str] | None = None) -> None:
        self._env = dict(os.environ)
        if overrides:
            self._env.update(overrides)

    def get(self, key: str) -> str | None:
        return self._env.get(key)

    def get_or_default(self, key: str, default: str) -> str:
        return self._env.get(key, default)

    def require(self, key: str) -> str:
        value = self._env.get(key)
        if value is None:
            raise KeyError(f"Required secret '{key}' is not set")
        return value
