"""DatabaseFactory: registry of named database instances with lazy instantiation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.secrets.interface import SecretsInterface


@dataclass
class DbConfig:
    """Deferred construction config for a named database instance."""

    impl_cls: type[Any]
    secrets: SecretsInterface | None = None
    prefix: str = "DB_POSTGRES"

    def create(self) -> DatabaseInterface:
        if self.secrets is not None:
            return self.impl_cls(secrets=self.secrets, prefix=self.prefix)
        return self.impl_cls()


class DatabaseFactory:
    """Registry of named database instances with lazy instantiation."""

    def __init__(self, configs: dict[str, DbConfig]) -> None:
        self._configs = configs
        self._instances: dict[str, DatabaseInterface] = {}

    def register_instance(self, name: str, instance: DatabaseInterface) -> None:
        """Register a pre-built DB instance (useful for testing)."""
        self._instances[name] = instance

    def get(self, name: str = "default") -> DatabaseInterface:
        if name in self._instances:
            return self._instances[name]
        config = self._configs.get(name)
        if config is None:
            available = ", ".join(self._configs.keys())
            raise KeyError(f"No database configured with name '{name}' (available: {available})")
        instance = config.create()
        self._instances[name] = instance
        return instance

    @property
    def names(self) -> list[str]:
        return list(self._configs.keys())

    def connect_all(self) -> None:
        for name in self._configs:
            db = self.get(name)
            db.connect()

    def disconnect_all(self) -> None:
        for db in self._instances.values():
            db.disconnect()
        self._instances.clear()
