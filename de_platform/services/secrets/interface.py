from abc import ABC, abstractmethod


class SecretsInterface(ABC):
    """Provides access to secrets and configuration values."""

    @abstractmethod
    def get(self, key: str) -> str | None:
        """Get a secret value by key. Returns None if not found."""
        ...

    @abstractmethod
    def get_or_default(self, key: str, default: str) -> str:
        """Get a secret value, returning default if not found."""
        ...

    @abstractmethod
    def require(self, key: str) -> str:
        """Get a secret value, raising KeyError if not found."""
        ...
