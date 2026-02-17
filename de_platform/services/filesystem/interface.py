from abc import ABC, abstractmethod


class FileSystemInterface(ABC):
    """Abstracts file storage for data lake and warehouse patterns."""

    @abstractmethod
    def read(self, path: str) -> bytes:
        """Read file contents. Raises FileNotFoundError if missing."""
        ...

    @abstractmethod
    def write(self, path: str, data: bytes) -> None:
        """Write data to a file. Creates intermediate directories."""
        ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]:
        """List all files under the given prefix (recursive). Returns relative paths."""
        ...

    @abstractmethod
    def delete(self, path: str) -> bool:
        """Delete a file. Returns True if deleted, False if it didn't exist."""
        ...

    @abstractmethod
    def exists(self, path: str) -> bool: ...

    @abstractmethod
    def health_check(self) -> bool: ...
