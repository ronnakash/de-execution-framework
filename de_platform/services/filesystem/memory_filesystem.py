from de_platform.services.filesystem.interface import FileSystemInterface


class MemoryFileSystem(FileSystemInterface):
    """In-memory file system for unit testing."""

    def __init__(self) -> None:
        self._files: dict[str, bytes] = {}

    def read(self, path: str) -> bytes:
        if path not in self._files:
            raise FileNotFoundError(f"File not found: {path}")
        return self._files[path]

    def write(self, path: str, data: bytes) -> None:
        self._files[path] = data

    def list(self, prefix: str) -> list[str]:
        return sorted(k for k in self._files if k.startswith(prefix))

    def delete(self, path: str) -> bool:
        if path in self._files:
            del self._files[path]
            return True
        return False

    def exists(self, path: str) -> bool:
        return path in self._files

    def health_check(self) -> bool:
        return True
