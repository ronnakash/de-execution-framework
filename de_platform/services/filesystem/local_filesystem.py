import os
import tempfile
from pathlib import Path

from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.secrets.interface import SecretsInterface


class LocalFileSystem(FileSystemInterface):
    """File system implementation backed by local disk."""

    def __init__(self, secrets: SecretsInterface) -> None:
        root = secrets.get_or_default("FS_LOCAL_ROOT", "/tmp/data-platform")
        self._root = Path(root)

    def _resolve(self, path: str) -> Path:
        """Resolve a relative path against root, rejecting traversal attempts."""
        if ".." in path.split("/"):
            raise ValueError(f"Path traversal not allowed: {path}")
        return self._root / path

    def read(self, path: str) -> bytes:
        full = self._resolve(path)
        if not full.exists():
            raise FileNotFoundError(f"File not found: {path}")
        return full.read_bytes()

    def write(self, path: str, data: bytes) -> None:
        full = self._resolve(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write: temp file + rename
        fd, tmp = tempfile.mkstemp(dir=full.parent)
        closed = False
        try:
            os.write(fd, data)
            os.fsync(fd)
            os.close(fd)
            closed = True
            os.replace(tmp, full)
        except BaseException:
            if not closed:
                os.close(fd)
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def list(self, prefix: str) -> list[str]:
        target = self._resolve(prefix)
        if not target.exists():
            return []
        results: list[str] = []
        for p in sorted(target.rglob("*")):
            if p.is_file():
                results.append(str(p.relative_to(self._root)))
        return results

    def delete(self, path: str) -> bool:
        full = self._resolve(path)
        if full.exists():
            full.unlink()
            return True
        return False

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()

    def health_check(self) -> bool:
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            return self._root.is_dir() and os.access(self._root, os.W_OK)
        except OSError:
            return False
