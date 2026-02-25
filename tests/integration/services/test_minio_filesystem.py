"""Integration tests for MinioFileSystem service."""

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("minio")

from de_platform.services.filesystem.minio_filesystem import MinioFileSystem  # noqa: E402
from de_platform.services.secrets.env_secrets import EnvSecrets  # noqa: E402


def _make_secrets() -> EnvSecrets:
    in_dc = os.environ.get("DEVCONTAINER", "") == "1"
    minio_host = "minio" if in_dc else "localhost"
    return EnvSecrets(overrides={
        "FS_MINIO_ENDPOINT": os.environ.get("FS_MINIO_ENDPOINT", f"{minio_host}:9000"),
        "FS_MINIO_ACCESS_KEY": os.environ.get("FS_MINIO_ACCESS_KEY", "minioadmin"),
        "FS_MINIO_SECRET_KEY": os.environ.get("FS_MINIO_SECRET_KEY", "minioadmin"),
        "FS_MINIO_BUCKET": os.environ.get("FS_MINIO_BUCKET", "de-platform-test"),
        "FS_MINIO_SECURE": "false",
    })


@pytest.fixture
def fs():
    secrets = _make_secrets()
    filesystem = MinioFileSystem(secrets=secrets)
    # Use unique prefix to avoid collisions
    filesystem._test_prefix = f"_test/{uuid.uuid4().hex[:8]}"
    yield filesystem
    # Cleanup: remove test files
    try:
        for obj in filesystem.list(filesystem._test_prefix):
            filesystem.delete(obj)
    except Exception:
        pass


def _path(fs, name: str) -> str:
    return f"{fs._test_prefix}/{name}"


def test_write_and_read(fs):
    path = _path(fs, "hello.txt")
    fs.write(path, b"Hello, MinIO!")
    data = fs.read(path)
    assert data == b"Hello, MinIO!"


def test_read_missing(fs):
    path = _path(fs, "nonexistent.txt")
    with pytest.raises(Exception):
        fs.read(path)


def test_write_overwrite(fs):
    path = _path(fs, "overwrite.txt")
    fs.write(path, b"version 1")
    fs.write(path, b"version 2")
    data = fs.read(path)
    assert data == b"version 2"


def test_list_files(fs):
    for i in range(3):
        fs.write(_path(fs, f"file_{i}.txt"), f"content {i}".encode())
    files = fs.list(fs._test_prefix)
    assert len(files) >= 3


def test_delete_file(fs):
    path = _path(fs, "to_delete.txt")
    fs.write(path, b"delete me")
    fs.delete(path)
    with pytest.raises(Exception):
        fs.read(path)


def test_write_json(fs):
    import json
    path = _path(fs, "data.json")
    data = {"key": "value", "count": 42}
    fs.write(path, json.dumps(data).encode())
    result = json.loads(fs.read(path).decode())
    assert result["key"] == "value"
    assert result["count"] == 42
