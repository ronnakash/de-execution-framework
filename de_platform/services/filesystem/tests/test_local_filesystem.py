import pytest

from de_platform.services.filesystem.local_filesystem import LocalFileSystem
from de_platform.services.secrets.env_secrets import EnvSecrets


@pytest.fixture
def fs(tmp_path):
    secrets = EnvSecrets(overrides={"FS_LOCAL_ROOT": str(tmp_path)})
    return LocalFileSystem(secrets)


def test_write_and_read(fs):
    fs.write("data/file.txt", b"hello world")
    assert fs.read("data/file.txt") == b"hello world"


def test_write_creates_directories(fs):
    fs.write("a/b/c/deep.txt", b"deep")
    assert fs.read("a/b/c/deep.txt") == b"deep"


def test_read_nonexistent(fs):
    with pytest.raises(FileNotFoundError):
        fs.read("no/such/file.txt")


def test_list_files(fs):
    fs.write("prefix/a.txt", b"a")
    fs.write("prefix/b.txt", b"b")
    fs.write("other/c.txt", b"c")
    result = fs.list("prefix")
    assert result == ["prefix/a.txt", "prefix/b.txt"]


def test_list_empty_prefix(fs):
    assert fs.list("nonexistent") == []


def test_delete_existing(fs):
    fs.write("to_delete.txt", b"data")
    assert fs.delete("to_delete.txt") is True
    assert fs.exists("to_delete.txt") is False


def test_delete_nonexistent(fs):
    assert fs.delete("nope.txt") is False


def test_exists(fs):
    assert fs.exists("nope.txt") is False
    fs.write("yep.txt", b"data")
    assert fs.exists("yep.txt") is True


def test_path_traversal_rejected(fs):
    with pytest.raises(ValueError, match="Path traversal"):
        fs.read("../etc/passwd")


def test_path_traversal_write_rejected(fs):
    with pytest.raises(ValueError, match="Path traversal"):
        fs.write("../../evil.txt", b"bad")


def test_overwrite(fs):
    fs.write("file.txt", b"v1")
    fs.write("file.txt", b"v2")
    assert fs.read("file.txt") == b"v2"


def test_health_check(fs):
    assert fs.health_check() is True


def test_health_check_creates_root(tmp_path):
    new_root = tmp_path / "new_root"
    secrets = EnvSecrets(overrides={"FS_LOCAL_ROOT": str(new_root)})
    fs = LocalFileSystem(secrets)
    assert fs.health_check() is True
    assert new_root.is_dir()
