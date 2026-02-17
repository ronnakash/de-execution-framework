import pytest

from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem


def test_write_and_read():
    fs = MemoryFileSystem()
    fs.write("data/file.txt", b"hello world")
    assert fs.read("data/file.txt") == b"hello world"


def test_read_nonexistent_raises():
    fs = MemoryFileSystem()
    with pytest.raises(FileNotFoundError):
        fs.read("nonexistent.txt")


def test_list_with_prefix():
    fs = MemoryFileSystem()
    fs.write("raw/2026/a.txt", b"a")
    fs.write("raw/2026/b.txt", b"b")
    fs.write("cleaned/c.txt", b"c")
    result = fs.list("raw/")
    assert result == ["raw/2026/a.txt", "raw/2026/b.txt"]


def test_list_empty_prefix():
    fs = MemoryFileSystem()
    assert fs.list("nonexistent/") == []


def test_delete_existing():
    fs = MemoryFileSystem()
    fs.write("file.txt", b"data")
    assert fs.delete("file.txt") is True
    assert not fs.exists("file.txt")


def test_delete_nonexistent():
    fs = MemoryFileSystem()
    assert fs.delete("nonexistent.txt") is False


def test_exists():
    fs = MemoryFileSystem()
    assert not fs.exists("file.txt")
    fs.write("file.txt", b"data")
    assert fs.exists("file.txt")


def test_overwrite():
    fs = MemoryFileSystem()
    fs.write("file.txt", b"old")
    fs.write("file.txt", b"new")
    assert fs.read("file.txt") == b"new"


def test_health_check():
    fs = MemoryFileSystem()
    assert fs.health_check() is True
