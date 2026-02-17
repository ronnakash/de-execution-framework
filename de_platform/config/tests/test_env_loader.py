from pathlib import Path

from de_platform.config.env_loader import load_env_file


def test_load_valid_env_file(tmp_path: Path):
    env_dir = tmp_path / ".env"
    env_dir.mkdir()
    (env_dir / "local.env").write_text("KEY1=value1\nKEY2=value2\n")
    result = load_env_file("local", project_root=tmp_path)
    assert result == {"KEY1": "value1", "KEY2": "value2"}


def test_load_missing_file_returns_empty(tmp_path: Path):
    result = load_env_file("nonexistent", project_root=tmp_path)
    assert result == {}


def test_comments_and_blank_lines(tmp_path: Path):
    env_dir = tmp_path / ".env"
    env_dir.mkdir()
    (env_dir / "test.env").write_text("# a comment\n\nKEY=val\n  # another\n")
    result = load_env_file("test", project_root=tmp_path)
    assert result == {"KEY": "val"}


def test_quoted_values(tmp_path: Path):
    env_dir = tmp_path / ".env"
    env_dir.mkdir()
    (env_dir / "test.env").write_text('SINGLE=\'hello\'\nDOUBLE="world"\n')
    result = load_env_file("test", project_root=tmp_path)
    assert result == {"SINGLE": "hello", "DOUBLE": "world"}


def test_value_with_equals_sign(tmp_path: Path):
    env_dir = tmp_path / ".env"
    env_dir.mkdir()
    (env_dir / "test.env").write_text("URL=postgres://user:pass@host/db?opt=1\n")
    result = load_env_file("test", project_root=tmp_path)
    assert result == {"URL": "postgres://user:pass@host/db?opt=1"}
