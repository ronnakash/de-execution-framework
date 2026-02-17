import pytest

from de_platform.services.secrets.env_secrets import EnvSecrets


def test_get_returns_value():
    secrets = EnvSecrets(overrides={"MY_KEY": "my_value"})
    assert secrets.get("MY_KEY") == "my_value"


def test_get_returns_none_for_missing():
    secrets = EnvSecrets(overrides={})
    assert secrets.get("NONEXISTENT_KEY_12345") is None


def test_get_or_default_returns_value():
    secrets = EnvSecrets(overrides={"MY_KEY": "my_value"})
    assert secrets.get_or_default("MY_KEY", "fallback") == "my_value"


def test_get_or_default_returns_default_for_missing():
    secrets = EnvSecrets(overrides={})
    assert secrets.get_or_default("NONEXISTENT_KEY_12345", "fallback") == "fallback"


def test_require_returns_value():
    secrets = EnvSecrets(overrides={"MY_KEY": "my_value"})
    assert secrets.require("MY_KEY") == "my_value"


def test_require_raises_for_missing():
    secrets = EnvSecrets(overrides={})
    with pytest.raises(KeyError, match="Required secret 'NONEXISTENT_KEY_12345' is not set"):
        secrets.require("NONEXISTENT_KEY_12345")


def test_overrides_take_precedence(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MY_KEY", "from_env")
    secrets = EnvSecrets(overrides={"MY_KEY": "from_override"})
    assert secrets.get("MY_KEY") == "from_override"
