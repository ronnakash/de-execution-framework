from de_platform.cli.runner import run_module
from de_platform.services.logger.memory_logger import MemoryLogger


def test_run_default_name():
    exit_code, module = run_module(["run", "hello_world", "--log", "memory"])
    assert exit_code == 0
    assert isinstance(module.log, MemoryLogger)
    assert "Hello, World!" in module.log.messages


def test_run_custom_name():
    exit_code, module = run_module(["run", "hello_world", "--name", "Alice", "--log", "memory"])
    assert exit_code == 0
    assert isinstance(module.log, MemoryLogger)
    assert "Hello, Alice!" in module.log.messages


def test_run_returns_zero():
    exit_code, _ = run_module(["run", "hello_world", "--name", "Test", "--log", "memory"])
    assert exit_code == 0


def test_env_override_sets_logger():
    exit_code, module = run_module(
        ["run", "hello_world", "--env", '{"LOG_IMPL": "memory"}']
    )
    assert exit_code == 0
    assert isinstance(module.log, MemoryLogger)
    assert "Hello, World!" in module.log.messages
