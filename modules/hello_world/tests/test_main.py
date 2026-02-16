from de_platform.config.context import ModuleConfig, PlatformContext
from de_platform.implementations.memory_logger import MemoryLogger

from modules.hello_world.main import run


def test_run_default_name():
    logger = MemoryLogger()
    ctx = PlatformContext(log=logger, config=ModuleConfig({"name": "World"}))
    result = run(ctx)
    assert result == 0
    assert "Hello, World!" in logger.messages


def test_run_custom_name():
    logger = MemoryLogger()
    ctx = PlatformContext(log=logger, config=ModuleConfig({"name": "Alice"}))
    result = run(ctx)
    assert result == 0
    assert "Hello, Alice!" in logger.messages


def test_run_returns_zero():
    logger = MemoryLogger()
    ctx = PlatformContext(log=logger, config=ModuleConfig({"name": "Test"}))
    result = run(ctx)
    assert result == 0
