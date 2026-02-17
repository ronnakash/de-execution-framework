"""Tests for LifecycleManager."""

from __future__ import annotations

import pytest

from de_platform.services.health.health_server import HealthCheckServer
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager


@pytest.mark.asyncio
async def test_hooks_execute_in_reverse_order() -> None:
    lm = LifecycleManager()
    order: list[str] = []
    lm.on_shutdown(lambda: order.append("A"))
    lm.on_shutdown(lambda: order.append("B"))
    lm.on_shutdown(lambda: order.append("C"))
    await lm.shutdown()
    assert order == ["C", "B", "A"]


@pytest.mark.asyncio
async def test_hook_failure_does_not_block_others() -> None:
    lm = LifecycleManager()
    calls: list[str] = []
    lm.on_shutdown(lambda: calls.append("first"))

    def failing_hook() -> None:
        raise RuntimeError("boom")

    lm.on_shutdown(failing_hook)
    lm.on_shutdown(lambda: calls.append("last"))
    await lm.shutdown()
    # Both non-failing hooks should have run
    assert calls == ["last", "first"]


@pytest.mark.asyncio
async def test_async_hooks_awaited() -> None:
    lm = LifecycleManager()
    result: list[str] = []

    async def async_hook() -> None:
        result.append("async_done")

    lm.on_shutdown(async_hook)
    await lm.shutdown()
    assert result == ["async_done"]


@pytest.mark.asyncio
async def test_is_shutting_down_flag() -> None:
    lm = LifecycleManager()
    assert not lm.is_shutting_down
    await lm.shutdown()
    assert lm.is_shutting_down


@pytest.mark.asyncio
async def test_health_server_marked_not_ready() -> None:
    lm = LifecycleManager()
    hs = HealthCheckServer(port=0)
    lm.set_health_server(hs)
    assert hs._ready is True
    await lm.shutdown()
    assert hs._ready is False


@pytest.mark.asyncio
async def test_double_shutdown_is_safe() -> None:
    lm = LifecycleManager()
    count = 0

    def hook() -> None:
        nonlocal count
        count += 1

    lm.on_shutdown(hook)
    await lm.shutdown()
    await lm.shutdown()
    assert count == 1
