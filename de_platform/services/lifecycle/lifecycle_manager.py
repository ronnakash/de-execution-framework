"""Centralized shutdown orchestration with signal handling and ordered cleanup hooks."""

from __future__ import annotations

import asyncio
import inspect
import signal
from typing import Any, Awaitable, Callable, Union

from de_platform.services.health.health_server import HealthCheckServer

ShutdownHook = Union[Callable[[], None], Callable[[], Awaitable[None]]]


class LifecycleManager:
    def __init__(self, drain_timeout: float = 30.0) -> None:
        self._drain_timeout = drain_timeout
        self._hooks: list[ShutdownHook] = []
        self._shutting_down = False
        self._shutdown_done = False
        self._health_server: HealthCheckServer | None = None

    @property
    def is_shutting_down(self) -> bool:
        """Modules can poll this to know if they should stop work."""
        return self._shutting_down

    def on_shutdown(self, callback: ShutdownHook) -> None:
        """Register a cleanup callback. Executed in reverse order on shutdown."""
        self._hooks.append(callback)

    def set_health_server(self, server: HealthCheckServer) -> None:
        """Link the health server so shutdown can mark it not-ready."""
        self._health_server = server

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Register SIGTERM and SIGINT handlers.

        If *loop* is provided, uses loop.add_signal_handler (async-safe).
        Otherwise falls back to signal.signal (sync context).
        """
        if loop is not None:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._trigger_shutdown_from_signal)
        else:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._handle_signal)

    async def shutdown(self) -> None:
        """Execute the full shutdown sequence."""
        if self._shutdown_done:
            return
        self._shutting_down = True
        self._shutdown_done = True

        # 1. Mark health server as not-ready
        if self._health_server:
            self._health_server.mark_not_ready()

        # 2. Execute hooks in reverse order
        for hook in reversed(self._hooks):
            try:
                result = hook()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass  # One failing hook must not prevent others from running

        # 3. Stop health server
        if self._health_server:
            await self._health_server.stop()

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Sync signal handler — sets the shutting_down flag."""
        self._shutting_down = True

    def _trigger_shutdown_from_signal(self) -> None:
        """Async signal handler — schedules full shutdown on the event loop."""
        if not self._shutting_down:
            self._shutting_down = True
            loop = asyncio.get_event_loop()
            loop.create_task(self.shutdown())
