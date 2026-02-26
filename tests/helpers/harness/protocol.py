"""PipelineHarness protocol and shared helpers."""

from __future__ import annotations

import asyncio
import socket
from typing import Any, AsyncContextManager, Callable, Protocol

from tests.helpers.step_logger import StepLogger

# -- Helpers ------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def poll_until(
    predicate: Callable[[], bool],
    timeout: float = 30.0,
    interval: float = 0.1,
    on_timeout: Callable[[], str] | None = None,
) -> None:
    """Repeatedly call predicate() until it returns truthy or timeout expires."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            detail = on_timeout() if on_timeout else ""
            msg = f"poll_until: condition not met within {timeout}s"
            if detail:
                msg = f"{msg}\n\n{detail}"
            raise TimeoutError(msg)
        await asyncio.sleep(interval)


# -- Protocol -----------------------------------------------------------------


class PipelineHarness(Protocol):
    tenant_id: str
    step_logger: StepLogger

    async def ingest(
        self, method: str, event_type: str, events: list[dict]
    ) -> None: ...

    async def wait_for_rows(
        self, table: str, expected: int, timeout: float = 30.0
    ) -> list[dict]: ...

    async def wait_for_no_new_rows(
        self, table: str, known: int, timeout: float = 3.0
    ) -> None: ...

    async def fetch_alerts(self) -> list[dict]: ...

    async def wait_for_alert(
        self, predicate: Callable[[dict], bool], timeout: float = 30.0
    ) -> list[dict]: ...

    async def query_api(
        self, endpoint: str, params: dict[str, str]
    ) -> tuple[int, Any]: ...

    async def publish_to_normalizer(self, topic: str, msg: dict) -> None: ...

    async def call_service(
        self,
        service: str,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, Any]: ...

    def step(self, name: str, description: str = "") -> AsyncContextManager: ...
