"""Tests for HealthCheckServer."""

from __future__ import annotations

import asyncio
import json

import pytest

from de_platform.services.health.health_server import HealthCheckServer


async def _get(port: int, path: str) -> tuple[int, dict]:
    """Make a GET request and return (status_code, json_body)."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        text = data.decode("utf-8")
        header_end = text.index("\r\n\r\n")
        status_line = text.split("\r\n")[0]
        status_code = int(status_line.split()[1])
        body = json.loads(text[header_end + 4 :])
        return status_code, body
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.fixture
async def server():
    srv = HealthCheckServer(port=0)
    await srv.start()
    # Get the actual bound port
    port = srv._server.sockets[0].getsockname()[1]
    srv._bound_port = port
    yield srv
    await srv.stop()


def _port(srv: HealthCheckServer) -> int:
    return srv._bound_port  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_liveness_returns_ok(server: HealthCheckServer) -> None:
    status, body = await _get(_port(server), "/health/live")
    assert status == 200
    assert body["status"] == "ok"


@pytest.mark.asyncio
async def test_readiness_all_checks_pass(server: HealthCheckServer) -> None:
    server.register_check("db", lambda: True)
    server.register_check("cache", lambda: True)
    status, body = await _get(_port(server), "/health/ready")
    assert status == 200
    assert body["status"] == "ok"
    assert body["checks"]["db"] == "ok"
    assert body["checks"]["cache"] == "ok"


@pytest.mark.asyncio
async def test_readiness_check_fails(server: HealthCheckServer) -> None:
    server.register_check("db", lambda: True)
    server.register_check("cache", lambda: False)
    status, body = await _get(_port(server), "/health/ready")
    assert status == 503
    assert body["checks"]["cache"] == "fail"


@pytest.mark.asyncio
async def test_startup_before_mark(server: HealthCheckServer) -> None:
    status, body = await _get(_port(server), "/health/startup")
    assert status == 503


@pytest.mark.asyncio
async def test_startup_after_mark(server: HealthCheckServer) -> None:
    server.mark_started()
    status, body = await _get(_port(server), "/health/startup")
    assert status == 200
    assert body["status"] == "ok"


@pytest.mark.asyncio
async def test_mark_not_ready(server: HealthCheckServer) -> None:
    server.register_check("db", lambda: True)
    server.mark_not_ready()
    status, body = await _get(_port(server), "/health/ready")
    assert status == 503
    assert body["reason"] == "shutting down"


@pytest.mark.asyncio
async def test_unknown_path_returns_404(server: HealthCheckServer) -> None:
    status, body = await _get(_port(server), "/unknown")
    assert status == 404
