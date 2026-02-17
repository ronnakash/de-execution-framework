"""Lightweight HTTP health check server using raw asyncio.

Serves /health/live, /health/ready, and /health/startup endpoints.
Only started for service and worker module types.
"""

from __future__ import annotations

import asyncio
import json
from typing import Callable


class HealthCheckServer:
    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._checks: dict[str, Callable[[], bool]] = {}
        self._started = False
        self._ready = True
        self._server: asyncio.Server | None = None

    def register_check(self, name: str, check: Callable[[], bool]) -> None:
        """Register a named health check (e.g. 'db', 'cache')."""
        self._checks[name] = check

    def mark_started(self) -> None:
        """Called after module.initialize() completes."""
        self._started = True

    def mark_not_ready(self) -> None:
        """Called on shutdown signal to fail readiness probes."""
        self._ready = False

    async def start(self) -> None:
        """Start the HTTP server."""
        self._server = await asyncio.start_server(
            self._handle_connection, "0.0.0.0", self._port
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not request_line:
                return
            parts = request_line.decode("utf-8", errors="replace").strip().split()
            if len(parts) < 2:
                self._send_response(writer, 400, {"error": "bad request"})
                return

            method, path = parts[0], parts[1]

            # Consume remaining headers
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if line in (b"\r\n", b"\n", b""):
                    break

            if method != "GET":
                self._send_response(writer, 405, {"error": "method not allowed"})
                return

            if path == "/health/live":
                self._send_response(writer, 200, {"status": "ok"})
            elif path == "/health/ready":
                self._handle_ready(writer)
            elif path == "/health/startup":
                self._handle_startup(writer)
            else:
                self._send_response(writer, 404, {"error": "not found"})
        except (asyncio.TimeoutError, ConnectionError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _handle_ready(self, writer: asyncio.StreamWriter) -> None:
        if not self._ready:
            self._send_response(writer, 503, {"status": "not ready", "reason": "shutting down"})
            return

        checks: dict[str, str] = {}
        all_ok = True
        for name, check_fn in self._checks.items():
            try:
                if check_fn():
                    checks[name] = "ok"
                else:
                    checks[name] = "fail"
                    all_ok = False
            except Exception as exc:
                checks[name] = f"error: {exc}"
                all_ok = False

        if all_ok:
            self._send_response(writer, 200, {"status": "ok", "checks": checks})
        else:
            self._send_response(writer, 503, {"status": "not ready", "checks": checks})

    def _handle_startup(self, writer: asyncio.StreamWriter) -> None:
        if self._started:
            self._send_response(writer, 200, {"status": "ok"})
        else:
            self._send_response(writer, 503, {"status": "not started"})

    def _send_response(
        self, writer: asyncio.StreamWriter, status: int, body: dict
    ) -> None:
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed", 503: "Service Unavailable"}.get(status, "Unknown")
        payload = json.dumps(body).encode("utf-8")
        header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        writer.write(header.encode("utf-8") + payload)
