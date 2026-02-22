"""Loki logger — sends structured JSON logs to Grafana Loki's HTTP push API.

Batches log entries and flushes periodically to avoid HTTP overhead.
Requires the ``requests`` library (optional infra dependency).
"""

from __future__ import annotations

import atexit
import json
import os
import threading
import time
from typing import Any

from de_platform.services.logger.interface import LoggingInterface

_DEFAULT_LOKI_URL = "http://localhost:3100"
_FLUSH_INTERVAL = 1.0  # seconds
_FLUSH_THRESHOLD = 100  # entries


class LokiLogger(LoggingInterface):
    """Structured logger that pushes to Grafana Loki via HTTP."""

    def __init__(self) -> None:
        self._base_url = os.environ.get("LOKI_URL", _DEFAULT_LOKI_URL)
        self._push_url = f"{self._base_url}/loki/api/v1/push"
        self._service = os.environ.get("LOKI_SERVICE", "de_platform")
        self._environment = os.environ.get("LOKI_ENVIRONMENT", "development")

        self._buffer: list[tuple[str, dict[str, Any]]] = []
        self._lock = threading.Lock()
        self._closed = False

        # Background flush thread
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()
        atexit.register(self.close)

    def info(self, msg: str, **ctx: Any) -> None:
        self._append("INFO", msg, ctx)

    def warn(self, msg: str, **ctx: Any) -> None:
        self._append("WARN", msg, ctx)

    def error(self, msg: str, **ctx: Any) -> None:
        self._append("ERROR", msg, ctx)

    def debug(self, msg: str, **ctx: Any) -> None:
        self._append("DEBUG", msg, ctx)

    def close(self) -> None:
        """Flush remaining entries and stop the background thread."""
        self._closed = True
        self._flush()

    # ── Internal ──────────────────────────────────────────────────────────

    def _append(self, level: str, msg: str, ctx: dict[str, Any]) -> None:
        line = json.dumps({"level": level, "msg": msg, **ctx})
        with self._lock:
            self._buffer.append((level, {"msg": msg, **ctx}))
            if len(self._buffer) >= _FLUSH_THRESHOLD:
                self._flush_locked()

    def _flush_loop(self) -> None:
        while not self._closed:
            time.sleep(_FLUSH_INTERVAL)
            self._flush()

    def _flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        """Flush buffer while already holding the lock."""
        if not self._buffer:
            return

        entries = self._buffer[:]
        self._buffer.clear()

        # Group by level for separate Loki streams
        streams: dict[str, list[list[str]]] = {}
        for level, ctx in entries:
            ts_ns = str(int(time.time() * 1e9))
            line = json.dumps(ctx)
            streams.setdefault(level, []).append([ts_ns, line])

        payload = {
            "streams": [
                {
                    "stream": {
                        "service": self._service,
                        "environment": self._environment,
                        "level": level,
                    },
                    "values": values,
                }
                for level, values in streams.items()
            ]
        }

        try:
            import requests

            requests.post(
                self._push_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=5,
            )
        except Exception:
            # Silently drop — logging should never crash the application
            pass
