import sys
from datetime import datetime, timezone
from typing import Any

from de_platform.services.logger.interface import LoggingInterface

_COLORS = {
    "INFO": "\033[32m",   # green
    "WARN": "\033[33m",   # yellow
    "ERROR": "\033[31m",  # red
    "DEBUG": "\033[36m",  # cyan
}
_RESET = "\033[0m"


class PrettyLogger(LoggingInterface):
    """Colorized human-readable logger for local development."""

    def info(self, msg: str, **ctx: Any) -> None:
        self._log("INFO", msg, ctx)

    def warn(self, msg: str, **ctx: Any) -> None:
        self._log("WARN", msg, ctx)

    def error(self, msg: str, **ctx: Any) -> None:
        self._log("ERROR", msg, ctx)

    def debug(self, msg: str, **ctx: Any) -> None:
        self._log("DEBUG", msg, ctx)

    def _log(self, level: str, msg: str, ctx: dict[str, Any]) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        color = _COLORS.get(level, "")
        extra = f"  {ctx}" if ctx else ""
        print(f"{color}{ts} [{level}]{_RESET} {msg}{extra}", file=sys.stderr)
