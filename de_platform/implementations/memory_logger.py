from dataclasses import dataclass
from typing import Any

from de_platform.interfaces.logging import LoggingInterface


@dataclass
class LogEntry:
    level: str
    msg: str
    ctx: dict[str, Any]


class MemoryLogger(LoggingInterface):
    """In-memory logger that stores entries for test assertions."""

    def __init__(self) -> None:
        self.entries: list[LogEntry] = []

    def info(self, msg: str, **ctx: Any) -> None:
        self.entries.append(LogEntry("INFO", msg, ctx))

    def warn(self, msg: str, **ctx: Any) -> None:
        self.entries.append(LogEntry("WARN", msg, ctx))

    def error(self, msg: str, **ctx: Any) -> None:
        self.entries.append(LogEntry("ERROR", msg, ctx))

    def debug(self, msg: str, **ctx: Any) -> None:
        self.entries.append(LogEntry("DEBUG", msg, ctx))

    @property
    def messages(self) -> list[str]:
        """Convenience: return just the message strings."""
        return [e.msg for e in self.entries]
