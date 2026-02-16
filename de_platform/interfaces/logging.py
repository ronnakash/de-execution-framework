from abc import ABC, abstractmethod
from typing import Any


class LoggingInterface(ABC):
    """Structured logging with correlation ID propagation."""

    @abstractmethod
    def info(self, msg: str, **ctx: Any) -> None: ...

    @abstractmethod
    def warn(self, msg: str, **ctx: Any) -> None: ...

    @abstractmethod
    def error(self, msg: str, **ctx: Any) -> None: ...

    @abstractmethod
    def debug(self, msg: str, **ctx: Any) -> None: ...
