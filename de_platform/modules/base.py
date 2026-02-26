from abc import ABC, abstractmethod


class Module(ABC):
    """Base class for all platform modules with an async lifecycle.

    All lifecycle methods are async. Modules with purely synchronous logic
    simply omit ``await`` in their implementations — Python allows this.
    """

    async def initialize(self) -> None:
        """Set up resources needed by the module. Override as needed."""

    async def validate(self) -> None:
        """Validate preconditions before execution. Override as needed."""

    @abstractmethod
    async def execute(self) -> int:
        """Run the module logic. Must return an exit code."""
        ...

    async def teardown(self) -> None:
        """Clean up resources. Override as needed."""

    async def run(self) -> int:
        """Execute the full module lifecycle."""
        try:
            await self.initialize()
            await self.validate()
            return await self.execute()
        finally:
            await self.teardown()


# Backwards-compat alias — prefer importing Module directly.
AsyncModule = Module
