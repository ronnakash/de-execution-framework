from abc import ABC, abstractmethod


class Module(ABC):
    """Base class for all platform modules with a standard lifecycle."""

    def initialize(self) -> None:
        """Set up resources needed by the module. Override as needed."""

    def validate(self) -> None:
        """Validate preconditions before execution. Override as needed."""

    @abstractmethod
    def execute(self) -> int:
        """Run the module logic. Must return an exit code."""
        ...

    def teardown(self) -> None:
        """Clean up resources. Override as needed."""

    def run(self) -> int:
        """Execute the full module lifecycle."""
        try:
            self.initialize()
            self.validate()
            return self.execute()
        finally:
            self.teardown()


class AsyncModule(ABC):
    """Base class for async modules (e.g. those using asyncpg).

    The CLI runner detects the coroutine returned by run() and uses asyncio.run().
    """

    async def initialize(self) -> None:
        """Async setup. Override as needed."""

    async def validate(self) -> None:
        """Async precondition checks. Override as needed."""

    @abstractmethod
    async def execute(self) -> int:
        """Async module logic. Must return an exit code."""
        ...

    async def teardown(self) -> None:
        """Async cleanup. Override as needed."""

    async def run(self) -> int:
        """Execute the full async module lifecycle."""
        try:
            await self.initialize()
            await self.validate()
            return await self.execute()
        finally:
            await self.teardown()
