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
