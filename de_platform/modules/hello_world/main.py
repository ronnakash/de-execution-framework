from de_platform.modules.base import Module
from de_platform.services.logger.interface import LoggingInterface


class HelloWorldModule(Module):
    log: LoggingInterface

    def initialize(self) -> None:
        self.log = self.context.logger.create()

    def execute(self) -> int:
        name = self.context.config.get("name")
        self.log.info(f"Hello, {name}!")
        return 0


module_class = HelloWorldModule
