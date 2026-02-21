from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface


class HelloWorldModule(Module):
    log: LoggingInterface

    def __init__(self, config: ModuleConfig, logger: LoggerFactory) -> None:
        self.config = config
        self.logger = logger

    async def initialize(self) -> None:
        self.log = self.logger.create()

    async def execute(self) -> int:
        name = self.config.get("name")
        self.log.info(f"Hello, {name}!")
        return 0


module_class = HelloWorldModule
