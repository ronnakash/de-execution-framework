from de_platform.config.context import PlatformContext


def run(ctx: PlatformContext) -> int:
    name = ctx.config.get("name")
    ctx.log.info(f"Hello, {name}!")
    return 0
