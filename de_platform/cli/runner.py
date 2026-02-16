import asyncio
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from de_platform.config.context import ModuleConfig, PlatformContext
from de_platform.implementations.memory_logger import MemoryLogger
from de_platform.implementations.pretty_logger import PrettyLogger
from de_platform.interfaces.logging import LoggingInterface

MODULES_DIR = Path(__file__).resolve().parent.parent.parent / "modules"

LOGGER_REGISTRY: dict[str, type[LoggingInterface]] = {
    "pretty": PrettyLogger,
    "memory": MemoryLogger,
}


def load_module_descriptor(module_name: str) -> dict[str, Any]:
    module_json = MODULES_DIR / module_name / "module.json"
    if not module_json.exists():
        print(f"Error: module '{module_name}' not found at {module_json}", file=sys.stderr)
        sys.exit(1)
    with open(module_json) as f:
        return json.load(f)


def parse_module_args(
    descriptor: dict[str, Any], raw_args: list[str]
) -> dict[str, Any]:
    """Parse CLI args against the module.json arg definitions."""
    arg_defs: list[dict[str, Any]] = descriptor.get("args", [])
    parsed: dict[str, Any] = {}

    # Build lookup by --name
    i = 0
    while i < len(raw_args):
        arg = raw_args[i]
        if arg.startswith("--"):
            key = arg[2:]
            if i + 1 < len(raw_args) and not raw_args[i + 1].startswith("--"):
                parsed[key] = raw_args[i + 1]
                i += 2
            else:
                parsed[key] = "true"
                i += 1
        else:
            i += 1

    # Apply defaults and validate
    result: dict[str, Any] = {}
    errors: list[str] = []

    for arg_def in arg_defs:
        name = arg_def["name"]
        if name in parsed:
            result[name] = _cast_value(parsed[name], arg_def.get("type", "string"))
        elif "default" in arg_def:
            result[name] = arg_def["default"]
        elif arg_def.get("required", False):
            errors.append(f"Missing required argument: --{name}")

        if name in result and "choices" in arg_def:
            if result[name] not in arg_def["choices"]:
                errors.append(
                    f"Invalid value for --{name}: '{result[name]}' "
                    f"(choices: {', '.join(str(c) for c in arg_def['choices'])})"
                )

    if errors:
        for err in errors:
            print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)

    return result


def _cast_value(value: str, type_name: str) -> Any:
    match type_name:
        case "integer":
            return int(value)
        case "float":
            return float(value)
        case "boolean":
            return value.lower() in ("true", "1", "yes")
        case _:
            return value


def print_module_help(descriptor: dict[str, Any]) -> None:
    print(f"\n  {descriptor['display_name']} v{descriptor['version']}")
    print(f"  {descriptor['description']}\n")
    print(f"  Type: {descriptor['type']}")
    print()

    args = descriptor.get("args", [])
    if args:
        print("  Module arguments:")
        for arg in args:
            required = " (required)" if arg.get("required") else ""
            default = f" [default: {arg['default']}]" if "default" in arg else ""
            choices_list = arg.get("choices")
            choices = f" (choices: {', '.join(str(c) for c in choices_list)})" if choices_list else ""
            print(f"    --{arg['name']:20s} {arg['description']}{required}{default}{choices}")
        print()

    print("  Global flags:")
    print(f"    --{'log':20s} Logging format: pretty, memory [default: pretty]")
    print()


def run_cli(argv: list[str] | None = None) -> None:
    args = argv if argv is not None else sys.argv[1:]

    if not args or args[0] != "run":
        print("Usage: python -m de_platform run <module_name> [flags] [module args]", file=sys.stderr)
        sys.exit(1)

    if len(args) < 2:
        print("Usage: python -m de_platform run <module_name> [flags] [module args]", file=sys.stderr)
        sys.exit(1)

    module_name = args[1]
    remaining = args[2:]

    descriptor = load_module_descriptor(module_name)

    # Check for --help
    if "--help" in remaining or "-h" in remaining:
        print_module_help(descriptor)
        sys.exit(0)

    # Extract global flags
    log_impl = "pretty"
    filtered_args: list[str] = []
    i = 0
    while i < len(remaining):
        if remaining[i] == "--log" and i + 1 < len(remaining):
            log_impl = remaining[i + 1]
            i += 2
        else:
            filtered_args.append(remaining[i])
            i += 1

    # Parse module args
    module_args = parse_module_args(descriptor, filtered_args)

    # Build context
    logger_cls = LOGGER_REGISTRY.get(log_impl, PrettyLogger)
    logger = logger_cls()
    config = ModuleConfig(module_args)
    context = PlatformContext(log=logger, config=config)

    # Import and run module
    module = importlib.import_module(f"modules.{module_name}.main")
    exit_code = asyncio.run(module.run(context))
    sys.exit(exit_code)
