import importlib
import json
import sys
from pathlib import Path
from typing import Any

from de_platform.config.context import ModuleConfig, PlatformConfig, PlatformContext
from de_platform.modules.base import Module
from de_platform.services.logger.factory import LoggerFactory

MODULES_DIR = Path(__file__).resolve().parent.parent / "modules"


def load_module_descriptor(module_name: str) -> dict[str, Any]:
    module_json = MODULES_DIR / module_name / "module.json"
    if not module_json.exists():
        raise FileNotFoundError(f"module '{module_name}' not found at {module_json}")
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
        raise ValueError("; ".join(errors))

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


def _parse_env_overrides(raw: str) -> dict[str, str]:
    """Parse a JSON string into env overrides. Validates types."""
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("--env value must be a JSON object")
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError("--env JSON must have string keys and string values")
    return data


def print_module_help(descriptor: dict[str, Any]) -> None:
    version = descriptor.get("version", "")
    version_suffix = f" v{version}" if version else ""
    print(f"\n  {descriptor['display_name']}{version_suffix}")
    print(f"  {descriptor['description']}\n")
    module_type = descriptor.get("type")
    if module_type:
        print(f"  Type: {module_type}")
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
    print(f"    --{'env':20s} JSON string of env var overrides")
    print()


def run_module(argv: list[str]) -> tuple[int, Module]:
    """Testable entry point: parses args, builds context, runs module, returns (exit_code, module)."""
    if not argv or argv[0] != "run":
        raise ValueError("Usage: python -m de_platform run <module_name> [flags] [module args]")

    if len(argv) < 2:
        raise ValueError("Usage: python -m de_platform run <module_name> [flags] [module args]")

    module_name = argv[1]
    remaining = argv[2:]

    descriptor = load_module_descriptor(module_name)

    # Check for --help
    if "--help" in remaining or "-h" in remaining:
        print_module_help(descriptor)
        return (0, None)  # type: ignore[return-value]

    # Extract global flags
    log_impl = "pretty"
    env_overrides: dict[str, str] = {}
    filtered_args: list[str] = []
    i = 0
    while i < len(remaining):
        if remaining[i] == "--log" and i + 1 < len(remaining):
            log_impl = remaining[i + 1]
            i += 2
        elif remaining[i] == "--env" and i + 1 < len(remaining):
            env_overrides.update(_parse_env_overrides(remaining[i + 1]))
            i += 2
        else:
            filtered_args.append(remaining[i])
            i += 1

    # --log convenience: put it into env overrides so factory can read it
    if "LOG_IMPL" not in env_overrides:
        env_overrides["LOG_IMPL"] = log_impl

    # Parse module args
    module_args = parse_module_args(descriptor, filtered_args)

    # Build context with factories
    env = PlatformConfig(overrides=env_overrides)
    logger_factory = LoggerFactory(default_impl=env.get("LOG_IMPL", "pretty"))
    config = ModuleConfig(module_args)
    context = PlatformContext(config=config, env=env, logger=logger_factory)

    # Import module and find module_class
    mod = importlib.import_module(f"de_platform.modules.{module_name}.main")
    if not hasattr(mod, "module_class"):
        raise AttributeError(
            f"Module 'de_platform.modules.{module_name}.main' must define a 'module_class' attribute"
        )

    module_instance = mod.module_class(context)
    exit_code = module_instance.run()
    return (exit_code, module_instance)


def run_cli(argv: list[str] | None = None) -> None:
    args = argv if argv is not None else sys.argv[1:]
    try:
        exit_code, _ = run_module(args)
        sys.exit(exit_code)
    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
