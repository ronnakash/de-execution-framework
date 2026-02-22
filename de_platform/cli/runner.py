from __future__ import annotations

import asyncio
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from de_platform.config.container import Container
from de_platform.config.context import ModuleConfig, PlatformConfig
from de_platform.config.env_loader import load_env_file
from de_platform.modules.base import Module
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.database.factory import DatabaseFactory, DbConfig
from de_platform.services.health.health_server import HealthCheckServer
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.registry import resolve_implementation, resolve_interface_type
from de_platform.services.secrets.env_secrets import EnvSecrets
from de_platform.services.secrets.interface import SecretsInterface

MODULES_DIR = Path(__file__).resolve().parent.parent / "modules"

# Global flags that select interface implementations.
# Maps flag name -> default value (None = not registered unless explicitly requested).
_GLOBAL_FLAGS: dict[str, str | None] = {
    "db": None,
    "fs": None,
    "cache": None,
    "mq": None,
    "metrics": None,
    "log": "pretty",
}

# Module types that should get health check server and lifecycle signal handling
_SERVICE_TYPES = {"service", "worker"}


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


def _extract_global_flags(
    remaining: list[str],
) -> tuple[dict[str, str], dict[str, str], list[str], list[str], int]:
    """Extract global flags from remaining args.

    Returns (impl_flags, env_overrides, filtered_module_args, db_entries, health_port).
    impl_flags maps flag names (fs, cache, mq, metrics, log) to selected impl.
    db_entries is a list of raw --db values (e.g. ["memory", "warehouse=postgres"]).
    """
    impl_flags: dict[str, str] = {}
    env_overrides: dict[str, str] = {}
    db_entries: list[str] = []
    env_file: str | None = None
    health_port: int = 8080
    filtered_args: list[str] = []

    all_flag_names = set(_GLOBAL_FLAGS.keys()) | {"env", "env-file", "health-port"}

    i = 0
    while i < len(remaining):
        flag = remaining[i]
        if flag.startswith("--") and flag[2:] in all_flag_names and i + 1 < len(remaining):
            name = flag[2:]
            value = remaining[i + 1]
            if name == "env":
                env_overrides.update(_parse_env_overrides(value))
            elif name == "env-file":
                env_file = value
            elif name == "db":
                db_entries.append(value)
            elif name == "health-port":
                health_port = int(value)
            else:
                impl_flags[name] = value
            i += 2
        else:
            filtered_args.append(remaining[i])
            i += 1

    # Load .env file if specified
    if env_file:
        file_vars = load_env_file(env_file)
        # File vars are lower priority — env_overrides (from --env) win
        merged = dict(file_vars)
        merged.update(env_overrides)
        env_overrides = merged

    # --log convenience: put it into env overrides so factory can read it
    if "log" in impl_flags and "LOG_IMPL" not in env_overrides:
        env_overrides["LOG_IMPL"] = impl_flags["log"]

    return impl_flags, env_overrides, filtered_args, db_entries, health_port


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
            choices = (
                f" (choices: {', '.join(str(c) for c in choices_list)})"
                if choices_list
                else ""
            )
            print(
                f"    --{arg['name']:20s} {arg['description']}{required}{default}{choices}"
            )
        print()

    print("  Global flags:")
    print(f"    --{'db':20s} Database: memory, postgres [default: none]")
    print(f"    --{'fs':20s} File system: memory, local [default: none]")
    print(f"    --{'cache':20s} Cache: memory, redis [default: none]")
    print(f"    --{'mq':20s} Message queue: memory, kafka [default: none]")
    print(f"    --{'metrics':20s} Metrics: noop, memory [default: none]")
    print(f"    --{'log':20s} Logging format: pretty, memory [default: pretty]")
    print(f"    --{'health-port':20s} Health check HTTP port (service/worker only) [default: 8080]")
    print(f"    --{'env':20s} JSON string of env var overrides")
    print(f"    --{'env-file':20s} Environment file name (loads .env/<name>.env)")
    print()


def _parse_db_entries(
    db_entries: list[str], secrets: SecretsInterface
) -> dict[str, DbConfig]:
    """Parse --db entries into DbConfig instances.

    Supports:
      --db memory           -> name="default", impl=MemoryDatabase
      --db warehouse=postgres -> name="warehouse", impl=PostgresDatabase, prefix=DB_WAREHOUSE
    """
    configs: dict[str, DbConfig] = {}
    for entry in db_entries:
        if "=" in entry:
            name, impl_name = entry.split("=", 1)
        else:
            name = "default"
            impl_name = entry
        impl_cls = resolve_implementation("db", impl_name)
        prefix = f"DB_{name.upper()}"
        # Check if implementation needs secrets (e.g. PostgresDatabase)
        hints = _get_init_hints(impl_cls)
        needs_secrets = bool(hints)
        configs[name] = DbConfig(
            impl_cls=impl_cls,
            secrets=secrets if needs_secrets else None,
            prefix=prefix,
        )
    return configs


def _build_container(
    impl_flags: dict[str, str],
    env_overrides: dict[str, str],
    module_args: dict[str, Any],
    db_entries: list[str] | None = None,
    health_port: int = 8080,
    module_type: str = "job",
) -> Container:
    """Build the DI container with all registered services."""
    container = Container()

    # 0. Register the container itself so modules can resolve ETL/plugin classes
    container.register_instance(Container, container)

    # 1. Bootstrap secrets (always available)
    secrets = EnvSecrets(overrides=env_overrides)
    container.register_instance(SecretsInterface, secrets)

    # 2. Platform-level config
    env = PlatformConfig(overrides=env_overrides)
    container.register_instance(PlatformConfig, env)

    config = ModuleConfig(module_args)
    container.register_instance(ModuleConfig, config)

    # 3. Logger factory — --log flag takes precedence, then LOG_IMPL env override
    log_impl = impl_flags.get("log") or env_overrides.get("LOG_IMPL", "pretty")
    logger_factory = LoggerFactory(default_impl=log_impl)
    container.register_factory(LoggerFactory, logger_factory)

    # 4. Lifecycle manager (always registered so any module can use it)
    lifecycle = LifecycleManager()
    container.register_instance(LifecycleManager, lifecycle)

    # 5. Health check server (for service/worker modules)
    if module_type in _SERVICE_TYPES:
        health_server = HealthCheckServer(port=health_port)
        lifecycle.set_health_server(health_server)
        container.register_instance(HealthCheckServer, health_server)

    # 6. Register DatabaseFactory from --db entries
    if db_entries:
        db_configs = _parse_db_entries(db_entries, secrets)
        factory = DatabaseFactory(db_configs)
        container.register_instance(DatabaseFactory, factory)

        # Also register DatabaseInterface as a singleton so modules that
        # declare ``db: DatabaseInterface`` get it injected automatically.
        # Priority: "default" entry > single named entry.
        from de_platform.services.database.interface import DatabaseInterface

        singleton_name: str | None = None
        if "default" in db_configs:
            singleton_name = "default"
        elif len(db_configs) == 1:
            singleton_name = next(iter(db_configs))

        if singleton_name is not None:
            db_instance = factory.get(singleton_name)
            container.register_instance(DatabaseInterface, db_instance)
            # Register health check if this is a service/worker
            if module_type in _SERVICE_TYPES and container.has(HealthCheckServer):
                hs = container._registry[HealthCheckServer]
                if hasattr(db_instance, "health_check"):
                    hs.register_check("db", db_instance.health_check)

    # 7. Register each requested interface implementation (non-db)
    for flag_name, impl_name in impl_flags.items():
        if flag_name == "log":
            continue  # handled above via LoggerFactory
        impl_cls = resolve_implementation(flag_name, impl_name)
        interface_type = resolve_interface_type(flag_name)

        # If the implementation's __init__ needs SecretsInterface, resolve via container
        hints = {}
        try:
            hints = {
                k: v
                for k, v in _get_init_hints(impl_cls).items()
                if k != "return"
            }
        except Exception:
            pass

        if hints:
            instance = container.resolve(impl_cls)
        else:
            instance = impl_cls()

        container.register_instance(interface_type, instance)

        # Register health checks for service/worker modules
        if module_type in _SERVICE_TYPES and container.has(HealthCheckServer):
            hs = container._registry[HealthCheckServer]
            if hasattr(instance, "health_check"):
                hs.register_check(flag_name, instance.health_check)

    # 8. Ensure MetricsInterface is always available (NoopMetrics as default)
    from de_platform.services.metrics.interface import MetricsInterface

    if not container.has(MetricsInterface):
        from de_platform.services.metrics.noop_metrics import NoopMetrics

        container.register_instance(MetricsInterface, NoopMetrics())

    # 9. Wrap DatabaseInterface with observability (timing histograms by caller)
    from de_platform.services.database.interface import DatabaseInterface as _DBI

    if container.has(_DBI):
        from de_platform.services.database.observable_database import ObservableDatabase

        raw_db = container._registry[_DBI]
        if not isinstance(raw_db, ObservableDatabase):
            wrapped = ObservableDatabase(raw_db, container._registry[MetricsInterface])
            container.register_instance(_DBI, wrapped)

    return container


def _get_init_hints(cls: type) -> dict[str, Any]:
    """Get type hints for cls.__init__, returning empty dict on failure."""
    try:
        from typing import get_type_hints

        hints = get_type_hints(cls.__init__)
        hints.pop("return", None)
        return hints
    except Exception:
        return {}


async def _run_service_module(module_instance: Any, container: Container) -> int:
    """Run a service/worker module with health check server and lifecycle management."""
    import asyncio

    lifecycle: LifecycleManager = container._registry[LifecycleManager]
    health_server: HealthCheckServer = container._registry[HealthCheckServer]

    # Start health server
    await health_server.start()

    # Install signal handlers on the running loop
    loop = asyncio.get_event_loop()
    lifecycle.install_signal_handlers(loop=loop)

    try:
        # Mark startup complete after initialize
        health_server.mark_started()
        exit_code = await module_instance.run()
    finally:
        await lifecycle.shutdown()

    return exit_code


def run_module(argv: list[str]) -> tuple[int, Module]:
    """Testable entry point: parses args, builds container, runs module, returns (exit_code, module)."""
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

    module_type = descriptor.get("type", "job")

    # Extract global flags and module args
    impl_flags, env_overrides, filtered_args, db_entries, health_port = (
        _extract_global_flags(remaining)
    )

    # Parse module-specific args
    module_args = parse_module_args(descriptor, filtered_args)

    # Build DI container
    container = _build_container(
        impl_flags, env_overrides, module_args, db_entries,
        health_port=health_port, module_type=module_type,
    )

    # Import module and find module_class
    mod = importlib.import_module(f"de_platform.modules.{module_name}.main")
    if not hasattr(mod, "module_class"):
        raise AttributeError(
            f"Module 'de_platform.modules.{module_name}.main' must define a 'module_class' attribute"
        )

    module_instance = container.resolve(mod.module_class)

    # Service/worker modules get the full lifecycle treatment
    if module_type in _SERVICE_TYPES:
        exit_code = asyncio.run(_run_service_module(module_instance, container))
    else:
        exit_code = asyncio.run(module_instance.run())

    return (exit_code, module_instance)


def run_cli(argv: list[str] | None = None) -> None:
    args = argv if argv is not None else sys.argv[1:]
    try:
        exit_code, _ = run_module(args)
        sys.exit(exit_code)
    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
