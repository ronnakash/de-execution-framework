import inspect
from typing import Any, TypeVar, get_type_hints

T = TypeVar("T")


class Container:
    """Constructor-based DI container. Stores pre-built objects and injects
    them into class constructors by matching parameter type hints."""

    def __init__(self) -> None:
        self._registry: dict[type, Any] = {}

    def register_instance(self, type_key: type, instance: Any) -> None:
        """Register a pre-built instance keyed by its type."""
        self._registry[type_key] = instance

    def register_factory(self, type_key: type, factory: Any) -> None:
        """Register a factory (semantic alias for register_instance)."""
        self._registry[type_key] = factory

    def resolve(self, cls: type[T]) -> T:
        """Instantiate *cls* by injecting registered dependencies into its constructor."""
        try:
            hints = get_type_hints(cls.__init__)
        except Exception as exc:
            raise TypeError(f"Cannot read type hints for {cls.__name__}.__init__: {exc}") from exc

        hints.pop("return", None)

        sig = inspect.signature(cls.__init__)
        kwargs: dict[str, Any] = {}
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            hint = hints.get(name)
            if hint is None:
                raise TypeError(
                    f"Parameter '{name}' of {cls.__name__}.__init__ has no type hint"
                )
            if hint not in self._registry:
                raise TypeError(
                    f"No registration found for type {hint.__name__!r} "
                    f"(parameter '{name}' of {cls.__name__}.__init__)"
                )
            kwargs[name] = self._registry[hint]

        return cls(**kwargs)

    def has(self, type_key: type) -> bool:
        """Check whether a type is registered."""
        return type_key in self._registry
