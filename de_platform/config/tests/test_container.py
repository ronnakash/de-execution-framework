import pytest

from de_platform.config.container import Container


class DummyDep:
    pass


class NoDeps:
    def __init__(self) -> None:
        self.value = 42


class WithDep:
    def __init__(self, dep: DummyDep) -> None:
        self.dep = dep


class MissingHint:
    def __init__(self, dep) -> None:  # noqa: ANN001
        self.dep = dep


def test_resolve_injected_instance():
    container = Container()
    dep = DummyDep()
    container.register_instance(DummyDep, dep)
    obj = container.resolve(WithDep)
    assert obj.dep is dep


def test_resolve_injected_factory():
    container = Container()
    dep = DummyDep()
    container.register_factory(DummyDep, dep)
    obj = container.resolve(WithDep)
    assert obj.dep is dep


def test_resolve_no_dependencies():
    container = Container()
    obj = container.resolve(NoDeps)
    assert obj.value == 42


def test_raises_on_missing_registration():
    container = Container()
    with pytest.raises(TypeError, match="No registration found for type 'DummyDep'"):
        container.resolve(WithDep)


def test_raises_on_missing_type_hint():
    container = Container()
    with pytest.raises(TypeError, match="has no type hint"):
        container.resolve(MissingHint)


def test_has_returns_correct_values():
    container = Container()
    assert container.has(DummyDep) is False
    container.register_instance(DummyDep, DummyDep())
    assert container.has(DummyDep) is True
