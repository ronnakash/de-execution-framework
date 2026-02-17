"""Tests for runner.py Phase 4 wiring: lifecycle, health checks, --health-port."""

from __future__ import annotations

import pytest

from de_platform.cli.runner import (
    _build_container,
    _extract_global_flags,
)
from de_platform.services.health.health_server import HealthCheckServer
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager


def test_lifecycle_manager_registered_for_job():
    """Job modules always get a LifecycleManager."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="job",
    )
    assert container.has(LifecycleManager)


def test_no_health_server_for_job():
    """Job modules should NOT get a HealthCheckServer."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="job",
    )
    assert not container.has(HealthCheckServer)


def test_health_server_registered_for_service():
    """Service modules get both LifecycleManager and HealthCheckServer."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="service",
    )
    assert container.has(LifecycleManager)
    assert container.has(HealthCheckServer)


def test_health_server_registered_for_worker():
    """Worker modules get HealthCheckServer too."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="worker",
    )
    assert container.has(HealthCheckServer)


def test_health_port_default():
    """Default health port is 8080."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="service",
        health_port=8080,
    )
    hs = container._registry[HealthCheckServer]
    assert hs._port == 8080


def test_health_port_custom():
    """--health-port overrides the default."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="service",
        health_port=9090,
    )
    hs = container._registry[HealthCheckServer]
    assert hs._port == 9090


def test_extract_global_flags_health_port():
    """--health-port is parsed from CLI args."""
    flags, _env, _args, _db, health_port = _extract_global_flags([
        "--log", "memory", "--health-port", "9999",
    ])
    assert health_port == 9999


def test_extract_global_flags_health_port_default():
    """Default health port when not specified."""
    flags, _env, _args, _db, health_port = _extract_global_flags([
        "--log", "memory",
    ])
    assert health_port == 8080


def test_health_checks_registered_for_service_with_db():
    """When --db is used with a service module, health check is registered."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        db_entries=["memory"],
        module_type="service",
    )
    hs = container._registry[HealthCheckServer]
    assert "db" in hs._checks


def test_health_checks_registered_for_service_with_cache():
    """When --cache is used with a service module, health check is registered."""
    container = _build_container(
        impl_flags={"log": "memory", "cache": "memory"},
        env_overrides={},
        module_args={},
        module_type="service",
    )
    hs = container._registry[HealthCheckServer]
    assert "cache" in hs._checks


def test_lifecycle_linked_to_health_server():
    """LifecycleManager has the health server linked for shutdown."""
    container = _build_container(
        impl_flags={"log": "memory"},
        env_overrides={},
        module_args={},
        module_type="service",
    )
    lm = container._registry[LifecycleManager]
    assert lm._health_server is not None
    assert isinstance(lm._health_server, HealthCheckServer)
