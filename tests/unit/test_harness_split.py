"""Tests verifying the harness package split and backward compatibility."""

from __future__ import annotations


def test_import_from_package():
    """All public names are importable from the harness package."""
    from tests.helpers.harness import (
        MemoryHarness,
        PipelineHarness,
        RealInfraHarness,
        SharedPipeline,
        _free_port,
        _launch_module,
        _wait_for_http_sync,
        poll_until,
    )
    assert MemoryHarness is not None
    assert PipelineHarness is not None
    assert RealInfraHarness is not None
    assert SharedPipeline is not None
    assert callable(poll_until)
    assert callable(_free_port)
    assert callable(_launch_module)
    assert callable(_wait_for_http_sync)


def test_import_from_submodules():
    """Names are importable from individual submodules."""
    from tests.helpers.harness.memory import MemoryHarness
    from tests.helpers.harness.protocol import PipelineHarness, _free_port, poll_until
    from tests.helpers.harness.real_infra import RealInfraHarness
    from tests.helpers.harness.shared_pipeline import SharedPipeline, _launch_module

    assert PipelineHarness is not None
    assert MemoryHarness is not None
    assert SharedPipeline is not None
    assert RealInfraHarness is not None
    assert callable(poll_until)
    assert callable(_free_port)
    assert callable(_launch_module)


def test_free_port_returns_int():
    """_free_port() returns a valid port number."""
    from tests.helpers.harness.protocol import _free_port
    port = _free_port()
    assert isinstance(port, int)
    assert 1024 <= port <= 65535


async def test_poll_until_immediate():
    """poll_until returns immediately when predicate is True."""
    from tests.helpers.harness.protocol import poll_until
    await poll_until(lambda: True, timeout=1.0)


async def test_poll_until_timeout():
    """poll_until raises TimeoutError when predicate is always False."""
    import pytest

    from tests.helpers.harness.protocol import poll_until
    with pytest.raises(TimeoutError, match="condition not met"):
        await poll_until(lambda: False, timeout=0.1, interval=0.01)


async def test_poll_until_with_on_timeout():
    """poll_until includes on_timeout detail in error message."""
    import pytest

    from tests.helpers.harness.protocol import poll_until
    with pytest.raises(TimeoutError, match="custom detail"):
        await poll_until(
            lambda: False,
            timeout=0.1,
            interval=0.01,
            on_timeout=lambda: "custom detail",
        )


def test_memory_harness_creation():
    """MemoryHarness can be instantiated without errors."""
    from tests.helpers.harness.memory import MemoryHarness
    h = MemoryHarness()
    assert h.tenant_id == "acme"
    assert h.step_logger is not None


async def test_memory_harness_ensure_modules():
    """MemoryHarness._ensure_modules() initializes all pipeline modules."""
    from tests.helpers.harness.memory import MemoryHarness
    h = MemoryHarness()
    await h._ensure_modules()
    assert h.normalizer is not None
    assert h.persistence is not None
    assert h.algos is not None
    assert h.diagnostics is not None


def test_pipeline_harness_protocol():
    """MemoryHarness satisfies the PipelineHarness protocol."""
    from tests.helpers.harness.memory import MemoryHarness

    # Protocol compliance check: MemoryHarness has all required attributes
    h = MemoryHarness()
    assert hasattr(h, "tenant_id")
    assert hasattr(h, "step_logger")
    assert hasattr(h, "ingest")
    assert hasattr(h, "wait_for_rows")
    assert hasattr(h, "wait_for_no_new_rows")
    assert hasattr(h, "fetch_alerts")
    assert hasattr(h, "wait_for_alert")
    assert hasattr(h, "query_api")
    assert hasattr(h, "publish_to_normalizer")
    assert hasattr(h, "call_service")
    assert hasattr(h, "step")
