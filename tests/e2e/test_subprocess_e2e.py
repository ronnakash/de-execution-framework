"""Subprocess E2E tests with real infrastructure.

The most production-realistic test suite: each pipeline module runs as a
separate OS subprocess via ``python3 -m de_platform run <module> ...``.

Same 34 test cases as the inprocess variant. Assertions use direct DB
queries and HTTP requests against the DataApi subprocess.

Requires: ``pytest -m real_infra`` or ``make test-real-infra``.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import pytest

from tests.e2e.conftest import InfraConfig

pytestmark = [pytest.mark.real_infra]


# ── Event factories ───────────────────────────────────────────────────────────


def _order(
    event_id: str | None = None,
    currency: str = "USD",
    quantity: float = 100.0,
    price: float = 200.0,
) -> dict[str, Any]:
    return {
        "event_type": "order",
        "id": event_id or f"o-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "order_type": "limit",
        "currency": currency,
    }


def _execution(event_id: str | None = None, currency: str = "EUR") -> dict[str, Any]:
    return {
        "event_type": "execution",
        "id": event_id or f"e-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "filled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "order_id": "o-abc",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": 50.0,
        "price": 210.0,
        "execution_venue": "NYSE",
        "currency": currency,
    }


def _transaction(
    event_id: str | None = None,
    currency: str = "GBP",
    amount: float = 500.0,
    counterparty_id: str = "cp1",
) -> dict[str, Any]:
    return {
        "event_type": "transaction",
        "id": event_id or f"tx-{uuid.uuid4().hex[:8]}",
        "tenant_id": "acme",
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": "acc1",
        "counterparty_id": counterparty_id,
        "amount": amount,
        "currency": currency,
        "transaction_type": "wire",
    }


def _invalid_event(event_type: str) -> dict[str, Any]:
    event = _EVENT_FACTORY[event_type]()
    event.pop("status")
    return event


_EVENT_FACTORY: dict[str, Any] = {
    "order": _order,
    "execution": _execution,
    "transaction": _transaction,
}
_EVENT_TABLE: dict[str, str] = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}
_REST_ENDPOINT: dict[str, str] = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}
_CLIENT_TOPIC: dict[str, str] = {
    "order": "client_orders",
    "execution": "client_executions",
    "transaction": "client_transactions",
}


# ── Utilities ─────────────────────────────────────────────────────────────────


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def poll_until(predicate: Any, timeout: float = 60.0, interval: float = 0.3) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise TimeoutError("poll_until: condition not met within timeout")
        await asyncio.sleep(interval)


async def _wait_for_http(url: str, timeout: float = 30.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status < 500:
                        return
        except (aiohttp.ClientConnectorError, aiohttp.ServerConnectionError, OSError):
            pass
        if loop.time() > deadline:
            raise TimeoutError(f"HTTP service at {url} not ready within {timeout}s")
        await asyncio.sleep(0.3)


# ── Subprocess launcher ──────────────────────────────────────────────────────


def _launch_module(
    module_name: str,
    infra: "InfraConfig",
    db_flags: list[str],
    extra_flags: list[str] | None = None,
    group_id: str = "test",
    health_port: int = 0,
) -> subprocess.Popen:
    """Launch a module as an OS subprocess."""
    cmd = [
        sys.executable, "-m", "de_platform", "run", module_name,
        *db_flags,
        "--mq", "kafka",
        "--cache", "redis",
        "--fs", "minio",
        "--log", "pretty",
        "--health-port", str(health_port),
        *(extra_flags or []),
    ]

    env = {**os.environ, **infra.to_env_overrides(group_id=group_id)}

    return subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


# ── Subprocess pipeline context ──────────────────────────────────────────────


@dataclass
class SubprocessCtx:
    infra: InfraConfig
    procs: dict[str, subprocess.Popen]
    rest_port: int
    api_port: int
    health_ports: dict[str, int]
    # Assertion helpers
    clickhouse_db: Any
    alerts_db: Any
    kafka_producer: Any
    minio_fs: Any


@asynccontextmanager
async def _make_subprocess_pipeline(
    infra: InfraConfig,
    algos_extra: list[str] | None = None,
):
    """Launch all 6 pipeline modules as OS subprocesses."""
    from de_platform.services.database.clickhouse_database import ClickHouseDatabase
    from de_platform.services.database.postgres_database import PostgresDatabase
    from de_platform.services.filesystem.minio_filesystem import MinioFileSystem
    from de_platform.services.message_queue.kafka_queue import KafkaQueue
    from de_platform.services.secrets.env_secrets import EnvSecrets

    rest_port = _free_port()
    api_port = _free_port()
    group_base = uuid.uuid4().hex[:8]

    module_specs: list[tuple[str, list[str], list[str]]] = [
        ("rest_starter", ["--mq", "kafka"], ["--port", str(rest_port)]),
        ("kafka_starter", ["--mq", "kafka"], []),
        ("normalizer", ["--db", "warehouse=postgres", "--cache", "redis", "--mq", "kafka"], []),
        (
            "persistence",
            ["--db", "default=clickhouse", "--fs", "minio", "--mq", "kafka"],
            ["--flush-threshold", "1", "--flush-interval", "0"],
        ),
        (
            "algos",
            ["--db", "default=postgres", "--cache", "redis", "--mq", "kafka"],
            algos_extra or [],
        ),
        (
            "data_api",
            ["--db", "events=clickhouse", "--db", "alerts=postgres", "--mq", "kafka"],
            ["--port", str(api_port)],
        ),
    ]

    procs: dict[str, subprocess.Popen] = {}
    health_ports: dict[str, int] = {}

    for module_name, db_flags, extra_flags in module_specs:
        hp = _free_port()
        health_ports[module_name] = hp
        gid = f"sub-{group_base}-{module_name}"

        # For rest_starter and kafka_starter, db_flags include --mq kafka already
        # but _launch_module also adds --mq kafka. Filter duplicates by
        # building the full flag set manually.
        all_flags = db_flags + extra_flags

        procs[module_name] = _launch_module(
            module_name=module_name,
            infra=infra,
            db_flags=[],  # we include these in extra_flags below
            extra_flags=all_flags,
            group_id=gid,
            health_port=hp,
        )

    # Wait for all health endpoints
    for name, hp in health_ports.items():
        try:
            await _wait_for_http(f"http://127.0.0.1:{hp}/health/startup", timeout=45)
        except TimeoutError:
            # Collect stderr for debugging
            for n, p in procs.items():
                if p.poll() is not None:
                    _, stderr = p.communicate(timeout=1)
                    print(f"[{n}] exited with code {p.returncode}: {stderr.decode()[:500]}")
            raise

    # Create assertion connections
    secrets = EnvSecrets(overrides=infra.to_env_overrides(group_id=f"assert-{group_base}"))

    ch_db = ClickHouseDatabase(secrets=secrets)
    ch_db.connect()

    alerts_db = PostgresDatabase(secrets=secrets, prefix="DB_ALERTS")
    await alerts_db.connect_async()

    producer = KafkaQueue(secrets)
    producer.connect()

    minio_fs = MinioFileSystem(secrets=secrets)

    ctx = SubprocessCtx(
        infra=infra,
        procs=procs,
        rest_port=rest_port,
        api_port=api_port,
        health_ports=health_ports,
        clickhouse_db=ch_db,
        alerts_db=alerts_db,
        kafka_producer=producer,
        minio_fs=minio_fs,
    )
    try:
        yield ctx
    finally:
        # SIGTERM all subprocesses
        for proc in procs.values():
            try:
                proc.send_signal(signal.SIGTERM)
            except OSError:
                pass
        for proc in procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

        # Cleanup assertion connections
        producer.disconnect()
        ch_db.disconnect()
        await alerts_db.disconnect_async()


@pytest.fixture
async def pipeline(infra):
    async with _make_subprocess_pipeline(infra) as ctx:
        yield ctx


@pytest.fixture
async def pipeline_suspicious_cp(infra):
    async with _make_subprocess_pipeline(
        infra,
        algos_extra=["--suspicious-counterparty-ids", "bad-cp-1"],
    ) as ctx:
        yield ctx


# ── Ingestion helpers ─────────────────────────────────────────────────────────


async def _ingest_rest(
    ctx: SubprocessCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    url = f"http://127.0.0.1:{ctx.rest_port}/api/v1/{_REST_ENDPOINT[event_type]}"
    async with aiohttp.ClientSession() as session:
        resp = await session.post(url, json={"events": events})
        assert resp.status == 200


def _ingest_kafka(
    ctx: SubprocessCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    topic = _CLIENT_TOPIC[event_type]
    for event in events:
        ctx.kafka_producer.publish(topic, event)


def _ingest_files(
    ctx: SubprocessCtx, event_type: str, events: list[dict[str, Any]]
) -> None:
    path = f"input/{event_type}/{uuid.uuid4().hex}.jsonl"
    ctx.minio_fs.write(
        path, ("\n".join(json.dumps(e) for e in events)).encode()
    )
    # Run file_processor as a subprocess job
    gid = f"fp-{uuid.uuid4().hex[:8]}"
    proc = _launch_module(
        module_name="file_processor",
        infra=ctx.infra,
        db_flags=[],
        extra_flags=[
            "--fs", "minio",
            "--mq", "kafka",
            "--file-path", path,
            "--event-type", event_type,
        ],
        group_id=gid,
        health_port=0,
    )
    proc.wait(timeout=30)
    assert proc.returncode == 0, f"file_processor failed: {proc.stderr.read().decode()[:500]}"


async def _ingest(
    ctx: SubprocessCtx, method: str, event_type: str, events: list[dict[str, Any]]
) -> None:
    if method == "rest":
        await _ingest_rest(ctx, event_type, events)
    elif method == "kafka":
        _ingest_kafka(ctx, event_type, events)
    elif method == "files":
        _ingest_files(ctx, event_type, events)


# ── Assertion helpers ─────────────────────────────────────────────────────────


def _ch_row_count(ctx: SubprocessCtx, table: str) -> int:
    return len(ctx.clickhouse_db.fetch_all(f"SELECT * FROM {table}"))


def _pg_alerts(ctx: SubprocessCtx) -> list[dict]:
    return ctx.alerts_db.fetch_all("SELECT * FROM alerts")


# ── Parametrize matrix ────────────────────────────────────────────────────────

_MATRIX = pytest.mark.parametrize(
    "method,event_type",
    [
        ("rest", "order"),
        ("rest", "execution"),
        ("rest", "transaction"),
        ("kafka", "order"),
        ("kafka", "execution"),
        ("kafka", "transaction"),
        ("files", "order"),
        ("files", "execution"),
        ("files", "transaction"),
    ],
)


# ── Scenario 1: 100 valid events ──────────────────────────────────────────────


@_MATRIX
async def test_100_valid_events_stored_and_accessible(pipeline, method, event_type):
    """100 valid events land in ClickHouse and are queryable via API."""
    events = [_EVENT_FACTORY[event_type]() for _ in range(100)]
    await _ingest(pipeline, method, event_type, events)

    table = _EVENT_TABLE[event_type]
    await poll_until(lambda: _ch_row_count(pipeline, table) >= 100)
    assert _ch_row_count(pipeline, table) == 100

    # Verify via DataApi HTTP endpoint
    url = (
        f"http://127.0.0.1:{pipeline.api_port}"
        f"/api/v1/events/{table}?tenant_id=acme&limit=200"
    )
    async with aiohttp.ClientSession() as session:
        resp = await session.get(url)
        assert resp.status == 200
        rows = await resp.json()
    assert len(rows) == 100


# ── Scenario 2: 100 invalid events ───────────────────────────────────────────


@_MATRIX
async def test_100_invalid_events_go_to_errors_table(pipeline, method, event_type):
    """100 invalid events -> normalization_errors, not the valid table."""
    invalid_events = [_invalid_event(event_type) for _ in range(100)]
    await _ingest(pipeline, method, event_type, invalid_events)

    await poll_until(
        lambda: _ch_row_count(pipeline, "normalization_errors") >= 100
    )
    assert _ch_row_count(pipeline, "normalization_errors") == 100
    table = _EVENT_TABLE[event_type]
    assert _ch_row_count(pipeline, table) == 0


# ── Scenario 3: same event 100 times ─────────────────────────────────────────


@_MATRIX
async def test_same_event_100_times_yields_1_valid_99_duplicates(
    pipeline, method, event_type
):
    """Same event 100x -> 1 valid + 99 duplicates."""
    fixed_id = f"fixed-{uuid.uuid4().hex[:8]}"
    same_events = [_EVENT_FACTORY[event_type](event_id=fixed_id) for _ in range(100)]
    await _ingest(pipeline, method, event_type, same_events)

    table = _EVENT_TABLE[event_type]
    await poll_until(
        lambda: (
            _ch_row_count(pipeline, table) >= 1
            and _ch_row_count(pipeline, "duplicates") >= 99
        ),
    )
    assert _ch_row_count(pipeline, table) == 1
    assert _ch_row_count(pipeline, "duplicates") == 99


# ── General: internal deduplication ──────────────────────────────────────────


async def test_internal_dedup_same_message_id_is_silently_discarded(pipeline):
    """Same message_id arriving twice at the normalizer -> second is dropped."""
    from de_platform.pipeline.topics import TRADE_NORMALIZATION

    event = {
        **_order(),
        "message_id": "dedup-test-fixed-msg-id",
        "ingested_at": "2026-01-15T10:00:00+00:00",
    }
    pipeline.kafka_producer.publish(TRADE_NORMALIZATION, event)
    await poll_until(lambda: _ch_row_count(pipeline, "orders") >= 1)
    assert _ch_row_count(pipeline, "orders") == 1

    pipeline.kafka_producer.publish(TRADE_NORMALIZATION, dict(event))
    await asyncio.sleep(5.0)
    assert _ch_row_count(pipeline, "orders") == 1


# ── General: alert generation per ingestion method ────────────────────────────


async def test_alert_generation_via_rest(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    await _ingest_rest(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(r.get("algorithm") == "large_notional" for r in _pg_alerts(pipeline))
    )


async def test_alert_generation_via_kafka(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    _ingest_kafka(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(r.get("algorithm") == "large_notional" for r in _pg_alerts(pipeline))
    )


async def test_alert_generation_via_files(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    _ingest_files(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(r.get("algorithm") == "large_notional" for r in _pg_alerts(pipeline))
    )


# ── General: per-algorithm alert tests ───────────────────────────────────────


async def test_large_notional_algorithm(pipeline):
    big_order = _order(price=15_000.0, quantity=100.0, currency="USD")
    await _ingest_rest(pipeline, "order", [big_order])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "large_notional" and r.get("severity") == "high"
            for r in _pg_alerts(pipeline)
        )
    )


async def test_velocity_algorithm(pipeline):
    orders = [_order() for _ in range(101)]
    await _ingest_rest(pipeline, "order", orders)

    await poll_until(
        lambda: any(r.get("algorithm") == "velocity" for r in _pg_alerts(pipeline))
    )


async def test_suspicious_counterparty_algorithm(pipeline_suspicious_cp):
    suspicious_tx = _transaction(counterparty_id="bad-cp-1")
    await _ingest_rest(pipeline_suspicious_cp, "transaction", [suspicious_tx])

    await poll_until(
        lambda: any(
            r.get("algorithm") == "suspicious_counterparty"
            for r in _pg_alerts(pipeline_suspicious_cp)
        )
    )
