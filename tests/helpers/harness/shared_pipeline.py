"""SharedPipeline -- session-scoped, one pipeline instance for ALL tests."""

from __future__ import annotations

import fcntl
import json as _json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from tests.helpers.harness.protocol import _free_port


def _wait_for_http_sync(url: str, timeout: float = 45.0) -> None:
    """Synchronous HTTP poll -- used by SharedPipeline (no event loop)."""
    deadline = time.monotonic() + timeout
    last_error = "no connection attempted"
    while True:
        try:
            req = urllib.request.urlopen(url, timeout=5)
            if req.status < 500:
                return
            last_error = f"status={req.status}"
        except (urllib.error.URLError, OSError) as e:
            last_error = str(e)
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"HTTP service at {url} not ready within {timeout}s (last: {last_error})"
            )
        time.sleep(0.2)


def _launch_module(
    module_name: str,
    infra: Any,
    db_flags: list[str],
    extra_flags: list[str] | None = None,
    group_id: str = "test",
    health_port: int = 0,
    extra_env: dict[str, str] | None = None,
    log_dir: Path | None = None,
) -> subprocess.Popen:
    """Launch a module as an OS subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "de_platform",
        "run",
        module_name,
        *db_flags,
        "--mq",
        "kafka",
        "--cache",
        "redis",
        "--fs",
        "minio",
        "--log",
        "pretty",
        "--health-port",
        str(health_port),
        *(extra_flags or []),
    ]
    env = {**os.environ, **infra.to_env_overrides(group_id=group_id), **(extra_env or {})}
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        out = open(log_dir / f"{module_name}.log", "w")
        return subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT)
    return subprocess.Popen(
        cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


class SharedPipeline:
    """Session-scoped pipeline that starts 11 module subprocesses once.

    Uses a file lock so only ONE xdist worker starts the pipeline;
    other workers connect to the same subprocesses.  The last worker
    to close kills the subprocesses.

    Fully synchronous -- no event loop required during setup/teardown.
    ClickHouse and Kafka connections are sync and shared across tests.
    PostgresDatabase (asyncpg) is NOT managed here -- each RealInfraHarness
    creates its own connection in the test's event loop.
    """

    JWT_SECRET = "e2e-test-jwt-secret-at-least-32-bytes-long"

    def __init__(self, infra: Any, lock_dir: Path) -> None:
        self._infra = infra
        self._lock_dir = lock_dir
        self._lock_file = lock_dir / "pipeline.lock"
        self._info_file = lock_dir / "pipeline.json"
        self._counter_file = lock_dir / "pipeline_workers.count"
        self._procs: dict[str, subprocess.Popen] = {}
        self._is_owner = False

        self.rest_port: int = 0
        self.api_port: int = 0
        self.config_port: int = 0
        self.auth_port: int = 0
        self.alert_manager_port: int = 0
        self.data_audit_port: int = 0
        self.task_scheduler_port: int = 0
        self._health_ports: dict[str, int] = {}
        self._clickhouse_db: Any = None
        self._kafka_producer: Any = None
        self._minio_fs: Any = None
        self._bootstrap_servers: str = ""

    def start(self) -> None:
        """Start or connect to the shared pipeline (file-lock coordinated)."""
        with open(self._lock_file, "w") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                if self._info_file.exists():
                    info = _json.loads(self._info_file.read_text())
                    self.rest_port = info["rest_port"]
                    self.api_port = info["api_port"]
                    self.config_port = info.get("config_port", 0)
                    self.auth_port = info.get("auth_port", 0)
                    self.alert_manager_port = info.get("alert_manager_port", 0)
                    self.data_audit_port = info.get("data_audit_port", 0)
                    self.task_scheduler_port = info.get("task_scheduler_port", 0)
                    self._health_ports = info.get("health_ports", {})
                else:
                    self._start_subprocesses()
                    self._info_file.write_text(_json.dumps({
                        "rest_port": self.rest_port,
                        "api_port": self.api_port,
                        "config_port": self.config_port,
                        "auth_port": self.auth_port,
                        "alert_manager_port": self.alert_manager_port,
                        "data_audit_port": self.data_audit_port,
                        "task_scheduler_port": self.task_scheduler_port,
                        "health_ports": self._health_ports,
                        "pids": {n: p.pid for n, p in self._procs.items()},
                    }))
                    self._is_owner = True

                # Increment worker counter
                count = int(self._counter_file.read_text()) if self._counter_file.exists() else 0
                self._counter_file.write_text(str(count + 1))
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

        # All workers: connect sync services (ClickHouse, Kafka, MinIO)
        self._connect_services()

        # Wait for ALL module health endpoints (ensures modules have
        # initialized and connected to services before we start producing).
        for name, hp in self._health_ports.items():
            if hp:
                try:
                    _wait_for_http_sync(
                        f"http://127.0.0.1:{hp}/health/startup",
                        timeout=60.0,
                    )
                except TimeoutError:
                    crash = self._check_procs_alive() if self._procs else None
                    raise TimeoutError(
                        f"Module '{name}' health endpoint not ready within 60s. "
                        f"Subprocess status: {crash or 'running'}"
                    )

        if self._is_owner:
            self._wait_for_consumer_readiness()

    def _start_subprocesses(self) -> None:
        from de_platform.pipeline.topics import (
            ALERTS,
            AUDIT_COUNTS,
            DUPLICATES,
            EXECUTIONS_PERSISTENCE,
            NORMALIZATION_ERRORS,
            ORDERS_PERSISTENCE,
            TRADE_NORMALIZATION,
            TRADES_ALGOS,
            TRANSACTIONS_ALGOS,
            TRANSACTIONS_PERSISTENCE,
            TX_NORMALIZATION,
        )

        infra = self._infra
        self.rest_port = _free_port()
        self.api_port = _free_port()
        self.config_port = _free_port()
        self.auth_port = _free_port()
        self.alert_manager_port = _free_port()
        self.data_audit_port = _free_port()
        self.task_scheduler_port = _free_port()

        # Per-module Kafka topic subscriptions: pre-subscribe all topics at
        # connect time to avoid incremental rebalance storms.
        _subscribe = {
            "kafka_starter": "client_orders,client_executions,client_transactions",
            "normalizer": f"{TRADE_NORMALIZATION},{TX_NORMALIZATION}",
            "persistence": ",".join([
                ORDERS_PERSISTENCE, EXECUTIONS_PERSISTENCE,
                TRANSACTIONS_PERSISTENCE, DUPLICATES, NORMALIZATION_ERRORS,
            ]),
            "algos": f"{TRADES_ALGOS},{TRANSACTIONS_ALGOS}",
            "alert_manager": ALERTS,
            "data_audit": ",".join([
                AUDIT_COUNTS,
                ORDERS_PERSISTENCE, EXECUTIONS_PERSISTENCE,
                TRANSACTIONS_PERSISTENCE, NORMALIZATION_ERRORS, DUPLICATES,
            ]),
        }

        _jwt_secret = self.JWT_SECRET

        module_specs: list[tuple[str, list[str], list[str]]] = [
            ("rest_starter", ["--mq", "kafka"], ["--port", str(self.rest_port)]),
            ("kafka_starter", ["--mq", "kafka"], []),
            ("normalizer", ["--db", "warehouse=postgres", "--cache", "redis", "--mq", "kafka"], []),
            ("persistence", ["--db", "clickhouse=clickhouse", "--fs", "minio", "--mq", "kafka"],
             ["--flush-threshold", "1", "--flush-interval", "0"]),
            ("algos", ["--cache", "redis", "--mq", "kafka"],
             ["--suspicious-counterparty-ids", "bad-cp-1"]),
            ("data_api", ["--db", "events=clickhouse"],
             ["--port", str(self.api_port)]),
            ("alert_manager", ["--db", "alerts=postgres", "--mq", "kafka", "--cache", "redis"],
             ["--port", str(self.alert_manager_port)]),
            ("client_config", ["--db", "client_config=postgres", "--cache", "redis"],
             ["--port", str(self.config_port)]),
            ("auth", ["--db", "auth=postgres"],
             ["--port", str(self.auth_port)]),
            ("data_audit", ["--db", "data_audit=postgres", "--mq", "kafka"],
             ["--port", str(self.data_audit_port),
              "--flush-threshold", "1", "--flush-interval", "1"]),
            ("task_scheduler", ["--db", "task_scheduler=postgres"],
             ["--port", str(self.task_scheduler_port)]),
        ]

        # Per-module extra env vars
        _module_env_overrides: dict[str, dict[str, str]] = {
            "auth": {"JWT_SECRET": _jwt_secret},
            "data_api": {
                "JWT_SECRET": _jwt_secret,
                "AUTH_URL": f"http://localhost:{self.auth_port}",
                "ALERT_MANAGER_URL": f"http://localhost:{self.alert_manager_port}",
                "CLIENT_CONFIG_URL": f"http://localhost:{self.config_port}",
                "DATA_AUDIT_URL": f"http://localhost:{self.data_audit_port}",
                "TASK_SCHEDULER_URL": f"http://localhost:{self.task_scheduler_port}",
            },
        }

        for module_name, db_flags, extra_flags in module_specs:
            hp = _free_port()
            self._health_ports[module_name] = hp
            all_flags = db_flags + extra_flags
            # Each module gets its own consumer group to avoid cross-module
            # rebalance storms (each module subscribes to different topics;
            # sharing a group causes cascading rebalances as topics are added).
            module_env = dict(_module_env_overrides.get(module_name, {}))
            if module_name in _subscribe:
                module_env["MQ_KAFKA_SUBSCRIBE_TOPICS"] = _subscribe[module_name]
            self._procs[module_name] = _launch_module(
                module_name=module_name,
                infra=infra,
                db_flags=[],
                extra_flags=all_flags,
                group_id=f"e2e-{module_name}",
                health_port=hp,
                extra_env=module_env,
                log_dir=self._lock_dir / "logs",
            )

    def _connect_services(self) -> None:
        """Connect sync-only services (ClickHouse, Kafka, MinIO).

        PostgresDatabase (async) is NOT connected here -- each RealInfraHarness
        creates its own asyncpg connection in the test's event loop.
        """
        from de_platform.services.database.clickhouse_database import ClickHouseDatabase
        from de_platform.services.filesystem.minio_filesystem import MinioFileSystem
        from de_platform.services.message_queue.kafka_queue import KafkaQueue
        from de_platform.services.secrets.env_secrets import EnvSecrets

        infra = self._infra
        secrets = EnvSecrets(
            overrides=infra.to_env_overrides(group_id=f"assert-{uuid.uuid4().hex[:8]}")
        )

        ch_db = ClickHouseDatabase(secrets=secrets)
        ch_db.connect()
        self._clickhouse_db = ch_db

        producer = KafkaQueue(secrets)
        producer.connect()
        self._kafka_producer = producer

        self._minio_fs = MinioFileSystem(secrets=secrets)
        self._bootstrap_servers = infra.kafka_bootstrap_servers

    def _check_procs_alive(self) -> str | None:
        """Return error string if any subprocess has exited, else None."""
        for name, proc in self._procs.items():
            if proc.poll() is not None:
                stderr = ""
                try:
                    _, stderr_bytes = proc.communicate(timeout=1)
                    stderr = stderr_bytes.decode()[:1000]
                except Exception:
                    pass
                return f"Module '{name}' exited with code {proc.returncode}: {stderr}"
        return None

    def _wait_for_consumer_readiness(self) -> None:
        """Produce sentinel events until one appears in ClickHouse (sync)."""
        from de_platform.pipeline.topics import TRADE_NORMALIZATION

        deadline = time.monotonic() + 45.0
        seen_ids: list[str] = []

        while time.monotonic() < deadline:
            # Check for crashed subprocesses
            crash = self._check_procs_alive()
            if crash:
                raise RuntimeError(f"Pipeline subprocess crashed during readiness check: {crash}")

            sentinel_id = f"sentinel-{uuid.uuid4().hex[:8]}"
            seen_ids.append(sentinel_id)
            sentinel = {
                "id": sentinel_id,
                "tenant_id": "PIPELINE_SENTINEL",
                "status": "new",
                "transact_time": "2026-01-01T00:00:00+00:00",
                "symbol": "SENTINEL",
                "side": "buy",
                "quantity": 1.0,
                "price": 1.0,
                "order_type": "limit",
                "currency": "USD",
                "event_type": "order",
                "message_id": uuid.uuid4().hex,
                "ingested_at": "2026-01-01T00:00:00+00:00",
            }
            self._kafka_producer.publish(TRADE_NORMALIZATION, sentinel)

            inner_deadline = time.monotonic() + 3.0
            while time.monotonic() < inner_deadline:
                rows = self._clickhouse_db.fetch_all("SELECT * FROM orders")
                if any(r.get("id") in seen_ids for r in rows):
                    return
                time.sleep(0.3)

        # Timeout -- gather diagnostic info
        crash = self._check_procs_alive()
        proc_info = crash or "all subprocesses still running"
        raise TimeoutError(
            f"Consumer readiness check failed: sentinel events did not appear "
            f"in ClickHouse within 45s. Subprocess status: {proc_info}"
        )

    def close(self) -> None:
        """Close connections and (if last worker) kill subprocesses."""
        self._kafka_producer.disconnect()
        self._clickhouse_db.disconnect()

        should_kill = False
        with open(self._lock_file, "w") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                count = int(self._counter_file.read_text()) if self._counter_file.exists() else 1
                count -= 1
                self._counter_file.write_text(str(count))
                if count <= 0:
                    should_kill = True
                    self._info_file.unlink(missing_ok=True)
                    self._counter_file.unlink(missing_ok=True)
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

        if should_kill:
            if self._procs:
                for proc in self._procs.values():
                    try:
                        proc.send_signal(signal.SIGTERM)
                    except OSError:
                        pass
                for proc in self._procs.values():
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
            else:
                # Non-owner worker: kill by stored PIDs
                try:
                    info = _json.loads((self._lock_dir / "pipeline.json").read_text())
                    for pid in info.get("pids", {}).values():
                        try:
                            os.kill(pid, signal.SIGTERM)
                        except OSError:
                            pass
                except Exception:
                    pass
