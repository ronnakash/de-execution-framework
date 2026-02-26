"""ExternalPipeline -- connects to services already running in a K8s cluster.

Unlike SharedPipeline (which starts module subprocesses), ExternalPipeline
assumes all services are already deployed and port-forwarded by dev-k8s.sh.
It only connects to ClickHouse for data assertions and waits for service
health endpoints.

Kafka and MinIO are not directly accessible from outside the cluster,
so K8s E2E tests use REST-only ingestion.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
import uuid
from typing import Any


class ExternalPipeline:
    """Pipeline backed by an external K8s cluster (services already running).

    Port-forwarded by dev-k8s.sh:
    - Application services on 8001-8007
    - Infrastructure on standard ports (5432, 8123, 6379, 9000)
    """

    # Fixed ports (port-forwarded by dev-k8s.sh)
    rest_port = 8001
    api_port = 8002
    config_port = 8003
    auth_port = 8004
    data_audit_port = 8005
    task_scheduler_port = 8006
    alert_manager_port = 8007

    JWT_SECRET = "dev-k8s-jwt-secret-minimum-32-bytes!!"

    def __init__(self, infra: Any) -> None:
        self._infra = infra
        self._clickhouse_db: Any = None
        self._kafka_producer = None   # Not available from outside K8s
        self._minio_fs = None         # Not needed (REST-only)
        self._screenshot_browser = None

    def start(self) -> None:
        """Connect to ClickHouse and wait for all service health endpoints."""
        self._connect_clickhouse()
        self._wait_for_services()

    def _connect_clickhouse(self) -> None:
        from de_platform.services.database.clickhouse_database import ClickHouseDatabase
        from de_platform.services.secrets.env_secrets import EnvSecrets

        secrets = EnvSecrets(
            overrides=self._infra.to_env_overrides(
                group_id=f"k8s-assert-{uuid.uuid4().hex[:8]}"
            )
        )
        ch_db = ClickHouseDatabase(secrets=secrets)
        ch_db.connect()
        self._clickhouse_db = ch_db

    def _wait_for_services(self) -> None:
        """Wait for all application services to accept HTTP connections.

        K8s services don't have the /health/startup sidecar used by
        SharedPipeline.  Instead, we check /api/v1/health.  Any HTTP
        response (even 401 from auth middleware) proves the service is up.
        Only connection errors mean it's truly unreachable.
        """
        services = {
            "rest-starter": self.rest_port,
            "data-api": self.api_port,
            "client-config": self.config_port,
            "auth": self.auth_port,
            "data-audit": self.data_audit_port,
            "task-scheduler": self.task_scheduler_port,
            "alert-manager": self.alert_manager_port,
        }
        for name, port in services.items():
            self._wait_for_http_any(name, port, timeout=30.0)

    @staticmethod
    def _wait_for_http_any(name: str, port: int, timeout: float = 30.0) -> None:
        """Poll until the port returns any HTTP response (including 4xx)."""
        url = f"http://127.0.0.1:{port}/api/v1/health"
        deadline = time.monotonic() + timeout
        last_error = "no connection attempted"
        while True:
            try:
                urllib.request.urlopen(url, timeout=5)
                return  # Any successful response means service is up
            except urllib.error.HTTPError:
                return  # 401/404 etc. still means the service is running
            except (urllib.error.URLError, OSError) as e:
                last_error = str(e)
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"K8s service '{name}' not ready at localhost:{port} within "
                    f"{timeout}s (last: {last_error}). "
                    f"Is 'make dev-k8s' running with port-forwards active?"
                )
            time.sleep(0.5)

    def close(self) -> None:
        """Disconnect from ClickHouse."""
        if self._clickhouse_db is not None:
            self._clickhouse_db.disconnect()
            self._clickhouse_db = None
