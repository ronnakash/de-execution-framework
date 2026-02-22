"""Conftest for integration tests — reuses e2e infra fixtures.

All integration tests are marked with ``integration`` and require
docker-compose services to be running (``make infra-up``).
"""

# Re-export all fixtures from the e2e conftest so they're available here.
from tests.e2e.conftest import (  # noqa: F401
    _init_schemas,
    alerts_db,
    clickhouse_db,
    infra,
    kafka_mq,
    minio_fs,
    redis_cache,
    secrets,
    shared_pipeline,
    test_group_id,
    warehouse_db,
)
