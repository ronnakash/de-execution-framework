"""Tests for the Client Config API module.

Uses MemoryDatabase + MemoryCache and aiohttp TestClient/TestServer
for HTTP endpoint testing.
"""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.client_config.main import ClientConfigModule
from de_platform.pipeline.auth_middleware import encode_token
from de_platform.pipeline.client_config_cache import CHANNEL
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets

JWT_SECRET = "test-client-config-secret-at-least-32b"


# ── Helpers ──────────────────────────────────────────────────────────────────


async def _setup_module(
    db: MemoryDatabase | None = None,
    cache: MemoryCache | None = None,
) -> tuple[ClientConfigModule, MemoryDatabase, MemoryCache]:
    db = db or MemoryDatabase()
    cache = cache or MemoryCache()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})

    module = ClientConfigModule(
        config=config, logger=logger, db=db, cache=cache,
        lifecycle=lifecycle, metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, db, cache


# ── Client CRUD tests ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_client() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/clients", json={
            "tenant_id": "acme",
            "display_name": "Acme Corp",
            "mode": "realtime",
        })
        assert resp.status == 201
        data = await resp.json()
        assert data["tenant_id"] == "acme"
        assert data["mode"] == "realtime"

    # Verify DB
    rows = db.fetch_all("SELECT * FROM clients")
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == "acme"


@pytest.mark.asyncio
async def test_create_client_defaults_to_batch() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/clients", json={
            "tenant_id": "t1",
            "display_name": "Test",
        })
        assert resp.status == 201
        data = await resp.json()
        assert data["mode"] == "batch"


@pytest.mark.asyncio
async def test_create_client_missing_fields() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/clients", json={"tenant_id": "t1"})
        assert resp.status == 400


@pytest.mark.asyncio
async def test_create_client_duplicate() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/clients", json={
            "tenant_id": "t1",
            "display_name": "T1 Again",
        })
        assert resp.status == 409


@pytest.mark.asyncio
async def test_list_clients() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    db.insert_one("clients", {"tenant_id": "t2", "display_name": "T2", "mode": "realtime"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2


@pytest.mark.asyncio
async def test_get_client() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients/t1")
        assert resp.status == 200
        data = await resp.json()
        assert data["tenant_id"] == "t1"
        assert data["display_name"] == "T1"


@pytest.mark.asyncio
async def test_get_client_not_found() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients/nonexistent")
        assert resp.status == 404


@pytest.mark.asyncio
async def test_update_client() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/clients/t1", json={"mode": "realtime"})
        assert resp.status == 200
        data = await resp.json()
        assert data["mode"] == "realtime"
        assert data["display_name"] == "T1"  # unchanged


@pytest.mark.asyncio
async def test_update_client_not_found() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/clients/nonexistent", json={"mode": "realtime"})
        assert resp.status == 404


@pytest.mark.asyncio
async def test_delete_client() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.delete("/api/v1/clients/t1")
        assert resp.status == 200

    rows = db.fetch_all("SELECT * FROM clients")
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_delete_client_not_found() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.delete("/api/v1/clients/nonexistent")
        assert resp.status == 404


# ── Algo config tests ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_algos_empty() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients/t1/algos")
        assert resp.status == 200
        data = await resp.json()
        assert data == []


@pytest.mark.asyncio
async def test_update_algo_config() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/clients/t1/algos/large_notional", json={
            "enabled": True,
            "thresholds": {"threshold_usd": 500_000},
        })
        assert resp.status == 200
        data = await resp.json()
        assert data["algorithm"] == "large_notional"
        assert data["enabled"] is True
        assert data["thresholds"]["threshold_usd"] == 500_000


@pytest.mark.asyncio
async def test_update_algo_client_not_found() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/clients/nonexistent/algos/large_notional", json={
            "enabled": True,
        })
        assert resp.status == 404


@pytest.mark.asyncio
async def test_get_algos_after_update() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        await client.put("/api/v1/clients/t1/algos/large_notional", json={
            "enabled": True,
            "thresholds": {"threshold_usd": 500_000},
        })
        await client.put("/api/v1/clients/t1/algos/velocity", json={
            "enabled": False,
            "thresholds": {"max_events": 50},
        })

        resp = await client.get("/api/v1/clients/t1/algos")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2
        algos = {r["algorithm"]: r for r in data}
        assert algos["large_notional"]["enabled"] is True
        assert algos["velocity"]["enabled"] is False


# ── Cache sync tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_client_writes_to_cache() -> None:
    module, db, cache = await _setup_module()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        await client.post("/api/v1/clients", json={
            "tenant_id": "t1",
            "display_name": "T1",
            "mode": "realtime",
        })

    cached = cache.get("client_config:t1")
    assert cached is not None
    assert cached["mode"] == "realtime"


@pytest.mark.asyncio
async def test_update_client_writes_to_cache() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        await client.put("/api/v1/clients/t1", json={"mode": "realtime"})

    cached = cache.get("client_config:t1")
    assert cached["mode"] == "realtime"


@pytest.mark.asyncio
async def test_update_algo_writes_to_cache() -> None:
    module, db, cache = await _setup_module()
    db.insert_one("clients", {"tenant_id": "t1", "display_name": "T1", "mode": "batch"})
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        await client.put("/api/v1/clients/t1/algos/large_notional", json={
            "enabled": True,
            "thresholds": {"threshold_usd": 500_000},
        })

    cached = cache.get("algo_config:t1:large_notional")
    assert cached is not None
    assert cached["enabled"] is True
    assert cached["thresholds"]["threshold_usd"] == 500_000


@pytest.mark.asyncio
async def test_mutations_publish_to_channel() -> None:
    module, db, cache = await _setup_module()
    published: list[str] = []
    cache.subscribe_channel(CHANNEL, published.append)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        await client.post("/api/v1/clients", json={
            "tenant_id": "t1",
            "display_name": "T1",
        })
        await client.put("/api/v1/clients/t1", json={"mode": "realtime"})
        await client.delete("/api/v1/clients/t1")

    # create + update + delete = 3 publishes
    assert len(published) == 3
    assert all(msg == "t1" for msg in published)


# ── Auth-enabled tests ───────────────────────────────────────────────────────


async def _setup_module_with_auth(
    db: MemoryDatabase | None = None,
    cache: MemoryCache | None = None,
) -> tuple[ClientConfigModule, MemoryDatabase, MemoryCache]:
    db = db or MemoryDatabase()
    cache = cache or MemoryCache()
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})
    secrets = EnvSecrets(overrides={"JWT_SECRET": JWT_SECRET})

    module = ClientConfigModule(
        config=config, logger=logger, db=db, cache=cache,
        lifecycle=lifecycle, metrics=NoopMetrics(), secrets=secrets,
    )
    await module.initialize()
    return module, db, cache


def _auth_header(user_id: str, tenant_id: str, role: str) -> dict[str, str]:
    token = encode_token(user_id, tenant_id, role, JWT_SECRET)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_auth_required_when_jwt_secret_set() -> None:
    module, db, cache = await _setup_module_with_auth()
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients")
        assert resp.status == 401


@pytest.mark.asyncio
async def test_auth_admin_can_create_client() -> None:
    module, db, cache = await _setup_module_with_auth()
    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/clients", json={
            "tenant_id": "acme",
            "display_name": "Acme Corp",
        }, headers=headers)
        assert resp.status == 201


@pytest.mark.asyncio
async def test_auth_viewer_cannot_create_client() -> None:
    module, db, cache = await _setup_module_with_auth()
    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/clients", json={
            "tenant_id": "acme",
            "display_name": "Acme Corp",
        }, headers=headers)
        assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_viewer_cannot_update_client() -> None:
    module, db, cache = await _setup_module_with_auth()
    db.insert_one("clients", {"tenant_id": "acme", "display_name": "Acme", "mode": "batch"})
    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/clients/acme", json={
            "mode": "realtime",
        }, headers=headers)
        assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_viewer_cannot_delete_client() -> None:
    module, db, cache = await _setup_module_with_auth()
    db.insert_one("clients", {"tenant_id": "acme", "display_name": "Acme", "mode": "batch"})
    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")

    async with TestClient(TestServer(app)) as client:
        resp = await client.delete("/api/v1/clients/acme", headers=headers)
        assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_tenant_isolation_get_client() -> None:
    """Non-admin cannot access another tenant's config."""
    module, db, cache = await _setup_module_with_auth()
    db.insert_one("clients", {"tenant_id": "other", "display_name": "Other", "mode": "batch"})
    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients/other", headers=headers)
        assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_admin_cross_tenant_access() -> None:
    """Admin can access another tenant's config."""
    module, db, cache = await _setup_module_with_auth()
    db.insert_one("clients", {"tenant_id": "other", "display_name": "Other", "mode": "batch"})
    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients/other", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert data["tenant_id"] == "other"


@pytest.mark.asyncio
async def test_auth_viewer_can_read_own_tenant() -> None:
    """Viewer can read their own tenant's config."""
    module, db, cache = await _setup_module_with_auth()
    db.insert_one("clients", {"tenant_id": "acme", "display_name": "Acme", "mode": "batch"})
    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/clients/acme", headers=headers)
        assert resp.status == 200


@pytest.mark.asyncio
async def test_auth_viewer_cannot_update_algo() -> None:
    module, db, cache = await _setup_module_with_auth()
    db.insert_one("clients", {"tenant_id": "acme", "display_name": "Acme", "mode": "batch"})
    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")

    async with TestClient(TestServer(app)) as client:
        resp = await client.put("/api/v1/clients/acme/algos/large_notional", json={
            "enabled": True,
        }, headers=headers)
        assert resp.status == 403
