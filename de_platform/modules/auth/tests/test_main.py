"""Tests for the Auth module.

Uses MemoryDatabase and aiohttp TestClient/TestServer for HTTP endpoint testing.
JWT_SECRET is always set for these tests since the auth module requires it.
"""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.auth.main import AuthModule
from de_platform.pipeline.auth_middleware import encode_token, hash_password
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets

JWT_SECRET = "test-secret-for-auth-module-at-least-32b"


# ── Helpers ──────────────────────────────────────────────────────────────────


async def _setup_module(
    db: MemoryDatabase | None = None,
) -> tuple[AuthModule, MemoryDatabase]:
    db = db or MemoryDatabase()
    secrets = EnvSecrets(overrides={"JWT_SECRET": JWT_SECRET})
    lifecycle = LifecycleManager()
    logger = LoggerFactory(default_impl="memory")
    config = ModuleConfig({"port": 0})

    module = AuthModule(
        config=config, logger=logger, db=db, secrets=secrets,
        lifecycle=lifecycle, metrics=NoopMetrics(),
    )
    await module.initialize()
    return module, db


def _seed_tenant_and_user(
    db: MemoryDatabase,
    tenant_id: str = "acme",
    user_id: str = "u1",
    email: str = "admin@acme.com",
    password: str = "secret123",
    role: str = "admin",
) -> dict:
    """Insert a tenant and user into the DB, return user dict."""
    db.insert_one("tenants", {"tenant_id": tenant_id, "name": "Acme Corp"})
    pw_hash = hash_password(password)
    user = {
        "user_id": user_id,
        "tenant_id": tenant_id,
        "email": email,
        "password_hash": pw_hash,
        "role": role,
    }
    db.insert_one("users", user)
    return user


def _auth_header(user_id: str, tenant_id: str, role: str) -> dict[str, str]:
    token = encode_token(user_id, tenant_id, role, JWT_SECRET)
    return {"Authorization": f"Bearer {token}"}


# ── Login tests ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_login_success() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db, password="secret123")

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={
            "email": "admin@acme.com",
            "password": "secret123",
        })
        assert resp.status == 200
        data = await resp.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_login_wrong_password() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db, password="secret123")

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={
            "email": "admin@acme.com",
            "password": "wrong-password",
        })
        assert resp.status == 401


@pytest.mark.asyncio
async def test_login_unknown_email() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db)

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={
            "email": "nobody@acme.com",
            "password": "secret123",
        })
        assert resp.status == 401


@pytest.mark.asyncio
async def test_login_missing_fields() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={"email": "a@b.com"})
        assert resp.status == 400


# ── Refresh tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_success() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db, password="secret123")

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        # Login first
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": "admin@acme.com",
            "password": "secret123",
        })
        tokens = await login_resp.json()

        # Refresh
        resp = await client.post("/api/v1/auth/refresh", json={
            "refresh_token": tokens["refresh_token"],
        })
        assert resp.status == 200
        data = await resp.json()
        assert "access_token" in data
        assert "refresh_token" in data
        # New refresh token should be different (rotation)
        assert data["refresh_token"] != tokens["refresh_token"]


@pytest.mark.asyncio
async def test_refresh_invalid_token() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/refresh", json={
            "refresh_token": "nonexistent-token",
        })
        assert resp.status == 401


@pytest.mark.asyncio
async def test_refresh_missing_token() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/refresh", json={})
        assert resp.status == 400


@pytest.mark.asyncio
async def test_refresh_used_token_is_invalidated() -> None:
    """After refresh, the old refresh token should no longer work."""
    module, db = await _setup_module()
    _seed_tenant_and_user(db, password="secret123")

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": "admin@acme.com",
            "password": "secret123",
        })
        tokens = await login_resp.json()
        old_refresh = tokens["refresh_token"]

        # Use the refresh token
        await client.post("/api/v1/auth/refresh", json={"refresh_token": old_refresh})

        # Try to use the old token again
        resp = await client.post("/api/v1/auth/refresh", json={"refresh_token": old_refresh})
        assert resp.status == 401


# ── Logout tests ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_logout_revokes_refresh_token() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db, password="secret123")

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        # Login
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": "admin@acme.com",
            "password": "secret123",
        })
        tokens = await login_resp.json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}

        # Logout
        resp = await client.post(
            "/api/v1/auth/logout",
            json={"refresh_token": tokens["refresh_token"]},
            headers=headers,
        )
        assert resp.status == 200

        # Refresh should fail now
        resp = await client.post("/api/v1/auth/refresh", json={
            "refresh_token": tokens["refresh_token"],
        })
        assert resp.status == 401


# ── /me tests ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_me_returns_user_info() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/auth/me", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert data["user_id"] == "u1"
        assert data["tenant_id"] == "acme"
        assert data["role"] == "admin"


@pytest.mark.asyncio
async def test_me_without_token_returns_401() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/auth/me")
        assert resp.status == 401


# ── User CRUD tests ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_user_as_admin() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db)

    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/users", json={
            "email": "viewer@acme.com",
            "password": "viewerpass",
            "role": "viewer",
        }, headers=headers)
        assert resp.status == 201
        data = await resp.json()
        assert data["email"] == "viewer@acme.com"
        assert data["role"] == "viewer"
        assert data["tenant_id"] == "acme"


@pytest.mark.asyncio
async def test_create_user_as_non_admin_returns_403() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/users", json={
            "email": "hacker@acme.com",
            "password": "hack",
        }, headers=headers)
        assert resp.status == 403


@pytest.mark.asyncio
async def test_create_user_duplicate_email() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db, email="taken@acme.com")

    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/users", json={
            "email": "taken@acme.com",
            "password": "pass",
        }, headers=headers)
        assert resp.status == 409


@pytest.mark.asyncio
async def test_list_users_as_admin() -> None:
    module, db = await _setup_module()
    _seed_tenant_and_user(db, user_id="u1", email="a@acme.com")
    # Add second user in same tenant
    db.insert_one("users", {
        "user_id": "u2",
        "tenant_id": "acme",
        "email": "b@acme.com",
        "password_hash": "x",
        "role": "viewer",
    })
    # Add user in different tenant (should not be returned)
    db.insert_one("tenants", {"tenant_id": "other", "name": "Other Corp"})
    db.insert_one("users", {
        "user_id": "u3",
        "tenant_id": "other",
        "email": "c@other.com",
        "password_hash": "x",
        "role": "admin",
    })

    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/auth/users", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2
        emails = {u["email"] for u in data}
        assert emails == {"a@acme.com", "b@acme.com"}


@pytest.mark.asyncio
async def test_list_users_as_non_admin_returns_403() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    headers = _auth_header("u1", "acme", "viewer")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/auth/users", headers=headers)
        assert resp.status == 403


@pytest.mark.asyncio
async def test_create_user_missing_fields() -> None:
    module, db = await _setup_module()

    app = module._create_app()
    headers = _auth_header("u1", "acme", "admin")
    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/users", json={
            "email": "a@b.com",
        }, headers=headers)
        assert resp.status == 400
