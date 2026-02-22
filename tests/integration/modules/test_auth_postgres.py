"""Integration tests: AuthModule against real PostgreSQL.

Verifies that the auth module works correctly with asyncpg and real Postgres,
catching type mismatches (e.g. datetime vs string) that MemoryDatabase silently
accepts but asyncpg rejects.
"""

from __future__ import annotations

import uuid

import pytest
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.auth.main import AuthModule
from de_platform.pipeline.auth_middleware import encode_token, hash_password
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.metrics.noop_metrics import NoopMetrics
from de_platform.services.secrets.env_secrets import EnvSecrets

pytestmark = pytest.mark.integration

JWT_SECRET = "integration-test-secret-minimum-32-bytes!!"


def _unique_email() -> str:
    return f"test-{uuid.uuid4().hex[:8]}@integ.test"


async def _make_module(auth_db) -> AuthModule:
    """Create and initialize an AuthModule backed by real Postgres."""
    module = AuthModule(
        config=ModuleConfig({"port": 0}),
        logger=LoggerFactory(default_impl="memory"),
        db=auth_db,
        secrets=EnvSecrets(overrides={"JWT_SECRET": JWT_SECRET}),
        lifecycle=LifecycleManager(),
        metrics=NoopMetrics(),
    )
    # Skip initialize() — it calls db.connect_async() which is already done
    # by the fixture.  Just set the fields that initialize() sets.
    module.log = module.logger.create()
    module.port = 0
    module.access_token_ttl = 1800
    module.refresh_token_ttl = 604800
    module.jwt_secret = JWT_SECRET
    return module


async def _seed_user(auth_db, email: str, password: str = "testpass123") -> dict:
    """Insert a tenant + user directly into Postgres, return user dict."""
    tenant_id = f"integ-{uuid.uuid4().hex[:8]}"
    user_id = uuid.uuid4().hex

    # Ensure tenant exists (ON CONFLICT not supported by insert_one_async,
    # so use a unique tenant_id per test)
    await auth_db.insert_one_async("tenants", {
        "tenant_id": tenant_id,
        "name": f"Integration Test {tenant_id}",
    })
    user = {
        "user_id": user_id,
        "tenant_id": tenant_id,
        "email": email,
        "password_hash": hash_password(password),
        "role": "admin",
    }
    await auth_db.insert_one_async("users", user)
    return user


# ── Login ─────────────────────────────────────────────────────────────────────


async def test_login_creates_refresh_token_in_postgres(auth_db):
    """Login inserts a refresh_token row with a proper datetime into Postgres.

    This is the exact bug that MemoryDatabase missed: expires_at was stored
    as an ISO string instead of a datetime object, causing asyncpg to reject it.
    """
    email = _unique_email()
    await _seed_user(auth_db, email=email, password="secret123")

    module = await _make_module(auth_db)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={
            "email": email,
            "password": "secret123",
        })
        assert resp.status == 200, f"Login failed: {await resp.text()}"
        data = await resp.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"


async def test_login_wrong_password_returns_401(auth_db):
    """Wrong password returns 401 against real Postgres."""
    email = _unique_email()
    await _seed_user(auth_db, email=email, password="correct")

    module = await _make_module(auth_db)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={
            "email": email,
            "password": "wrong",
        })
        assert resp.status == 401


async def test_login_unknown_email_returns_401(auth_db):
    """Unknown email returns 401."""
    module = await _make_module(auth_db)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        resp = await client.post("/api/v1/auth/login", json={
            "email": "nobody@integ.test",
            "password": "anything",
        })
        assert resp.status == 401


# ── Refresh ───────────────────────────────────────────────────────────────────


async def test_refresh_token_roundtrip_with_postgres(auth_db):
    """Full login → refresh → new tokens cycle against real Postgres.

    Tests that refresh_tokens rows are correctly inserted (datetime types)
    and that token rotation (delete old + insert new) works with asyncpg.
    """
    email = _unique_email()
    await _seed_user(auth_db, email=email, password="secret123")

    module = await _make_module(auth_db)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        # Login
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": email,
            "password": "secret123",
        })
        assert login_resp.status == 200
        tokens = await login_resp.json()

        # Refresh
        refresh_resp = await client.post("/api/v1/auth/refresh", json={
            "refresh_token": tokens["refresh_token"],
        })
        assert refresh_resp.status == 200, f"Refresh failed: {await refresh_resp.text()}"
        new_tokens = await refresh_resp.json()
        assert "access_token" in new_tokens
        assert "refresh_token" in new_tokens
        # Token rotation: new refresh token must differ
        assert new_tokens["refresh_token"] != tokens["refresh_token"]


async def test_refresh_old_token_rejected_after_rotation(auth_db):
    """After refresh, the old token is deleted from Postgres and can't be reused."""
    email = _unique_email()
    await _seed_user(auth_db, email=email, password="secret123")

    module = await _make_module(auth_db)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": email,
            "password": "secret123",
        })
        tokens = await login_resp.json()
        old_refresh = tokens["refresh_token"]

        # Use it once
        await client.post("/api/v1/auth/refresh", json={"refresh_token": old_refresh})

        # Try again — should fail
        resp = await client.post("/api/v1/auth/refresh", json={"refresh_token": old_refresh})
        assert resp.status == 401


# ── Logout ────────────────────────────────────────────────────────────────────


async def test_logout_revokes_refresh_token_in_postgres(auth_db):
    """Logout deletes the refresh token from Postgres so it can't be reused."""
    email = _unique_email()
    await _seed_user(auth_db, email=email, password="secret123")

    module = await _make_module(auth_db)
    app = module._create_app()

    async with TestClient(TestServer(app)) as client:
        # Login
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": email,
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

        # Refresh should now fail
        resp = await client.post("/api/v1/auth/refresh", json={
            "refresh_token": tokens["refresh_token"],
        })
        assert resp.status == 401


# ── User management ──────────────────────────────────────────────────────────


async def test_create_user_and_login_with_postgres(auth_db):
    """Admin creates a user via API, then that user can log in.

    End-to-end test of user creation + login against real Postgres, verifying
    that password hashing and all DB inserts use correct types.
    """
    email = _unique_email()
    admin = await _seed_user(auth_db, email=email, password="admin123")

    module = await _make_module(auth_db)
    app = module._create_app()

    admin_headers = {
        "Authorization": (
            f"Bearer {encode_token(admin['user_id'], admin['tenant_id'], 'admin', JWT_SECRET)}"
        ),
    }
    new_email = _unique_email()

    async with TestClient(TestServer(app)) as client:
        # Create user
        resp = await client.post("/api/v1/auth/users", json={
            "email": new_email,
            "password": "newuserpass",
            "role": "viewer",
        }, headers=admin_headers)
        assert resp.status == 201, f"Create user failed: {await resp.text()}"
        data = await resp.json()
        assert data["email"] == new_email
        assert data["role"] == "viewer"
        assert data["tenant_id"] == admin["tenant_id"]

        # Login as the new user
        login_resp = await client.post("/api/v1/auth/login", json={
            "email": new_email,
            "password": "newuserpass",
        })
        assert login_resp.status == 200
        tokens = await login_resp.json()
        assert "access_token" in tokens


async def test_list_users_returns_only_same_tenant(auth_db):
    """List users only returns users from the requesting user's tenant."""
    email1 = _unique_email()
    admin = await _seed_user(auth_db, email=email1, password="admin123")

    module = await _make_module(auth_db)
    app = module._create_app()

    admin_headers = {
        "Authorization": (
            f"Bearer {encode_token(admin['user_id'], admin['tenant_id'], 'admin', JWT_SECRET)}"
        ),
    }

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/v1/auth/users", headers=admin_headers)
        assert resp.status == 200
        data = await resp.json()
        # All returned users belong to the same tenant
        for user in data:
            assert user["tenant_id"] == admin["tenant_id"]
        # At least the admin user is in the list
        emails = {u["email"] for u in data}
        assert email1 in emails
