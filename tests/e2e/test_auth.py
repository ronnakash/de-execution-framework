"""E2E tests: authentication service.

Tests login, token refresh, logout, user info, and user management
against the auth module running with JWT middleware enabled.

7 tests.

Requires: ``pytest -m e2e`` or ``make test-e2e``.
"""

from __future__ import annotations

import uuid

import pytest

from tests.helpers.harness import RealInfraHarness

pytestmark = [pytest.mark.e2e]


@pytest.fixture
async def harness(shared_pipeline):
    async with RealInfraHarness(shared_pipeline) as h:
        yield h


async def _login(harness: RealInfraHarness) -> tuple[str, str]:
    """Log in with the pre-seeded e2e_tenant admin user."""
    status, body = await harness.call_service(
        "auth", "POST", "/api/v1/auth/login",
        json={"email": "admin@e2e.test", "password": "e2e_password"},
    )
    assert status == 200, f"Login failed: {body}"
    return body["access_token"], body["refresh_token"]


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_login_returns_tokens(harness):
    status, body = await harness.call_service(
        "auth", "POST", "/api/v1/auth/login",
        json={"email": "admin@e2e.test", "password": "e2e_password"},
    )
    assert status == 200
    assert "access_token" in body
    assert "refresh_token" in body
    assert body["token_type"] == "bearer"


async def test_login_wrong_password_returns_401(harness):
    status, _body = await harness.call_service(
        "auth", "POST", "/api/v1/auth/login",
        json={"email": "admin@e2e.test", "password": "wrong"},
    )
    assert status == 401


async def test_me_returns_user_info(harness):
    access_token, _ = await _login(harness)
    status, body = await harness.call_service(
        "auth", "GET", "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert status == 200
    assert body["user_id"] == "e2e_admin"
    assert body["tenant_id"] == "e2e_tenant"
    assert body["role"] == "admin"


async def test_me_without_token_returns_401(harness):
    status, _body = await harness.call_service(
        "auth", "GET", "/api/v1/auth/me",
    )
    assert status == 401


async def test_refresh_token_rotation(harness):
    access_token, refresh_token = await _login(harness)

    # Refresh tokens
    status, body = await harness.call_service(
        "auth", "POST", "/api/v1/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert status == 200
    new_access = body["access_token"]
    new_refresh = body["refresh_token"]
    assert new_access != access_token
    assert new_refresh != refresh_token

    # Old refresh token should be revoked
    status2, _ = await harness.call_service(
        "auth", "POST", "/api/v1/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert status2 == 401


async def test_logout_revokes_refresh_token(harness):
    access_token, refresh_token = await _login(harness)

    status, body = await harness.call_service(
        "auth", "POST", "/api/v1/auth/logout",
        json={"refresh_token": refresh_token},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert status == 200
    assert body["status"] == "logged_out"

    # Old refresh token should be revoked
    status2, _ = await harness.call_service(
        "auth", "POST", "/api/v1/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert status2 == 401


async def test_create_user_and_login(harness):
    access_token, _ = await _login(harness)

    unique_email = f"user_{uuid.uuid4().hex[:8]}@e2e.test"
    status, body = await harness.call_service(
        "auth", "POST", "/api/v1/auth/users",
        json={"email": unique_email, "password": "new_password", "role": "analyst"},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert status == 201
    assert body["email"] == unique_email
    assert body["role"] == "analyst"

    # New user can log in
    status2, body2 = await harness.call_service(
        "auth", "POST", "/api/v1/auth/login",
        json={"email": unique_email, "password": "new_password"},
    )
    assert status2 == 200
    assert "access_token" in body2
