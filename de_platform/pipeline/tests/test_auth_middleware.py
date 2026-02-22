"""Tests for the shared JWT auth middleware and password helpers."""

from __future__ import annotations

import time

import jwt
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from de_platform.pipeline.auth_middleware import (
    create_auth_middleware,
    encode_token,
    hash_password,
    verify_password,
)

SECRET = "test-secret-key-that-is-at-least-32-bytes"


# ── Helper to build a minimal aiohttp app with the middleware ─────────────────


def _make_app(public_paths: set[str] | None = None) -> web.Application:
    middleware = create_auth_middleware(SECRET, public_paths=public_paths)
    app = web.Application(middlewares=[middleware])

    async def protected(request: web.Request) -> web.Response:
        return web.json_response({
            "user_id": request.get("user_id", ""),
            "tenant_id": request.get("tenant_id", ""),
            "role": request.get("role", ""),
        })

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    app.router.add_get("/protected", protected)
    app.router.add_get("/health", health)
    app.router.add_get("/health/startup", health)
    return app


# ── Middleware tests ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_valid_token_sets_request_fields() -> None:
    app = _make_app()
    token = encode_token("u1", "acme", "editor", SECRET)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert resp.status == 200
        data = await resp.json()
        assert data["user_id"] == "u1"
        assert data["tenant_id"] == "acme"
        assert data["role"] == "editor"


@pytest.mark.asyncio
async def test_missing_authorization_header_returns_401() -> None:
    app = _make_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/protected")
        assert resp.status == 401
        data = await resp.json()
        assert "Missing" in data["error"]


@pytest.mark.asyncio
async def test_invalid_token_returns_401() -> None:
    app = _make_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/protected", headers={"Authorization": "Bearer not-a-jwt"})
        assert resp.status == 401
        data = await resp.json()
        assert "Invalid" in data["error"]


@pytest.mark.asyncio
async def test_wrong_secret_returns_401() -> None:
    app = _make_app()
    token = encode_token("u1", "acme", "editor", "wrong-secret-key-that-is-long-enough")
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert resp.status == 401


@pytest.mark.asyncio
async def test_expired_token_returns_401() -> None:
    app = _make_app()
    token = encode_token("u1", "acme", "editor", SECRET, expires_in=-1)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert resp.status == 401
        data = await resp.json()
        assert "expired" in data["error"]


@pytest.mark.asyncio
async def test_public_paths_bypass_auth() -> None:
    app = _make_app()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/health")
        assert resp.status == 200

        resp = await client.get("/health/startup")
        assert resp.status == 200


@pytest.mark.asyncio
async def test_custom_public_paths() -> None:
    app = _make_app(public_paths={"/protected"})
    async with TestClient(TestServer(app)) as client:
        # /protected is now public — no auth needed
        resp = await client.get("/protected")
        assert resp.status == 200


@pytest.mark.asyncio
async def test_bearer_prefix_required() -> None:
    app = _make_app()
    token = encode_token("u1", "acme", "editor", SECRET)
    async with TestClient(TestServer(app)) as client:
        # No "Bearer " prefix
        resp = await client.get("/protected", headers={"Authorization": token})
        assert resp.status == 401


# ── encode_token / decode roundtrip ──────────────────────────────────────────


def test_encode_token_roundtrip() -> None:
    token = encode_token("u1", "acme", "admin", SECRET, expires_in=60)
    payload = jwt.decode(token, SECRET, algorithms=["HS256"])
    assert payload["sub"] == "u1"
    assert payload["tenant_id"] == "acme"
    assert payload["role"] == "admin"
    assert payload["exp"] > time.time()


def test_encode_token_custom_ttl() -> None:
    token = encode_token("u1", "acme", "viewer", SECRET, expires_in=10)
    payload = jwt.decode(token, SECRET, algorithms=["HS256"])
    assert payload["exp"] - payload["iat"] == 10


# ── Password helpers ─────────────────────────────────────────────────────────


def test_hash_and_verify_password() -> None:
    pw = "super-secret-123"
    hashed = hash_password(pw)
    assert hashed != pw
    assert verify_password(pw, hashed) is True


def test_verify_password_wrong_password() -> None:
    hashed = hash_password("correct-password")
    assert verify_password("wrong-password", hashed) is False


def test_hash_password_different_salts() -> None:
    pw = "same-password"
    h1 = hash_password(pw)
    h2 = hash_password(pw)
    # bcrypt should produce different hashes (different salts)
    assert h1 != h2
    # But both should verify
    assert verify_password(pw, h1) is True
    assert verify_password(pw, h2) is True
