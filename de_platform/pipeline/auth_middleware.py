"""Shared JWT authentication middleware and password helpers.

Provides:
- ``create_auth_middleware()`` — aiohttp middleware factory for JWT verification
- ``encode_token()`` — create a signed JWT access token
- ``hash_password()`` / ``verify_password()`` — bcrypt password hashing
"""

from __future__ import annotations

import json
import time

import bcrypt
import jwt
from aiohttp import web

_DEFAULT_PUBLIC_PATHS = {"/health", "/health/startup"}


def create_auth_middleware(
    jwt_secret: str,
    public_paths: set[str] | None = None,
) -> web.middleware:
    """Create an aiohttp middleware that verifies JWT Bearer tokens.

    Requests to *public_paths* bypass authentication.  All other requests
    must include an ``Authorization: Bearer <token>`` header with a valid
    HS256-signed JWT.  On success the middleware sets ``request["user_id"]``,
    ``request["tenant_id"]``, and ``request["role"]`` from the token payload.
    """
    allowed = _DEFAULT_PUBLIC_PATHS | (public_paths or set())

    @web.middleware
    async def auth_middleware(request: web.Request, handler):
        if request.path in allowed:
            return await handler(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "Missing or invalid Authorization header"}),
            )

        token = auth_header[7:]
        try:
            payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        except jwt.ExpiredSignatureError:
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "Token has expired"}),
            )
        except jwt.InvalidTokenError:
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "Invalid token"}),
            )

        request["user_id"] = payload.get("sub", "")
        request["tenant_id"] = payload.get("tenant_id", "")
        request["role"] = payload.get("role", "viewer")

        return await handler(request)

    return auth_middleware


def encode_token(
    user_id: str,
    tenant_id: str,
    role: str,
    secret: str,
    expires_in: int = 1800,
) -> str:
    """Create a signed JWT access token.

    Parameters
    ----------
    user_id:
        Subject claim (``sub``).
    tenant_id:
        Tenant the user belongs to.
    role:
        User role (``admin``, ``editor``, ``viewer``).
    secret:
        HS256 signing key.
    expires_in:
        Token lifetime in seconds (default 30 min).
    """
    now = int(time.time())
    payload = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "role": role,
        "iat": now,
        "exp": now + expires_in,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def hash_password(password: str) -> str:
    """Hash a plaintext password with bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    return bcrypt.checkpw(password.encode(), password_hash.encode())
