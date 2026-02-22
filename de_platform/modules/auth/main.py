"""Auth Service.

JWT authentication service with login, refresh, logout, and user management.

Endpoints::

    POST /api/v1/auth/login             — email + password -> access_token + refresh_token
    POST /api/v1/auth/refresh            — refresh token -> new access_token + refresh_token
    POST /api/v1/auth/logout             — revoke refresh token
    GET  /api/v1/auth/me                 — current user info from JWT
    POST /api/v1/auth/users              — create user (admin only)
    GET  /api/v1/auth/users              — list users in tenant (admin only)
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.auth_middleware import (
    create_auth_middleware,
    encode_token,
    hash_password,
    verify_password,
)
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.metrics.interface import MetricsInterface
from de_platform.services.secrets.interface import SecretsInterface

_PUBLIC_PATHS = {
    "/api/v1/auth/login",
    "/api/v1/auth/refresh",
    "/health",
    "/health/startup",
}


class AuthModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        secrets: SecretsInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.secrets = secrets
        self.lifecycle = lifecycle
        self.metrics = metrics
        self._runner: web.AppRunner | None = None
        self.jwt_secret: str = ""

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8004)
        self.access_token_ttl: int = int(self.config.get("access-token-ttl", 1800))
        self.refresh_token_ttl: int = int(self.config.get("refresh-token-ttl", 604800))
        jwt_secret = self.secrets.get("JWT_SECRET")
        if not jwt_secret:
            raise RuntimeError("JWT_SECRET must be set for the auth module")
        self.jwt_secret = jwt_secret
        await self.db.connect_async()
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self.db.disconnect_async)
        self.log.info("Auth service initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Auth service listening", module="auth", port=self.port)

        while not self.lifecycle.is_shutting_down:
            await asyncio.sleep(0.1)

        return 0

    def _create_app(self) -> web.Application:
        middlewares = [
            create_auth_middleware(self.jwt_secret, public_paths=_PUBLIC_PATHS),
        ]
        app = web.Application(middlewares=middlewares)
        app.router.add_post("/api/v1/auth/login", self._login)
        app.router.add_post("/api/v1/auth/refresh", self._refresh)
        app.router.add_post("/api/v1/auth/logout", self._logout)
        app.router.add_get("/api/v1/auth/me", self._me)
        app.router.add_post("/api/v1/auth/users", self._create_user)
        app.router.add_get("/api/v1/auth/users", self._list_users)
        return app

    # ── Auth endpoints ─────────────────────────────────────────────────────

    async def _login(self, request: web.Request) -> web.Response:
        self.metrics.counter(
            "http_requests_total",
            tags={"service": "auth", "endpoint": "/api/v1/auth/login", "method": "POST"},
        )
        body = await request.json()
        email = body.get("email", "")
        password = body.get("password", "")

        if not email or not password:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "email and password are required"}),
            )

        user = await self._find_user_by_email(email)
        if user is None or not verify_password(password, user["password_hash"]):
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "Invalid email or password"}),
            )

        access_token = encode_token(
            user_id=user["user_id"],
            tenant_id=user["tenant_id"],
            role=user["role"],
            secret=self.jwt_secret,
            expires_in=self.access_token_ttl,
        )
        refresh_token = await self._create_refresh_token(user["user_id"])

        self.log.info("User logged in", user_id=user["user_id"], tenant_id=user["tenant_id"])
        return web.json_response({
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
        })

    async def _refresh(self, request: web.Request) -> web.Response:
        self.metrics.counter(
            "http_requests_total",
            tags={"service": "auth", "endpoint": "/api/v1/auth/refresh", "method": "POST"},
        )
        body = await request.json()
        refresh_token = body.get("refresh_token", "")

        if not refresh_token:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "refresh_token is required"}),
            )

        # Look up the refresh token
        token_row = await self._find_refresh_token(refresh_token)
        if token_row is None:
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "Invalid refresh token"}),
            )

        # Check expiration
        expires_at = token_row["expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        if expires_at < datetime.now(timezone.utc).replace(tzinfo=None):
            # Delete the expired token
            await self.db.execute_async(
                "DELETE FROM refresh_tokens WHERE token_id = $1", [refresh_token]
            )
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "Refresh token has expired"}),
            )

        # Delete old token (rotation)
        await self.db.execute_async(
            "DELETE FROM refresh_tokens WHERE token_id = $1", [refresh_token]
        )

        # Look up user
        user = await self._find_user_by_id(token_row["user_id"])
        if user is None:
            return web.Response(
                status=401,
                content_type="application/json",
                text=json.dumps({"error": "User not found"}),
            )

        access_token = encode_token(
            user_id=user["user_id"],
            tenant_id=user["tenant_id"],
            role=user["role"],
            secret=self.jwt_secret,
            expires_in=self.access_token_ttl,
        )
        new_refresh_token = await self._create_refresh_token(user["user_id"])

        return web.json_response({
            "access_token": access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
        })

    async def _logout(self, request: web.Request) -> web.Response:
        self.metrics.counter(
            "http_requests_total",
            tags={"service": "auth", "endpoint": "/api/v1/auth/logout", "method": "POST"},
        )
        body = await request.json()
        refresh_token = body.get("refresh_token", "")

        if refresh_token:
            await self.db.execute_async(
                "DELETE FROM refresh_tokens WHERE token_id = $1", [refresh_token]
            )

        return web.json_response({"status": "logged_out"})

    async def _me(self, request: web.Request) -> web.Response:
        self.metrics.counter(
            "http_requests_total",
            tags={"service": "auth", "endpoint": "/api/v1/auth/me", "method": "GET"},
        )
        return web.json_response({
            "user_id": request["user_id"],
            "tenant_id": request["tenant_id"],
            "role": request["role"],
        })

    # ── User management endpoints ──────────────────────────────────────────

    async def _create_user(self, request: web.Request) -> web.Response:
        self.metrics.counter(
            "http_requests_total",
            tags={"service": "auth", "endpoint": "/api/v1/auth/users", "method": "POST"},
        )
        if request["role"] != "admin":
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Admin role required"}),
            )

        body = await request.json()
        email = body.get("email", "")
        password = body.get("password", "")
        role = body.get("role", "viewer")

        if not email or not password:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "email and password are required"}),
            )

        # Check for duplicate email
        existing = await self._find_user_by_email(email)
        if existing is not None:
            return web.Response(
                status=409,
                content_type="application/json",
                text=json.dumps({"error": "Email already exists"}),
            )

        user_id = uuid.uuid4().hex
        tenant_id = request["tenant_id"]
        pw_hash = hash_password(password)

        row = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "email": email,
            "password_hash": pw_hash,
            "role": role,
        }
        await self.db.insert_one_async("users", row)

        self.log.info("User created", user_id=user_id, tenant_id=tenant_id, email=email)
        return web.json_response(
            {"user_id": user_id, "tenant_id": tenant_id, "email": email, "role": role},
            status=201,
        )

    async def _list_users(self, request: web.Request) -> web.Response:
        self.metrics.counter(
            "http_requests_total",
            tags={"service": "auth", "endpoint": "/api/v1/auth/users", "method": "GET"},
        )
        if request["role"] != "admin":
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Admin role required"}),
            )

        tenant_id = request["tenant_id"]
        rows = await self.db.fetch_all_async("SELECT * FROM users")
        users = [
            {
                "user_id": r["user_id"],
                "tenant_id": r["tenant_id"],
                "email": r["email"],
                "role": r["role"],
            }
            for r in rows
            if r.get("tenant_id") == tenant_id
        ]
        return web.json_response(users)

    # ── Helpers ─────────────────────────────────────────────────────────────

    async def _find_user_by_email(self, email: str) -> dict | None:
        rows = await self.db.fetch_all_async("SELECT * FROM users")
        for row in rows:
            if row.get("email") == email:
                return row
        return None

    async def _find_user_by_id(self, user_id: str) -> dict | None:
        rows = await self.db.fetch_all_async("SELECT * FROM users")
        for row in rows:
            if row.get("user_id") == user_id:
                return row
        return None

    async def _find_refresh_token(self, token_id: str) -> dict | None:
        rows = await self.db.fetch_all_async("SELECT * FROM refresh_tokens")
        for row in rows:
            if row.get("token_id") == token_id:
                return row
        return None

    async def _create_refresh_token(self, user_id: str) -> str:
        token_id = uuid.uuid4().hex
        expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=self.refresh_token_ttl)
        await self.db.insert_one_async("refresh_tokens", {
            "token_id": token_id,
            "user_id": user_id,
            "expires_at": expires_at.isoformat(),
        })
        return token_id

    async def _stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None


module_class = AuthModule
