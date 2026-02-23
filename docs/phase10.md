# Phase 10: Bug Investigation & Fixes

This document describes four bugs discovered during testing and production validation,
along with their root causes, exact code changes, affected files, and verification steps.

---

## Table of Contents

1. [Bug 10.1: Admin user sees no events without tenant filter](#bug-101-admin-user-sees-no-events-without-tenant-filter)
2. [Bug 10.2: Missing semicolon in alerts migration](#bug-102-missing-semicolon-in-alerts-migration)
3. [Bug 10.3: No proxy routes in data_api for production](#bug-103-no-proxy-routes-in-data_api-for-production)
4. [Bug 10.4: Redis key TTL](#bug-104-redis-key-ttl)
5. [Deliverables](#deliverables)
6. [Verification](#verification)

---

## Bug 10.1: Admin user sees no events without tenant filter

### Symptom

An admin user logging into the UI and navigating to the Events page sees zero results.
Events only appear if the admin manually adds a `?tenant_id=SOME_CLIENT` query parameter.

### Root Cause

In both `data_api/main.py` (lines 115-124) and `alert_manager/main.py` (lines 311-318),
the `_resolve_tenant_id()` method returns the admin's own JWT `tenant_id` when no query
parameter is provided. The admin's own tenant has no pipeline data -- pipeline events use
`INTEGRATION_CLIENT_*` tenant IDs. Since the method returns the admin's tenant instead of
`None`, the query filters to an empty result set.

### Current Code

Both files contain identical logic:

```python
def _resolve_tenant_id(self, request: web.Request) -> str | None:
    """Get tenant_id from JWT (if auth active) with admin override via query param."""
    jwt_tenant = request.get("tenant_id")
    query_tenant = request.rel_url.query.get("tenant_id")
    if jwt_tenant:
        # Admin users can query other tenants via ?tenant_id=
        if request.get("role") == "admin" and query_tenant:
            return query_tenant
        return jwt_tenant
    return query_tenant
```

The problem is in the `if jwt_tenant:` block: when the user is an admin and `query_tenant`
is `None` (no filter specified), execution falls through to `return jwt_tenant`, which
returns the admin's own tenant ID instead of `None` (all tenants).

### Fix

Replace the method in both files with:

```python
def _resolve_tenant_id(self, request: web.Request) -> str | None:
    """Get tenant_id from JWT (if auth active) with admin override via query param."""
    jwt_tenant = request.get("tenant_id")
    query_tenant = request.rel_url.query.get("tenant_id")
    if jwt_tenant and request.get("role") == "admin":
        return query_tenant  # None means "all tenants"
    return query_tenant or jwt_tenant
```

When the user is an admin:
- If `query_tenant` is provided, return that specific tenant (filter).
- If `query_tenant` is `None`, return `None` (show all tenants).

When the user is a regular (non-admin) user:
- Return `query_tenant` if provided, otherwise `jwt_tenant` (always scoped to own tenant).

### Files to Change

| File | Lines |
|------|-------|
| `de_platform/modules/data_api/main.py` | 115-124 |
| `de_platform/modules/alert_manager/main.py` | 311-318 |

---

## Bug 10.2: Missing semicolon in alerts migration

### Symptom

The alerts migration `005_create_alerts_table.up.sql` may fail or leave the
`idx_alerts_severity` index uncreated on strict SQL parsers that require statement
terminators.

### Root Cause

Line 15 of `de_platform/migrations/alerts/005_create_alerts_table.up.sql` is missing a
trailing semicolon:

```sql
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity)
```

### Current Code

```sql
CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    severity TEXT NOT NULL,
    description TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_tenant ON alerts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity)
```

### Fix

Add the missing semicolon on line 15:

```sql
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
```

### Files to Change

| File | Lines |
|------|-------|
| `de_platform/migrations/alerts/005_create_alerts_table.up.sql` | 15 |

---

## Bug 10.3: No proxy routes in data_api for production

### Symptom

When the UI is built and served as static files from `data_api`'s `/ui` route, API calls
to `/api/v1/alerts`, `/api/v1/cases`, `/api/v1/clients`, `/api/v1/audit`, `/api/v1/tasks`,
`/api/v1/runs`, and `/api/v1/auth` all return 404 errors. These routes work in development
because Vite's dev server proxies them to the correct backend ports via `ui/vite.config.ts`.

### Root Cause

The `data_api` module only registers its own routes (`/api/v1/events/*`). In production
there is no Vite dev server to proxy requests to the other backend services. The built UI
makes API calls to the same origin, which is `data_api`, and those paths have no handlers.

### Fix

Add reverse proxy routes in `data_api/main.py` that forward requests to the appropriate
backend services using `aiohttp.ClientSession`.

#### 1. Add proxy URL configuration in `initialize()`

Read service URLs from secrets with sensible localhost defaults. Create a shared
`aiohttp.ClientSession` for outbound proxy requests.

```python
import aiohttp as aiohttp_client  # avoid conflict with existing aiohttp.web import

async def initialize(self) -> None:
    self.log = self.logger.create()
    self.port = self.config.get("port", 8002)
    self.events_db: DatabaseInterface = self.db_factory.get("events")
    await self.events_db.connect_async()

    # Proxy target URLs for production static UI
    self._proxy_targets = {
        "alerts": self.secrets.get("ALERT_MANAGER_URL") or "http://localhost:8007",
        "cases": self.secrets.get("ALERT_MANAGER_URL") or "http://localhost:8007",
        "clients": self.secrets.get("CLIENT_CONFIG_URL") or "http://localhost:8003",
        "audit": self.secrets.get("DATA_AUDIT_URL") or "http://localhost:8005",
        "tasks": self.secrets.get("TASK_SCHEDULER_URL") or "http://localhost:8006",
        "runs": self.secrets.get("TASK_SCHEDULER_URL") or "http://localhost:8006",
        "auth": self.secrets.get("AUTH_URL") or "http://localhost:8004",
    }
    self._proxy_session = aiohttp_client.ClientSession()

    self.lifecycle.on_shutdown(self._stop_server)
    self.lifecycle.on_shutdown(self._close_proxy_session)
    self.lifecycle.on_shutdown(self.events_db.disconnect_async)
    self.log.info("Data API initialized", port=self.port)

async def _close_proxy_session(self) -> None:
    if self._proxy_session:
        await self._proxy_session.close()
```

#### 2. Add `_proxy_handler()` method

```python
async def _proxy_handler(self, request: web.Request, target_base: str) -> web.Response:
    """Forward request to upstream service and return its response."""
    # Reconstruct the target URL preserving path and query string
    target_url = f"{target_base}{request.path}"
    if request.query_string:
        target_url += f"?{request.query_string}"

    # Forward headers (including Authorization) but skip hop-by-hop headers
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "transfer-encoding")
    }

    body = await request.read()

    async with self._proxy_session.request(
        method=request.method,
        url=target_url,
        headers=headers,
        data=body if body else None,
    ) as resp:
        resp_body = await resp.read()
        return web.Response(
            status=resp.status,
            body=resp_body,
            content_type=resp.content_type,
        )
```

#### 3. Register proxy routes in `_create_app()` BEFORE the static route

```python
def _create_app(self) -> web.Application:
    middlewares: list = []
    jwt_secret = self.secrets.get("JWT_SECRET")
    if jwt_secret:
        from de_platform.pipeline.auth_middleware import create_auth_middleware
        middlewares.append(create_auth_middleware(jwt_secret))
    app = web.Application(middlewares=middlewares)

    # Event endpoints (native)
    app.router.add_get("/api/v1/events/orders", self._get_orders)
    app.router.add_get("/api/v1/events/executions", self._get_executions)
    app.router.add_get("/api/v1/events/transactions", self._get_transactions)

    # Proxy routes to backend services (must be before static catch-all)
    for prefix, service_key in [
        ("/api/v1/alerts", "alerts"),
        ("/api/v1/cases", "cases"),
        ("/api/v1/clients", "clients"),
        ("/api/v1/audit", "audit"),
        ("/api/v1/tasks", "tasks"),
        ("/api/v1/runs", "runs"),
        ("/api/v1/auth", "auth"),
    ]:
        target = self._proxy_targets[service_key]
        # Use a closure to capture target for each route
        app.router.add_route(
            "*", prefix + "{path_info:.*}",
            lambda req, t=target: self._proxy_handler(req, t),
        )
        # Also handle the bare prefix (no trailing path)
        app.router.add_route(
            "*", prefix,
            lambda req, t=target: self._proxy_handler(req, t),
        )

    # Serve static UI if the directory is present
    if _STATIC_DIR.exists():
        app.router.add_static("/ui", _STATIC_DIR)

    return app
```

#### Proxy Route Mapping

| Route Pattern | Target Service | Default URL |
|---|---|---|
| `/api/v1/alerts{path_info:.*}` | Alert Manager | `http://localhost:8007` |
| `/api/v1/cases{path_info:.*}` | Alert Manager | `http://localhost:8007` |
| `/api/v1/clients{path_info:.*}` | Client Config | `http://localhost:8003` |
| `/api/v1/audit{path_info:.*}` | Data Audit | `http://localhost:8005` |
| `/api/v1/tasks{path_info:.*}` | Task Scheduler | `http://localhost:8006` |
| `/api/v1/runs{path_info:.*}` | Task Scheduler | `http://localhost:8006` |
| `/api/v1/auth{path_info:.*}` | Auth | `http://localhost:8004` |

### Files to Change

| File | Changes |
|------|---------|
| `de_platform/modules/data_api/main.py` | Add proxy URL config in `initialize()`, add `_proxy_handler()` method, add `_close_proxy_session()` method, register proxy routes in `_create_app()` before static route, create shared `aiohttp.ClientSession` |

---

## Bug 10.4: Redis key TTL

### Symptom

Some Redis cache keys persist indefinitely, consuming memory over time. Pipeline keys
(currency rates, dedup entries, client config) should all have bounded lifetimes.

### Root Cause

Several `cache.set()` calls across the pipeline lack a `ttl` parameter, and existing TTL
values are inconsistent (1 hour for currency, 24 hours for dedup). The target is a uniform
2-hour (7200 second) TTL on all pipeline keys.

### Current State

| Location | Current TTL | Target TTL |
|----------|-------------|------------|
| `de_platform/pipeline/currency.py` line 18 | `_CACHE_TTL = 3600` (1 hour) | `7200` (2 hours) |
| `de_platform/pipeline/dedup.py` line 15 | `_DEDUP_TTL = 86400` (24 hours) | `7200` (2 hours) |
| `de_platform/modules/client_config/main.py` line 366 | No TTL (infinite) | `7200` (2 hours) |
| `de_platform/modules/client_config/main.py` line 381 | No TTL (infinite) | `7200` (2 hours) |

### Fix

#### 1. `de_platform/pipeline/currency.py` line 18

Change:
```python
_CACHE_TTL = 3600  # seconds -- refresh rates every hour
```
To:
```python
_CACHE_TTL = 7200  # seconds -- refresh rates every 2 hours
```

#### 2. `de_platform/pipeline/dedup.py` line 15

Change:
```python
_DEDUP_TTL = 86400  # 24 hours
```
To:
```python
_DEDUP_TTL = 7200  # 2 hours
```

#### 3. `de_platform/modules/client_config/main.py` line 366

Change:
```python
self.cache.set(f"client_config:{tenant_id}", {
    "mode": row.get("mode", "batch"),
    "algo_run_hour": row.get("algo_run_hour"),
    "display_name": row.get("display_name", ""),
    "window_size_minutes": row.get("window_size_minutes", 0),
    "window_slide_minutes": row.get("window_slide_minutes", 0),
    "case_aggregation_minutes": row.get("case_aggregation_minutes", 60),
})
```
To:
```python
self.cache.set(f"client_config:{tenant_id}", {
    "mode": row.get("mode", "batch"),
    "algo_run_hour": row.get("algo_run_hour"),
    "display_name": row.get("display_name", ""),
    "window_size_minutes": row.get("window_size_minutes", 0),
    "window_slide_minutes": row.get("window_slide_minutes", 0),
    "case_aggregation_minutes": row.get("case_aggregation_minutes", 60),
}, ttl=7200)
```

#### 4. `de_platform/modules/client_config/main.py` line 381

Change:
```python
self.cache.set(f"algo_config:{tenant_id}:{algo}", {
    "enabled": row.get("enabled", True),
    "thresholds": thresholds,
})
```
To:
```python
self.cache.set(f"algo_config:{tenant_id}:{algo}", {
    "enabled": row.get("enabled", True),
    "thresholds": thresholds,
}, ttl=7200)
```

### Files to Change

| File | Lines | Change |
|------|-------|--------|
| `de_platform/pipeline/currency.py` | 18 | `_CACHE_TTL = 3600` to `_CACHE_TTL = 7200` |
| `de_platform/pipeline/dedup.py` | 15 | `_DEDUP_TTL = 86400` to `_DEDUP_TTL = 7200` |
| `de_platform/modules/client_config/main.py` | 366 | Add `ttl=7200` to `self.cache.set()` |
| `de_platform/modules/client_config/main.py` | 381 | Add `ttl=7200` to `self.cache.set()` |

---

## Deliverables

| Artifact | Description |
|----------|-------------|
| `docs/phase10_bugs.md` | Investigation findings document |
| All bug fixes applied | Changes to 6 files across 4 bugs |
| `make test` passing | Unit tests verify correctness |

## Verification

### Automated

1. **`make test`** -- all unit tests pass (no regressions from TTL changes, tenant resolution fix, or migration fix).
2. **`make lint`** -- no lint errors introduced.

### Manual (requires `make infra-up` + `make dev`)

1. **Bug 10.1** -- Login as admin user, navigate to Events page. Verify events from all tenants are displayed without needing a `?tenant_id=` filter. Verify that adding `?tenant_id=SPECIFIC_CLIENT` correctly filters to that tenant only.
2. **Bug 10.2** -- Run `make migrate cmd=up args="alerts --db postgres"` and confirm both indexes are created (`idx_alerts_tenant` and `idx_alerts_severity`).
3. **Bug 10.3** -- Build the UI (`cd ui && npm run build`), start `data_api`, and navigate to `/ui`. Verify that Alerts, Cases, Clients, and other pages load data correctly through the proxy routes. Confirm the browser network tab shows 200 responses (not 404) for `/api/v1/alerts`, `/api/v1/cases`, etc.
4. **Bug 10.4** -- After processing some events, use `redis-cli TTL currency_rate:EUR_USD` and similar commands to verify all pipeline keys have TTLs set (should show values <= 7200).
