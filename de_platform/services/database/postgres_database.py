from __future__ import annotations

import contextvars
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import asyncpg

from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.secrets.interface import SecretsInterface


class PostgresDatabase(DatabaseInterface):
    """PostgreSQL implementation using asyncpg with connection pooling.

    All query methods are sync wrappers around async operations. When used
    from an AsyncModule, prefer calling the _async variants directly.
    """

    def __init__(self, secrets: SecretsInterface) -> None:
        self._secrets = secrets
        self._pool: asyncpg.Pool | None = None
        self._tx_conn: contextvars.ContextVar[asyncpg.Connection | None] = contextvars.ContextVar(
            "pg_tx_conn", default=None
        )

    # -- Connection management ------------------------------------------------

    async def connect_async(self) -> None:
        url = self._secrets.require("DB_POSTGRES_URL")
        min_size = int(self._secrets.get_or_default("DB_POSTGRES_POOL_MIN", "2"))
        max_size = int(self._secrets.get_or_default("DB_POSTGRES_POOL_MAX", "10"))
        timeout = int(self._secrets.get_or_default("DB_POSTGRES_STATEMENT_TIMEOUT", "30000"))
        self._pool = await asyncpg.create_pool(
            url,
            min_size=min_size,
            max_size=max_size,
            command_timeout=timeout / 1000,
        )

    def connect(self) -> None:
        import asyncio

        asyncio.get_event_loop().run_until_complete(self.connect_async())

    async def disconnect_async(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    def disconnect(self) -> None:
        import asyncio

        asyncio.get_event_loop().run_until_complete(self.disconnect_async())

    def is_connected(self) -> bool:
        return self._pool is not None and not self._pool._closed  # type: ignore[attr-defined]

    # -- Helpers --------------------------------------------------------------

    def _check_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database is not connected. Call connect() first.")
        return self._pool

    async def _acquire(self) -> asyncpg.Connection:
        """Return the transaction connection if active, else acquire from pool."""
        conn = self._tx_conn.get(None)
        if conn is not None:
            return conn
        return await self._check_pool().acquire()

    async def _release(self, conn: asyncpg.Connection) -> None:
        """Release a connection back to the pool (skip if it's the tx connection)."""
        if conn is not self._tx_conn.get(None):
            await self._check_pool().release(conn)

    # -- Transactions ---------------------------------------------------------

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        pool = self._check_pool()
        conn = await pool.acquire()
        tx = conn.transaction()
        await tx.start()
        token = self._tx_conn.set(conn)
        try:
            yield
            await tx.commit()
        except BaseException:
            await tx.rollback()
            raise
        finally:
            self._tx_conn.reset(token)
            await pool.release(conn)

    # -- Query methods (async) ------------------------------------------------

    async def execute_async(self, query: str, params: list[Any] | None = None) -> int:
        conn = await self._acquire()
        try:
            result = await conn.execute(query, *(params or []))
            # asyncpg returns e.g. "INSERT 0 5" — extract affected count
            parts = result.split()
            return int(parts[-1]) if parts and parts[-1].isdigit() else 0
        finally:
            await self._release(conn)

    async def fetch_one_async(
        self, query: str, params: list[Any] | None = None
    ) -> dict[str, Any] | None:
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(query, *(params or []))
            return dict(row) if row else None
        finally:
            await self._release(conn)

    async def fetch_all_async(
        self, query: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        conn = await self._acquire()
        try:
            rows = await conn.fetch(query, *(params or []))
            return [dict(r) for r in rows]
        finally:
            await self._release(conn)

    async def bulk_insert_async(self, table: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        conn = await self._acquire()
        try:
            columns = list(rows[0].keys())
            records = [tuple(r[c] for c in columns) for r in rows]
            try:
                await conn.copy_records_to_table(table, records=records, columns=columns)
            except Exception:
                # Fallback to executemany
                placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
                cols = ", ".join(columns)
                query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
                await conn.executemany(query, records)
            return len(rows)
        finally:
            await self._release(conn)

    async def health_check_async(self) -> bool:
        try:
            pool = self._check_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception:
            return False

    # -- Sync interface (wraps async) -----------------------------------------

    def execute(self, query: str, params: list[Any] | None = None) -> int:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.execute_async(query, params))

    def fetch_one(self, query: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.fetch_one_async(query, params))

    def fetch_all(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.fetch_all_async(query, params))

    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.bulk_insert_async(table, rows))

    def health_check(self) -> bool:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.health_check_async())
