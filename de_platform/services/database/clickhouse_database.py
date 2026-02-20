"""ClickHouse database implementation using clickhouse-connect.

ClickHouse is a column-oriented OLAP database optimised for large analytic
inserts and queries. This implementation bridges the ``DatabaseInterface`` to
ClickHouse while adapting to its semantics:

- Bulk inserts use ``client.insert()`` with columnar data for efficiency.
- There are no ACID transactions; ``execute()`` sends arbitrary SQL statements.
- ``fetch_one`` / ``fetch_all`` use ``client.query()`` and return dicts.
- Parameter binding uses ClickHouse's ``{name:Type}`` syntax when params are
  provided as a dict; positional params are not directly supported so a basic
  positional ``$N`` → ``{pN:String}`` shim is applied for simple cases.

Config (via secrets, prefix defaults to ``"DB_CLICKHOUSE"``):

    {prefix}_HOST        - Hostname (default: localhost)
    {prefix}_PORT        - HTTP port (default: 8123)
    {prefix}_DATABASE    - Database name (default: default)
    {prefix}_USER        - Username (default: default)
    {prefix}_PASSWORD    - Password (default: "")
"""

from __future__ import annotations

from typing import Any

from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.secrets.interface import SecretsInterface


class ClickHouseDatabase(DatabaseInterface):
    def __init__(
        self, secrets: SecretsInterface, prefix: str = "DB_CLICKHOUSE"
    ) -> None:
        self._host = secrets.get_or_default(f"{prefix}_HOST", "localhost")
        self._port = int(secrets.get_or_default(f"{prefix}_PORT", "8123"))
        self._database = secrets.get_or_default(f"{prefix}_DATABASE", "default")
        self._user = secrets.get_or_default(f"{prefix}_USER", "default")
        self._password = secrets.get_or_default(f"{prefix}_PASSWORD", "clickhouse")
        self._client: Any = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        import clickhouse_connect  # type: ignore[import-untyped]

        self._client = clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            database=self._database,
            username=self._user,
            password=self._password,
        )

    def disconnect(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def _check_connected(self) -> None:
        if self._client is None:
            raise RuntimeError("ClickHouseDatabase is not connected. Call connect() first.")

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def execute(self, query: str, params: list[Any] | None = None) -> int:
        """Execute a DDL or DML statement. Returns 0 (row count unavailable)."""
        self._check_connected()
        kw_params = _positional_to_named(params) if params else {}
        self._client.command(query, parameters=kw_params)
        return 0

    def fetch_one(self, query: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        self._check_connected()
        kw_params = _positional_to_named(params) if params else {}
        result = self._client.query(query, parameters=kw_params)
        rows = result.named_results()
        return next(iter(rows), None)

    def fetch_all(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        self._check_connected()
        kw_params = _positional_to_named(params) if params else {}
        result = self._client.query(query, parameters=kw_params)
        return list(result.named_results())

    # ------------------------------------------------------------------
    # Bulk insert (columnar — ClickHouse's preferred path)
    # ------------------------------------------------------------------

    def insert_one(self, table: str, row: dict[str, Any]) -> int:
        self._check_connected()
        column_names = list(row.keys())
        data = [[row[col] for col in column_names]]
        self._client.insert(table, data, column_names=column_names)
        return 1

    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert rows into *table* using ClickHouse's columnar insert API.

        All dicts must have the same keys (column names).
        Returns the number of rows inserted.
        """
        self._check_connected()
        if not rows:
            return 0

        column_names = list(rows[0].keys())
        data = [[row[col] for col in column_names] for row in rows]
        self._client.insert(table, data, column_names=column_names)
        return len(rows)

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def health_check(self) -> bool:
        try:
            if self._client is None:
                return False
            self._client.command("SELECT 1")
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _positional_to_named(params: list[Any]) -> dict[str, Any]:
    """Convert a positional ``[$1, $2, ...]`` param list to a ``{p1: …}`` dict.

    This is a best-effort shim for the simple cases where callers use
    ``$1``-style placeholders. For complex queries use named params directly
    by passing a dict instead.
    """
    return {f"p{i + 1}": v for i, v in enumerate(params)}
