"""PipelineTestClient -- shared test interaction methods backed by a harness.

Encapsulates all DB, Kafka, and ingestion interactions so that harness
implementations remain thin adapters while tests use a unified API.
"""

from __future__ import annotations

from typing import Any, Callable

from tests.helpers.harness.protocol import PipelineHarness


class PipelineTestClient:
    """Shared test interaction methods backed by a ``PipelineHarness``."""

    def __init__(self, harness: PipelineHarness) -> None:
        self._harness = harness

    @property
    def tenant_id(self) -> str:
        return self._harness.tenant_id

    # -- Ingestion ------------------------------------------------------------

    async def ingest_events(
        self,
        event_type: str,
        events: list[dict],
        method: str = "rest",
    ) -> None:
        """Ingest events via the specified method (rest, kafka, files)."""
        await self._harness.ingest(method, event_type, events)

    async def ingest_file(self, event_type: str, events: list[dict]) -> None:
        """Ingest events via file upload."""
        await self._harness.ingest("files", event_type, events)

    # -- Polling / waiting ----------------------------------------------------

    async def wait_for_rows(
        self,
        table: str,
        expected: int,
        timeout: float = 30.0,
    ) -> list[dict]:
        """Wait until at least ``expected`` rows appear in ``table``."""
        return await self._harness.wait_for_rows(table, expected, timeout)

    async def wait_for_no_new_rows(
        self,
        table: str,
        known: int,
        timeout: float = 3.0,
    ) -> None:
        """Assert no new rows appear in ``table`` beyond ``known``."""
        await self._harness.wait_for_no_new_rows(table, known, timeout)

    async def wait_for_alerts(
        self,
        expected: int = 1,
        timeout: float = 60.0,
    ) -> list[dict]:
        """Wait until at least ``expected`` alerts exist."""
        return await self._harness.wait_for_alert(
            lambda _: True, timeout=timeout,
        )

    async def wait_for_alert(
        self,
        predicate: Callable[[dict], bool],
        timeout: float = 60.0,
    ) -> list[dict]:
        """Wait until an alert matching ``predicate`` exists."""
        return await self._harness.wait_for_alert(predicate, timeout=timeout)

    # -- Direct queries -------------------------------------------------------

    async def fetch_rows(self, table: str) -> list[dict]:
        """Fetch current rows from ``table`` (no waiting)."""
        return await self._harness.wait_for_rows(table, 0, timeout=0.1)

    async def fetch_alerts(self) -> list[dict]:
        """Fetch current alerts (no waiting)."""
        return await self._harness.fetch_alerts()

    # -- API calls ------------------------------------------------------------

    async def query_api(
        self,
        endpoint: str,
        params: dict[str, str] | None = None,
    ) -> tuple[int, Any]:
        """Query the data API."""
        return await self._harness.query_api(endpoint, params or {})

    async def call_service(
        self,
        service: str,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, Any]:
        """Call an arbitrary service endpoint."""
        return await self._harness.call_service(
            service, method, path, json=json, params=params, headers=headers,
        )

    # -- Kafka ----------------------------------------------------------------

    async def publish(self, topic: str, message: dict) -> None:
        """Publish a message directly to a Kafka topic."""
        await self._harness.publish_to_normalizer(topic, message)

    # -- Step logger ----------------------------------------------------------

    def step(self, name: str, description: str = ""):
        """Create a named step for the test report."""
        return self._harness.step(name, description)
