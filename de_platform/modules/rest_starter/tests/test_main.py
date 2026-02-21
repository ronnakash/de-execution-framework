"""Tests for the REST Starter module using aiohttp TestClient and MemoryQueue."""

from __future__ import annotations

import json
import pytest
from aiohttp.test_utils import TestClient, TestServer
from aiohttp import web

from de_platform.config.context import ModuleConfig
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.message_queue.memory_queue import MemoryQueue


# ── Helpers ───────────────────────────────────────────────────────────────────

VALID_ORDER = {
    "id": "ord-1", "tenant_id": "t1", "status": "new",
    "transact_time": "2026-01-15T10:00:00Z", "symbol": "AAPL",
    "side": "buy", "quantity": 100.0, "price": 150.0,
    "order_type": "limit", "currency": "USD",
}

VALID_EXECUTION = {
    "id": "exec-1", "tenant_id": "t1", "status": "filled",
    "transact_time": "2026-01-15T10:00:00Z", "order_id": "ord-1",
    "symbol": "AAPL", "side": "sell", "quantity": 50.0, "price": 151.0,
    "execution_venue": "NYSE", "currency": "EUR",
}

VALID_TX = {
    "id": "tx-1", "tenant_id": "t1", "status": "settled",
    "transact_time": "2026-01-15T10:00:00Z", "account_id": "acc-1",
    "counterparty_id": "cp-1", "amount": 10000.0, "currency": "USD",
    "transaction_type": "wire",
}


def _build_app(mq: MemoryQueue) -> web.Application:
    """Build the aiohttp app directly for testing without running the full module lifecycle."""
    from de_platform.modules.rest_starter.main import RestStarterModule, dto_to_message_from_raw
    from de_platform.pipeline.serialization import error_to_dict
    from de_platform.pipeline.topics import NORMALIZATION_ERRORS, TRADE_NORMALIZATION, TX_NORMALIZATION
    from de_platform.pipeline.validation import group_errors_by_event, validate_events
    from typing import Any

    async def handle(request: web.Request, event_type: str) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON body"}, status=400)
        events = body.get("events", [])
        valid, errors = validate_events(event_type, events)
        topic = TRADE_NORMALIZATION if event_type in ("order", "execution") else TX_NORMALIZATION
        for raw in valid:
            mq.publish(topic, dto_to_message_from_raw(raw, event_type))
        for event_index, event_errors in group_errors_by_event(errors).items():
            raw_event = events[event_index] if event_index < len(events) else {}
            mq.publish(NORMALIZATION_ERRORS, error_to_dict(raw_event, event_type, event_errors))
        rejected = {e.event_index for e in errors}
        return web.json_response({
            "accepted": len(valid),
            "rejected": len(rejected),
            "errors": [{"event_index": e.event_index, "field": e.field, "message": e.message} for e in errors],
        })

    app = web.Application()
    app.router.add_post("/api/v1/orders", lambda r: handle(r, "order"))
    app.router.add_post("/api/v1/executions", lambda r: handle(r, "execution"))
    app.router.add_post("/api/v1/transactions", lambda r: handle(r, "transaction"))
    app.router.add_get("/api/v1/health", lambda r: web.json_response({"status": "ok"}))
    return app


@pytest.fixture
def mq() -> MemoryQueue:
    return MemoryQueue()


@pytest.fixture
async def client(mq: MemoryQueue) -> TestClient:
    app = _build_app(mq)
    server = TestServer(app)
    cli = TestClient(server)
    await cli.start_server()
    yield cli
    await cli.close()


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_health_endpoint(client: TestClient):
    resp = await client.get("/api/v1/health")
    assert resp.status == 200
    body = await resp.json()
    assert body["status"] == "ok"


async def test_ingest_valid_orders(client: TestClient, mq: MemoryQueue):
    resp = await client.post("/api/v1/orders", json={"events": [VALID_ORDER, VALID_ORDER]})
    assert resp.status == 200
    body = await resp.json()
    assert body["accepted"] == 2
    assert body["rejected"] == 0
    assert body["errors"] == []


async def test_orders_published_to_trade_normalization_topic(client: TestClient, mq: MemoryQueue):
    await client.post("/api/v1/orders", json={"events": [VALID_ORDER]})
    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "order"
    assert msg["id"] == "ord-1"
    assert "message_id" in msg
    assert "ingested_at" in msg


async def test_executions_published_to_trade_normalization_topic(client: TestClient, mq: MemoryQueue):
    await client.post("/api/v1/executions", json={"events": [VALID_EXECUTION]})
    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "execution"


async def test_transactions_published_to_tx_normalization_topic(client: TestClient, mq: MemoryQueue):
    await client.post("/api/v1/transactions", json={"events": [VALID_TX]})
    msg = mq.consume_one(TX_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "transaction"


async def test_ingest_invalid_orders(client: TestClient, mq: MemoryQueue):
    bad = {**VALID_ORDER, "quantity": -1}
    resp = await client.post("/api/v1/orders", json={"events": [bad]})
    assert resp.status == 200
    body = await resp.json()
    assert body["accepted"] == 0
    assert body["rejected"] == 1
    assert len(body["errors"]) > 0
    # Nothing on normalization topic
    assert mq.consume_one(TRADE_NORMALIZATION) is None


async def test_errors_published_to_normalization_errors_topic(client: TestClient, mq: MemoryQueue):
    bad = {**VALID_ORDER, "price": 0}
    await client.post("/api/v1/orders", json={"events": [bad]})
    err_msg = mq.consume_one(NORMALIZATION_ERRORS)
    assert err_msg is not None
    assert err_msg["event_type"] == "order"
    assert len(err_msg["errors"]) > 0


async def test_ingest_mixed_batch(client: TestClient, mq: MemoryQueue):
    bad = {**VALID_ORDER, "side": "long"}
    resp = await client.post("/api/v1/orders", json={"events": [VALID_ORDER, bad, VALID_ORDER]})
    body = await resp.json()
    assert body["accepted"] == 2
    assert body["rejected"] == 1


async def test_multi_field_invalid_event_produces_single_error_message(client: TestClient, mq: MemoryQueue):
    """An event with multiple validation failures produces 1 error on the topic."""
    bad = {**VALID_ORDER, "id": "", "currency": "x", "quantity": -5}
    resp = await client.post("/api/v1/orders", json={"events": [bad]})
    body = await resp.json()
    assert body["accepted"] == 0
    assert body["rejected"] == 1
    # Response still lists all individual errors
    assert len(body["errors"]) >= 3

    # Exactly 1 consolidated error message on normalization_errors
    err_msg = mq.consume_one(NORMALIZATION_ERRORS)
    assert err_msg is not None
    assert len(err_msg["errors"]) >= 3
    assert mq.consume_one(NORMALIZATION_ERRORS) is None


async def test_invalid_json_body(client: TestClient, mq: MemoryQueue):
    resp = await client.post(
        "/api/v1/orders",
        data=b"not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400
