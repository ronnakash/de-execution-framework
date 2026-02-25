"""Tests for Phase 3 data model evolution.

Covers:
- New fields (additional_fields, client_id, ingestion_method) in enrichment
- event_ids on Alert dataclass
- Ingestion module ingestion_method tagging
"""

from __future__ import annotations

import json
from datetime import datetime

from de_platform.pipeline.algorithms import Alert, LargeNotionalAlgo, VelocityAlgo, SuspiciousCounterpartyAlgo
from de_platform.pipeline.enrichment import (
    enrich_trade_event,
    enrich_transaction_event,
    _passthrough_new_fields,
)


# ── Fake CurrencyConverter ───────────────────────────────────────────────────


class _FakeConverter:
    def convert(self, amount: float, currency: str) -> float:
        return amount  # 1:1 conversion


# ── Enrichment passthrough ───────────────────────────────────────────────────


class TestPassthroughNewFields:
    def test_defaults_when_no_fields_present(self) -> None:
        extras = _passthrough_new_fields({})
        assert extras["client_id"] == ""
        assert extras["ingestion_method"] == ""
        assert extras["additional_fields"] == "{}"

    def test_preserves_existing_values(self) -> None:
        msg = {
            "client_id": "cli-1",
            "ingestion_method": "kafka",
            "additional_fields": '{"custom": "data"}',
        }
        extras = _passthrough_new_fields(msg)
        # Existing string values should not be overridden
        assert "client_id" not in extras
        assert "ingestion_method" not in extras
        assert "additional_fields" not in extras

    def test_dict_additional_fields_serialized(self) -> None:
        msg = {"additional_fields": {"custom": "data"}}
        extras = _passthrough_new_fields(msg)
        assert extras["additional_fields"] == '{"custom": "data"}'


class TestEnrichmentWithNewFields:
    def test_trade_event_gets_defaults(self) -> None:
        msg = {
            "id": "o1",
            "tenant_id": "t1",
            "event_type": "order",
            "quantity": 100,
            "price": 50.0,
            "currency": "USD",
            "transact_time": "2026-01-15T10:00:00Z",
        }
        result = enrich_trade_event(msg, _FakeConverter(), "2026-01-15T10:00:01Z")
        assert result["client_id"] == ""
        assert result["ingestion_method"] == ""
        assert result["additional_fields"] == "{}"

    def test_trade_event_preserves_client_id(self) -> None:
        msg = {
            "id": "o1",
            "tenant_id": "t1",
            "event_type": "order",
            "quantity": 100,
            "price": 50.0,
            "currency": "USD",
            "transact_time": "2026-01-15T10:00:00Z",
            "client_id": "cli-42",
            "ingestion_method": "rest",
        }
        result = enrich_trade_event(msg, _FakeConverter(), "2026-01-15T10:00:01Z")
        assert result["client_id"] == "cli-42"
        assert result["ingestion_method"] == "rest"

    def test_transaction_event_gets_defaults(self) -> None:
        msg = {
            "id": "tx1",
            "tenant_id": "t1",
            "event_type": "transaction",
            "amount": 1000.0,
            "currency": "USD",
            "transact_time": "2026-01-15T10:00:00Z",
        }
        result = enrich_transaction_event(msg, _FakeConverter(), "2026-01-15T10:00:01Z")
        assert result["client_id"] == ""
        assert result["ingestion_method"] == ""
        assert result["additional_fields"] == "{}"

    def test_additional_fields_dict_serialized_in_enrichment(self) -> None:
        msg = {
            "id": "o1",
            "tenant_id": "t1",
            "event_type": "order",
            "quantity": 100,
            "price": 50.0,
            "currency": "USD",
            "transact_time": "2026-01-15T10:00:00Z",
            "additional_fields": {"source": "bloomberg"},
        }
        result = enrich_trade_event(msg, _FakeConverter(), "2026-01-15T10:00:01Z")
        assert json.loads(result["additional_fields"]) == {"source": "bloomberg"}


# ── Alert event_ids ──────────────────────────────────────────────────────────


class TestAlertEventIds:
    def test_large_notional_has_event_ids(self) -> None:
        algo = LargeNotionalAlgo(threshold_usd=1_000_000)
        alerts = algo.evaluate_window(
            [{"id": "o42", "tenant_id": "t1", "event_type": "order",
              "message_id": "m1", "notional_usd": 2_000_000}],
            "t1", datetime.min, datetime.max,
        )
        assert alerts[0].event_ids == ["o42"]

    def test_velocity_has_all_event_ids(self) -> None:
        algo = VelocityAlgo(max_events=2, window_seconds=60)
        events = [
            {"id": f"e-{i}", "tenant_id": "t1", "event_type": "order", "message_id": f"m-{i}"}
            for i in range(5)
        ]
        alerts = algo.evaluate_window(events, "t1", datetime.min, datetime.max)
        assert alerts[0].event_ids == ["e-0", "e-1", "e-2", "e-3", "e-4"]

    def test_suspicious_counterparty_has_event_ids(self) -> None:
        algo = SuspiciousCounterpartyAlgo(suspicious_ids={"BAD1"})
        alerts = algo.evaluate_window(
            [{"id": "tx99", "tenant_id": "t1", "event_type": "transaction",
              "message_id": "m1", "counterparty_id": "BAD1"}],
            "t1", datetime.min, datetime.max,
        )
        assert alerts[0].event_ids == ["tx99"]

    def test_event_ids_in_to_dict(self) -> None:
        alert = Alert(
            alert_id="a1", tenant_id="t1", event_type="order", event_id="o1",
            message_id="m1", algorithm="test", severity="high",
            description="test", details={}, created_at="2026-01-01T00:00:00Z",
            event_ids=["o1", "o2"],
        )
        d = alert.to_dict()
        assert d["event_ids"] == ["o1", "o2"]

    def test_event_ids_none_not_in_to_dict(self) -> None:
        alert = Alert(
            alert_id="a1", tenant_id="t1", event_type="order", event_id="o1",
            message_id="m1", algorithm="test", severity="high",
            description="test", details={}, created_at="2026-01-01T00:00:00Z",
        )
        d = alert.to_dict()
        assert "event_ids" not in d


# ── Alert manager event_ids persistence ──────────────────────────────────────


class TestAlertManagerEventIds:
    def test_prepare_db_row_adds_event_ids_default(self) -> None:
        from de_platform.modules.alert_manager.main import AlertManagerModule
        row = AlertManagerModule._prepare_db_row({
            "alert_id": "a1", "event_id": "o1", "details": '{}',
            "created_at": "2026-01-15T10:00:00Z",
        })
        assert row["event_ids"] == ["o1"]

    def test_prepare_db_row_preserves_event_ids(self) -> None:
        from de_platform.modules.alert_manager.main import AlertManagerModule
        row = AlertManagerModule._prepare_db_row({
            "alert_id": "a1", "event_id": "o1", "details": '{}',
            "created_at": "2026-01-15T10:00:00Z",
            "event_ids": ["o1", "o2", "o3"],
        })
        assert row["event_ids"] == ["o1", "o2", "o3"]
