"""Tests for CurrencyConverter using MemoryDatabase and MemoryCache."""

from __future__ import annotations

import pytest

from de_platform.pipeline.currency import CurrencyConverter
from de_platform.services.cache.memory_cache import MemoryCache
from de_platform.services.database.memory_database import MemoryDatabase


# ── Stub ──────────────────────────────────────────────────────────────────────

class _RateDb(MemoryDatabase):
    """MemoryDatabase subclass with two-param WHERE support for currency tests."""

    def fetch_one(self, query: str, params=None):
        self._check_connected()
        if "currency_rates" in query and params and len(params) >= 2:
            from_curr, to_curr = params[0], params[1]
            for row in self._tables.get("currency_rates", []):
                if row["from_currency"] == from_curr and row["to_currency"] == to_curr:
                    return row
            return None
        return super().fetch_one(query, params)


def _db_with_rates(rates: list[dict]) -> _RateDb:
    db = _RateDb()
    db.connect()
    db.bulk_insert("currency_rates", rates)
    return db


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_convert_usd_to_usd_returns_same():
    converter = CurrencyConverter(db=_db_with_rates([]), cache=MemoryCache())
    assert converter.convert(500.0, "USD", "USD") == 500.0


def test_currency_converter_reads_from_db():
    db = _db_with_rates([{"from_currency": "EUR", "to_currency": "USD", "rate": 1.08}])
    converter = CurrencyConverter(db=db, cache=MemoryCache())
    rate = converter.get_rate("EUR", "USD")
    assert rate == pytest.approx(1.08)


def test_currency_converter_cache_miss_reads_db():
    cache = MemoryCache()
    db = _db_with_rates([{"from_currency": "GBP", "to_currency": "USD", "rate": 1.27}])
    converter = CurrencyConverter(db=db, cache=cache)

    # Cache is empty — falls through to DB
    rate = converter.get_rate("GBP", "USD")
    assert rate == pytest.approx(1.27)


def test_currency_converter_cache_hit_skips_db():
    cache = MemoryCache()
    cache.set("currency_rate:EUR_USD", 1.1)  # pre-populate cache

    # DB is empty — but cache has the rate
    db = _db_with_rates([])
    converter = CurrencyConverter(db=db, cache=cache)

    rate = converter.get_rate("EUR", "USD")
    assert rate == pytest.approx(1.1)


def test_currency_converter_populates_cache_after_db_read():
    cache = MemoryCache()
    db = _db_with_rates([{"from_currency": "JPY", "to_currency": "USD", "rate": 0.0067}])
    converter = CurrencyConverter(db=db, cache=cache)

    converter.get_rate("JPY", "USD")

    # Now remove from DB and verify cache is used
    db._tables["currency_rates"] = []
    rate = converter.get_rate("JPY", "USD")
    assert rate == pytest.approx(0.0067)


def test_convert_multiplies_amount_by_rate():
    db = _db_with_rates([{"from_currency": "EUR", "to_currency": "USD", "rate": 2.0}])
    converter = CurrencyConverter(db=db, cache=MemoryCache())
    result = converter.convert(500.0, "EUR", "USD")
    assert result == pytest.approx(1000.0)


def test_missing_rate_raises_key_error():
    converter = CurrencyConverter(db=_db_with_rates([]), cache=MemoryCache())
    with pytest.raises(KeyError, match="CHF"):
        converter.get_rate("CHF", "USD")
