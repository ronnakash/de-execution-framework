"""Tests for the Currency Rate Loader module."""

from __future__ import annotations

import json

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.services.database.memory_database import MemoryDatabase
from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
from de_platform.services.logger.factory import LoggerFactory


# ── Helper ────────────────────────────────────────────────────────────────────

def _run(rates_file: str, data: bytes) -> tuple[int, MemoryDatabase]:
    from de_platform.modules.currency_loader.main import CurrencyLoaderModule

    fs = MemoryFileSystem()
    fs.write(rates_file, data)
    db = MemoryDatabase()
    config = ModuleConfig({"rates-file": rates_file})
    module = CurrencyLoaderModule(config, LoggerFactory(), db, fs)
    rc = module.run()
    return rc, db


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_currency_loader_inserts_rates():
    rates = [
        {"from_currency": "EUR", "to_currency": "USD", "rate": 1.08},
        {"from_currency": "GBP", "to_currency": "USD", "rate": 1.27},
    ]
    rc, db = _run("rates.json", json.dumps(rates).encode())
    assert rc == 0
    rows = db._tables.get("currency_rates", [])
    assert len(rows) == 2
    assert rows[0]["from_currency"] == "EUR"
    assert rows[0]["rate"] == 1.08


def test_currency_loader_uppercases_currency_codes():
    rates = [{"from_currency": "eur", "to_currency": "usd", "rate": 1.08}]
    rc, db = _run("rates.json", json.dumps(rates).encode())
    rows = db._tables.get("currency_rates", [])
    assert rows[0]["from_currency"] == "EUR"
    assert rows[0]["to_currency"] == "USD"


def test_currency_loader_skips_incomplete_records():
    rates = [
        {"from_currency": "EUR", "to_currency": "USD", "rate": 1.08},
        {"from_currency": "GBP"},  # missing to_currency and rate
    ]
    rc, db = _run("rates.json", json.dumps(rates).encode())
    assert rc == 0
    rows = db._tables.get("currency_rates", [])
    assert len(rows) == 1


def test_currency_loader_empty_file_returns_zero():
    rc, db = _run("rates.json", json.dumps([]).encode())
    assert rc == 0
    assert db._tables.get("currency_rates", []) == []


def test_currency_loader_missing_rates_file_raises():
    from de_platform.modules.currency_loader.main import CurrencyLoaderModule

    module = CurrencyLoaderModule(
        ModuleConfig({}), LoggerFactory(), MemoryDatabase(), MemoryFileSystem(),
    )
    module.initialize()
    with pytest.raises(ValueError, match="rates-file"):
        module.validate()
