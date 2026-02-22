"""Integration tests: CurrencyLoaderModule inserts rates into real Postgres.

Verifies that currency_loader can read a JSON file and upsert rates
into the Postgres currency_rates table.
"""

from __future__ import annotations

import json

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.currency_loader.main import CurrencyLoaderModule
from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
from de_platform.services.logger.factory import LoggerFactory


pytestmark = pytest.mark.real_infra


async def test_currency_loader_inserts_to_postgres(warehouse_db):
    """CurrencyLoaderModule loads rates into real Postgres."""
    import uuid

    # Use unique 3-char currency codes to avoid conflicts with seeded data
    suffix = uuid.uuid4().hex[:2].upper()
    from_a, from_b = f"T{suffix}", f"U{suffix}"
    rates = [
        {"from_currency": from_a, "to_currency": "USD", "rate": 0.0067},
        {"from_currency": from_b, "to_currency": "USD", "rate": 1.12},
    ]
    fs = MemoryFileSystem()
    fs.write("rates.json", json.dumps(rates).encode())

    module = CurrencyLoaderModule(
        config=ModuleConfig({"rates-file": "rates.json"}),
        logger=LoggerFactory(default_impl="memory"),
        db=warehouse_db,
        fs=fs,
    )
    rc = await module.run()
    assert rc == 0

    # Verify rates are in Postgres
    rows = await warehouse_db.fetch_all_async(
        "SELECT * FROM currency_rates"
    )
    test_rows = [r for r in rows if r["from_currency"] == from_a]
    assert len(test_rows) >= 1
    assert test_rows[0]["rate"] == pytest.approx(0.0067)
