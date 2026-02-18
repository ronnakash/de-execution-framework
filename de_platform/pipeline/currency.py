"""Shared currency conversion logic.

``CurrencyConverter`` checks a Redis cache first, falls back to a PostgreSQL
``currency_rates`` table, and caches the result for subsequent lookups.

Usage::

    converter = CurrencyConverter(db=my_db, cache=my_cache)
    rate = converter.get_rate("EUR", "USD")   # -> float
    amount_usd = converter.convert(1000.0, "EUR")
"""

from __future__ import annotations

from de_platform.services.cache.interface import CacheInterface
from de_platform.services.database.interface import DatabaseInterface

_CACHE_TTL = 3600  # seconds — refresh rates every hour
_CACHE_PREFIX = "currency_rate:"


class CurrencyConverter:
    """Currency exchange rate lookup with Redis caching and PostgreSQL fallback."""

    def __init__(self, db: DatabaseInterface, cache: CacheInterface) -> None:
        self.db = db
        self.cache = cache

    def get_rate(self, from_currency: str, to_currency: str = "USD") -> float:
        """Return the exchange rate from *from_currency* to *to_currency*.

        Lookup order:
        1. Redis cache (key ``currency_rate:{FROM}_{TO}``)
        2. PostgreSQL ``currency_rates`` table
        3. If not found, raises ``KeyError``
        """
        if from_currency == to_currency:
            return 1.0

        cache_key = f"{_CACHE_PREFIX}{from_currency}_{to_currency}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return float(cached)

        row = self.db.fetch_one(
            "SELECT rate FROM currency_rates WHERE from_currency = $1 AND to_currency = $2",
            [from_currency, to_currency],
        )
        if row is None:
            raise KeyError(
                f"No exchange rate found for {from_currency} -> {to_currency}"
            )

        rate = float(row["rate"])
        self.cache.set(cache_key, rate, ttl=_CACHE_TTL)
        return rate

    def convert(self, amount: float, from_currency: str, to_currency: str = "USD") -> float:
        """Convert *amount* from *from_currency* to *to_currency*."""
        return amount * self.get_rate(from_currency, to_currency)
