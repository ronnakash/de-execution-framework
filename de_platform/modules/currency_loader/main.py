"""Currency Rate Loader job module.

Reads currency exchange rates from a JSON file and upserts them into the
``currency_rates`` PostgreSQL table. This job is intended to run on a
schedule (e.g. hourly via cron) to keep rates fresh.

Input file format (JSON array)::

    [
        {"from_currency": "EUR", "to_currency": "USD", "rate": 1.08},
        {"from_currency": "GBP", "to_currency": "USD", "rate": 1.27},
        ...
    ]

Args:
    rates-file: Path to the JSON rates file (read via FileSystemInterface)
"""

from __future__ import annotations

import json

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface

_REQUIRED_FIELDS = {"from_currency", "to_currency", "rate"}


class CurrencyLoaderModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        fs: FileSystemInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.fs = fs

    def initialize(self) -> None:
        self.log = self.logger.create()
        self.rates_file: str = self.config.get("rates-file", "")

    def validate(self) -> None:
        if not self.rates_file:
            raise ValueError("rates-file is required")

    def execute(self) -> int:
        raw = self.fs.read(self.rates_file)
        rates = json.loads(raw.decode("utf-8"))

        if not isinstance(rates, list):
            raise ValueError("Rates file must contain a JSON array")

        # Validate each record
        valid_rows = []
        for i, row in enumerate(rates):
            missing = _REQUIRED_FIELDS - set(row.keys())
            if missing:
                self.log.info(
                    "Skipping incomplete rate record",
                    index=i,
                    missing=list(missing),
                )
                continue
            valid_rows.append({
                "from_currency": str(row["from_currency"]).upper(),
                "to_currency": str(row["to_currency"]).upper(),
                "rate": float(row["rate"]),
            })

        if not valid_rows:
            self.log.info("No valid rate records found", path=self.rates_file)
            return 0

        self.db.connect()
        inserted = self.db.bulk_insert("currency_rates", valid_rows)
        self.log.info(
            "Currency rates loaded",
            path=self.rates_file,
            records=len(valid_rows),
            inserted=inserted,
        )
        return 0


module_class = CurrencyLoaderModule
