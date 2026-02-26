CREATE TABLE IF NOT EXISTS currency_rates (
    from_currency VARCHAR(3) NOT NULL,
    to_currency   VARCHAR(3) NOT NULL,
    rate          DOUBLE PRECISION NOT NULL,
    effective_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (from_currency, to_currency)
);
