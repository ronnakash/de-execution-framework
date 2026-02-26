CREATE TABLE IF NOT EXISTS daily_audit (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    date                DATE NOT NULL,
    event_type          TEXT NOT NULL,
    received_count      INTEGER NOT NULL DEFAULT 0,
    processed_count     INTEGER NOT NULL DEFAULT 0,
    error_count         INTEGER NOT NULL DEFAULT 0,
    duplicate_count     INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, date, event_type)
);

CREATE INDEX IF NOT EXISTS idx_daily_audit_tenant_id ON daily_audit(tenant_id);
CREATE INDEX IF NOT EXISTS idx_daily_audit_date ON daily_audit(date);

CREATE TABLE IF NOT EXISTS file_audit (
    file_id             TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    file_name           TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    total_count         INTEGER NOT NULL DEFAULT 0,
    processed_count     INTEGER NOT NULL DEFAULT 0,
    error_count         INTEGER NOT NULL DEFAULT 0,
    duplicate_count     INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'pending',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_file_audit_tenant_id ON file_audit(tenant_id);
