CREATE TABLE IF NOT EXISTS tenants (
    tenant_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
