CREATE TABLE IF NOT EXISTS clients (
    tenant_id       TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    mode            TEXT NOT NULL DEFAULT 'batch',
    algo_run_hour   INTEGER,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
