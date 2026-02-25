-- Raw received counts from audit_counts topic (published by starters)
CREATE TABLE IF NOT EXISTS audit_received (
    id                SERIAL PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    ingestion_method  TEXT NOT NULL,
    count             INTEGER NOT NULL,
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_received_tenant ON audit_received(tenant_id);

-- File upload records from audit_file_uploads topic
CREATE TABLE IF NOT EXISTS audit_file_uploads (
    id                SERIAL PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    file_name         TEXT NOT NULL,
    event_count       INTEGER NOT NULL,
    uploaded_at       TIMESTAMP NOT NULL,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_file_uploads_tenant ON audit_file_uploads(tenant_id);

-- Calculated audit summaries (written by audit_calculator job)
CREATE TABLE IF NOT EXISTS audit_daily (
    id                SERIAL PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    date              DATE NOT NULL,
    event_type        TEXT NOT NULL,
    ingestion_method  TEXT NOT NULL,
    received_count    INTEGER DEFAULT 0,
    processed_count   INTEGER DEFAULT 0,
    error_count       INTEGER DEFAULT 0,
    duplicate_count   INTEGER DEFAULT 0,
    calculated_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (tenant_id, date, event_type, ingestion_method)
);

-- High watermark for incremental calculation
CREATE TABLE IF NOT EXISTS audit_watermarks (
    tenant_id         TEXT NOT NULL,
    metric            TEXT NOT NULL,
    high_watermark    TIMESTAMP NOT NULL,
    PRIMARY KEY (tenant_id, metric)
);
