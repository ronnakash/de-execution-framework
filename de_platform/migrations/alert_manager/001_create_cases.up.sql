CREATE TABLE IF NOT EXISTS cases (
    case_id             TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'open',
    severity            TEXT NOT NULL DEFAULT 'medium',
    title               TEXT NOT NULL,
    description         TEXT,
    alert_count         INTEGER NOT NULL DEFAULT 0,
    first_alert_at      TIMESTAMP NOT NULL,
    last_alert_at       TIMESTAMP NOT NULL,
    algorithms          TEXT[] NOT NULL DEFAULT '{}',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cases_tenant_id ON cases(tenant_id);
CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(status);

CREATE TABLE IF NOT EXISTS case_alerts (
    case_id     TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
    alert_id    TEXT NOT NULL,
    PRIMARY KEY (case_id, alert_id)
);
