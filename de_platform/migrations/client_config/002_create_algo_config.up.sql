CREATE TABLE IF NOT EXISTS client_algo_config (
    tenant_id       TEXT NOT NULL REFERENCES clients(tenant_id) ON DELETE CASCADE,
    algorithm       TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    thresholds      JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, algorithm)
);
