-- Compound index for the most common case aggregation lookup:
-- open cases for a specific tenant
CREATE INDEX IF NOT EXISTS idx_cases_tenant_status ON cases(tenant_id, status);

-- Index on last_alert_at for aggregation window cutoff filtering
CREATE INDEX IF NOT EXISTS idx_cases_last_alert_at ON cases(last_alert_at);

-- Index for cross-algorithm grouping: find alerts by event_id within a tenant
CREATE INDEX IF NOT EXISTS idx_alerts_tenant_event ON alerts(tenant_id, event_id);

-- Compound index for REST API filtering: alerts by tenant + algorithm
CREATE INDEX IF NOT EXISTS idx_alerts_tenant_algorithm ON alerts(tenant_id, algorithm);

-- Index for case_alerts lookups by alert_id (reverse lookup: alert -> case)
CREATE INDEX IF NOT EXISTS idx_case_alerts_alert_id ON case_alerts(alert_id);
