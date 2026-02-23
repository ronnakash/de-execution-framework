DROP INDEX IF EXISTS idx_daily_audit_source;
ALTER TABLE daily_audit DROP CONSTRAINT IF EXISTS daily_audit_unique;
ALTER TABLE daily_audit DROP COLUMN IF EXISTS source;
ALTER TABLE daily_audit ADD CONSTRAINT daily_audit_tenant_id_date_event_type_key
    UNIQUE (tenant_id, date, event_type);
