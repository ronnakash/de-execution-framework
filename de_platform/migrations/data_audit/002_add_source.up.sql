-- Add source column (default 'unknown' for existing rows)
ALTER TABLE daily_audit ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';

-- Drop old unique constraint and create new one including source
ALTER TABLE daily_audit DROP CONSTRAINT IF EXISTS daily_audit_tenant_id_date_event_type_key;
ALTER TABLE daily_audit ADD CONSTRAINT daily_audit_unique
    UNIQUE (tenant_id, date, event_type, source);

-- Index on source for filtered queries
CREATE INDEX IF NOT EXISTS idx_daily_audit_source ON daily_audit(source);
