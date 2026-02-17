CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_created ON events (created_at);
