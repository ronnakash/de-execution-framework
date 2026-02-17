CREATE TABLE IF NOT EXISTS cleaned_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(255) NOT NULL,
    event_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_type, payload, event_date)
);

CREATE INDEX IF NOT EXISTS idx_cleaned_events_date ON cleaned_events (event_date);
CREATE INDEX IF NOT EXISTS idx_cleaned_events_type ON cleaned_events (event_type, event_date);
