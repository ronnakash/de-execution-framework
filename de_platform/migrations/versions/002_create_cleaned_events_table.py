"""Create the cleaned_events table for Batch ETL output."""

from de_platform.services.database.interface import DatabaseInterface


def up(db: DatabaseInterface) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS cleaned_events (
            id BIGSERIAL PRIMARY KEY,
            event_type VARCHAR(255) NOT NULL,
            payload JSONB NOT NULL,
            source VARCHAR(255) NOT NULL,
            event_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (event_type, payload, event_date)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_cleaned_events_date ON cleaned_events (event_date)")
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_cleaned_events_type "
        "ON cleaned_events (event_type, event_date)"
    )


def down(db: DatabaseInterface) -> None:
    db.execute("DROP TABLE IF EXISTS cleaned_events CASCADE")
