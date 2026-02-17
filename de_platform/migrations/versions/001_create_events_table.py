"""Create the events table for raw event storage."""

from de_platform.services.database.interface import DatabaseInterface


def up(db: DatabaseInterface) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY,
            event_type VARCHAR(255) NOT NULL,
            payload JSONB NOT NULL,
            source VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events (created_at)")


def down(db: DatabaseInterface) -> None:
    db.execute("DROP TABLE IF EXISTS events CASCADE")
