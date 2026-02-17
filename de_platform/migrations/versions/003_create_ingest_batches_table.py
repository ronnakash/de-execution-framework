"""Create the ingest_batches table for stream ingest metadata."""


def up(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS ingest_batches (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            event_count INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(filename)
        )
    """)


def down(db):
    db.execute("DROP TABLE IF EXISTS ingest_batches")
