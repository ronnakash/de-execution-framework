ALTER TABLE clients
    DROP COLUMN IF EXISTS window_size_minutes,
    DROP COLUMN IF EXISTS window_slide_minutes,
    DROP COLUMN IF EXISTS case_aggregation_minutes;
