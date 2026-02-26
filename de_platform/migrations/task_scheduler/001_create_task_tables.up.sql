CREATE TABLE IF NOT EXISTS task_definitions (
    task_id         TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    module_name     TEXT NOT NULL,
    default_args    JSONB NOT NULL DEFAULT '{}',
    schedule_cron   TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    arg_generator   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id          TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES task_definitions(task_id),
    status          TEXT NOT NULL DEFAULT 'pending',
    args            JSONB NOT NULL DEFAULT '{}',
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    exit_code       INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_runs_task_id ON task_runs(task_id);
CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status);
