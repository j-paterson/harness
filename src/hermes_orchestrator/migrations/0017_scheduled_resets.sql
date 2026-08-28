BEGIN IMMEDIATE;

-- Durable, restart-safe scheduled work for the wait_for_reset remedy
-- (INFRA-170 correction): a scheduled reset survives process restarts and
-- is consumed by the supervisor once due, with the consumption journaled.
CREATE TABLE scheduled_resets (
    reset_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    predicate_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    due_at TEXT NOT NULL,
    state TEXT NOT NULL,
    consumed_at TEXT
);

CREATE INDEX scheduled_resets_due_idx ON scheduled_resets(state, due_at);

INSERT INTO schema_migrations(version, applied_at)
VALUES (17, CURRENT_TIMESTAMP);

COMMIT;
