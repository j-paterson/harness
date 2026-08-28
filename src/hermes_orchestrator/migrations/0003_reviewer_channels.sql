BEGIN IMMEDIATE;

CREATE TABLE reviewer_channels (
    project_key TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    integration_branch TEXT NOT NULL DEFAULT '',
    prior_thread_id TEXT,
    replacement_reason TEXT,
    last_delivered_event_id TEXT,
    last_delivered_candidate_sha TEXT,
    last_delivery_failure_at TEXT,
    heartbeat_enabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT INTO schema_migrations(version, applied_at)
VALUES (3, CURRENT_TIMESTAMP);

COMMIT;
