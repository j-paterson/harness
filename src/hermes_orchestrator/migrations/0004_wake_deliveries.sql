BEGIN IMMEDIATE;

CREATE TABLE wake_deliveries (
    project_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    branch TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    manifest_device INTEGER NOT NULL,
    manifest_inode INTEGER NOT NULL,
    manifest_size INTEGER NOT NULL,
    manifest_mtime_ns INTEGER NOT NULL,
    manifest_mode INTEGER NOT NULL,
    state TEXT NOT NULL,
    thread_id TEXT,
    generation INTEGER,
    claim_token TEXT,
    claim_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_key, event_id)
);

CREATE UNIQUE INDEX wake_deliveries_candidate_unique
    ON wake_deliveries(project_key, status, candidate_sha);

INSERT INTO schema_migrations(version, applied_at)
VALUES (4, CURRENT_TIMESTAMP);

COMMIT;
