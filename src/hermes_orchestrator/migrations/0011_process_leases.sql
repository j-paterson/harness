BEGIN IMMEDIATE;

-- Exact managed process-group leases (INFRA-167). Every launched lead,
-- App Server, queue CLI, subagent, test process, server, or sidecar is
-- recorded here with its pid, process group, and psutil create_time so
-- termination targets only the recorded group after validating identity.
-- Every signal and result is persisted.
CREATE TABLE process_leases (
    lease_id TEXT PRIMARY KEY,
    worker_id TEXT,
    project_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    pid INTEGER NOT NULL,
    pgid INTEGER NOT NULL,
    executable TEXT,
    cwd TEXT,
    create_time REAL NOT NULL,
    state TEXT NOT NULL,
    last_signal INTEGER,
    stop_checkpoint_id TEXT,
    stop_reason TEXT,
    exit_code INTEGER,
    acquired_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX process_leases_active_idx ON process_leases(state, project_key);

INSERT INTO schema_migrations(version, applied_at)
VALUES (11, CURRENT_TIMESTAMP);

COMMIT;
