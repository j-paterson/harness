BEGIN IMMEDIATE;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    correlation_id TEXT,
    actor TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX events_aggregate_idx
    ON events(aggregate_type, aggregate_id, sequence);

CREATE TABLE admitted_issues (
    issue_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    priority INTEGER NOT NULL,
    state TEXT NOT NULL,
    instruction_id TEXT UNIQUE,
    dependency_ready INTEGER NOT NULL DEFAULT 1 CHECK (dependency_ready IN (0, 1)),
    overlap_risk INTEGER NOT NULL DEFAULT 0,
    admitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE project_cells (
    cell_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    state TEXT NOT NULL,
    profile_alias TEXT,
    session_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX one_active_cell_per_project
    ON project_cells(project_key)
    WHERE state IN ('starting', 'active', 'handoff_required', 'paused');

CREATE TABLE worker_leases (
    lease_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    project_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    pid INTEGER,
    process_create_time REAL,
    state TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE TABLE profile_leases (
    profile_alias TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    state TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    cooldown_until TEXT
);

CREATE TABLE external_effects (
    effect_id TEXT PRIMARY KEY,
    adapter TEXT NOT NULL,
    operation TEXT NOT NULL,
    target TEXT NOT NULL,
    state TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE resource_samples (
    sample_id TEXT PRIMARY KEY,
    sampled_at TEXT NOT NULL,
    pressure TEXT NOT NULL,
    available_memory_bytes INTEGER NOT NULL,
    total_memory_bytes INTEGER NOT NULL,
    swap_used_bytes INTEGER NOT NULL,
    load_one REAL NOT NULL,
    logical_cpus INTEGER NOT NULL,
    disk_json TEXT NOT NULL,
    managed_rss_bytes INTEGER NOT NULL
);

CREATE INDEX resource_samples_sampled_at_idx ON resource_samples(sampled_at);

CREATE TABLE reconciliation_runs (
    run_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    findings_json TEXT NOT NULL DEFAULT '[]'
);

INSERT INTO schema_migrations(version, applied_at)
VALUES (1, CURRENT_TIMESTAMP);

COMMIT;
