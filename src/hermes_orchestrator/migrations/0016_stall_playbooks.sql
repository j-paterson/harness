BEGIN IMMEDIATE;

-- Stall consultations (INFRA-170): one row per operator consultation for a
-- normalized reason and exact evidence predicate. Automation requires two
-- approved consultations bound to the same predicate and the exact
-- content hash of the approved playbook.
CREATE TABLE stall_consultations (
    consultation_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    predicate_key TEXT NOT NULL,
    predicate_json TEXT NOT NULL,
    state TEXT NOT NULL,
    proposed_json TEXT,
    remedy_json TEXT,
    playbook_hash TEXT,
    asked_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE INDEX stall_consultations_predicate_idx
    ON stall_consultations(predicate_key, state);

-- Every approved playbook file content hash, versioned; only an explicit
-- operator approval writes the file and records its hash here.
CREATE TABLE playbook_versions (
    content_hash TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    path TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

-- Automatic and consulted remedy executions with their verified outcome; a
-- failed or timed-out automatic remedy revokes automation for its predicate
-- until the operator is consulted again.
CREATE TABLE stall_executions (
    execution_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    predicate_key TEXT NOT NULL,
    playbook_hash TEXT,
    mode TEXT NOT NULL,
    success INTEGER NOT NULL CHECK (success IN (0, 1)),
    detail TEXT,
    executed_at TEXT NOT NULL
);

INSERT INTO schema_migrations(version, applied_at)
VALUES (16, CURRENT_TIMESTAMP);

COMMIT;
