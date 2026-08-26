BEGIN IMMEDIATE;

CREATE TABLE handoff_requests (
    request_id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    requested_at TEXT NOT NULL
);

CREATE TABLE handoffs (
    handoff_id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    state TEXT NOT NULL,
    document_json TEXT NOT NULL,
    markdown TEXT NOT NULL,
    replacement_session_id TEXT,
    restated_next_action TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX handoffs_cell_idx ON handoffs(cell_id, created_at);

INSERT INTO schema_migrations(version, applied_at)
VALUES (2, CURRENT_TIMESTAMP);

COMMIT;
