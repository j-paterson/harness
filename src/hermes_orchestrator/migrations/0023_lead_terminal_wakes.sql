BEGIN IMMEDIATE;

-- INFRA-181: durable outbox of Claude lead terminal wakes. One row is the
-- single authoritative record that a lead turn reached a terminal boundary
-- (completed, provider capped, blocked, or handoff required). Rows hold
-- orchestration metadata only — never prompts or responses — and are
-- deduplicated by turn identity so a crash between persistence and
-- delivery replays without duplicate orchestration effects.
CREATE TABLE lead_terminal_wakes (
    wake_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    profile_alias TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    reset_at TEXT,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT
);

CREATE UNIQUE INDEX lead_terminal_wakes_turn_unique
    ON lead_terminal_wakes(project_key, cell_id, session_id, turn_key, kind);

CREATE INDEX lead_terminal_wakes_pending
    ON lead_terminal_wakes(state, project_key);

INSERT INTO schema_migrations(version, applied_at)
VALUES (23, CURRENT_TIMESTAMP);

COMMIT;
