BEGIN IMMEDIATE;
-- INFRA-195: a classic lead turn that ends while background children
-- are still running must not depend on an operator prompt to resume.
-- The Stop hook records the durable continuation condition; every
-- child start/stop increments a durable counter through the
-- PreToolUse(Agent) and SubagentStop hooks; and the completion that
-- settles the last outstanding child queues exactly one reactivation
-- (a children.completed control operation, deduplicated by the
-- continuation id) so implementation resumes through the dedicated
-- channel with no terminal input.
CREATE TABLE lead_child_activity (
    session_id TEXT PRIMARY KEY,
    started INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE TABLE lead_continuations (
    continuation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    project_key TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    condition TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('waiting', 'reactivated', 'superseded')
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- One waiting continuation per session; reactivation is exactly-once
-- by compare-and-swap on this row plus the control-operation dedup.
CREATE UNIQUE INDEX lead_continuations_waiting
    ON lead_continuations(session_id)
    WHERE state = 'waiting';
INSERT INTO schema_migrations(version, applied_at)
VALUES (41, CURRENT_TIMESTAMP);
COMMIT;
