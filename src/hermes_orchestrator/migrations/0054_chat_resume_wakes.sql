BEGIN IMMEDIATE;
-- INFRA-216 (packet W2): after an ACTUAL recovery of the Orchestrator
-- workspace's durable Hermes chat pane (the daemon rebuilt the workspace
-- under a successor binding generation, or respawned the lower pane's
-- process in place), the reattached session's persisted `/goal` row
-- survives but nothing resumes it: the chat idles with an active goal.
-- One durable claim row per (binding_id, generation) makes the resume
-- wake exactly-once: an insert-or-ignore claim, a delivery attempt, and a
-- CAS settle (`state` 'pending' -> 'delivered', `delivered_at` set
-- exactly once). A crash between claim and settle leaves the row
-- 'pending' for the next daemon pass to retry; a settled row is never
-- redelivered — the unique index is the sole dedup authority, mirroring
-- ``lead_terminal_wakes`` (0023) and ``fakechat_signal_ports`` (0048).
CREATE TABLE chat_resume_wakes (
    wake_id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    workspace_uuid TEXT NOT NULL,
    session_name TEXT NOT NULL,
    trigger TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivered')),
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    last_attempt_at TEXT,
    last_error TEXT
);
CREATE UNIQUE INDEX chat_resume_wakes_claim_unique
    ON chat_resume_wakes(binding_id, generation);
CREATE INDEX chat_resume_wakes_pending
    ON chat_resume_wakes(state);
INSERT INTO schema_migrations(version, applied_at)
VALUES (54, CURRENT_TIMESTAMP);
COMMIT;
