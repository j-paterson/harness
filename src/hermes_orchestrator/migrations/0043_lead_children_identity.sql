BEGIN IMMEDIATE;
-- INFRA-195 (Sol correction 032cd4a5): aggregate start/completion
-- counters cannot make child accounting exactly-once — a duplicated
-- or replayed SubagentStop could satisfy completed >= started and
-- reactivate a lead while a distinct child still runs. Child activity
-- is now keyed by the stable hook-provided child identity: a start
-- inserts exactly once per (session, child), a completion is a
-- compare-and-swap on that exact identity (duplicates, reordered
-- deliveries, foreign sessions, and identity-less events are refused
-- without advancing progress), and reactivation fires only when the
-- distinct outstanding set is empty.
CREATE TABLE lead_children (
    session_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started', 'completed')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (session_id, child_id)
);
DROP TABLE lead_child_activity;
INSERT INTO schema_migrations(version, applied_at)
VALUES (43, CURRENT_TIMESTAMP);
COMMIT;
