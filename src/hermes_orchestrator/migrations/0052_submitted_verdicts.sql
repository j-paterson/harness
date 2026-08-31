BEGIN IMMEDIATE;
-- INFRA-197 (operator correction ec1f6bdf): explicit Sol submit-review.
-- Sol submits the final structured verdict explicitly instead of the
-- orchestrator inferring it from turn-completed/idle observation. Each
-- bound submission is persisted exactly once before settlement runs:
-- event_id is the compare-and-set key (one verdict per wake event), the
-- stored bindings and verdict bytes decide whether a duplicate is
-- idempotent (identical: return the recorded result) or conflicting
-- (different: fail closed), and state tracks whether the idempotent
-- settlement path has completed for the row. result_json records the
-- settled outcome so an identical duplicate returns the same result
-- without re-entering settlement. Observation never inserts here; it
-- only resumes a row left 'submitted' by an interrupted submission.
-- Additive only: one new table, no existing table is altered.
CREATE TABLE submitted_verdicts (
    event_id TEXT PRIMARY KEY NOT NULL,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    reviewed_thread_id TEXT NOT NULL,
    reviewed_generation INTEGER NOT NULL,
    verdict_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('submitted', 'settled')),
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO schema_migrations(version, applied_at)
VALUES (52, CURRENT_TIMESTAMP);
COMMIT;
