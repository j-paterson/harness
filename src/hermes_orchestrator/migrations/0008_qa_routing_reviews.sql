BEGIN IMMEDIATE;

-- Durable QA origin per admitted issue (INFRA-166). The origin is recorded
-- only when the issue is admitted or explicitly designated; it is never
-- inferred from later Linear assignment changes. Unrecorded issues are
-- ordinary and project Done after a proven merge.
CREATE TABLE qa_origins (
    issue_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('ordinary', 'ryan_assigned', 'operator_designated')
    ),
    recorded_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- One durable review record per admitted candidate wake (INFRA-166).
-- review_id is derived from (project, event) so a restarted merge replays
-- against the same row; states: approved, corrections_required, merged,
-- blocked, reconciliation_required, superseded, qa_rejected.
CREATE TABLE reviews (
    review_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    reviewed_sha TEXT NOT NULL,
    state TEXT NOT NULL,
    merge_sha TEXT,
    reason TEXT,
    projection_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX reviews_issue_idx ON reviews(issue_id, created_at);

INSERT INTO schema_migrations(version, applied_at)
VALUES (8, CURRENT_TIMESTAMP);

COMMIT;
