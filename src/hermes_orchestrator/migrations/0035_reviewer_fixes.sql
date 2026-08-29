BEGIN IMMEDIATE;
-- INFRA-194: auditable receipts for the bounded reviewer-fix path.
-- One eligible mechanical fix per submitted candidate: the labeled
-- ACCEPT_WITH_REVIEWER_FIX commit atop the immutable submitted SHA,
-- the final SHA it produced, the recomputed base-to-final manifest
-- event, the exact files and line volume, and the focused
-- verification that proved it.
CREATE TABLE reviewer_fixes (
    fix_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    submitted_sha TEXT NOT NULL,
    final_sha TEXT NOT NULL,
    event_id TEXT NOT NULL,
    message TEXT NOT NULL,
    files_json TEXT NOT NULL,
    changed_lines INTEGER NOT NULL,
    verification_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (project_key, submitted_sha)
);
INSERT INTO schema_migrations(version, applied_at)
VALUES (35, CURRENT_TIMESTAMP);
COMMIT;
