BEGIN IMMEDIATE;
-- INFRA-194 (Sol correction 2e1ddcce): a reviewer fix crosses the
-- remote mutation boundary only behind a durable publication intent.
-- Every locally provable gate runs before the push; the intent row is
-- journaled before it; the push itself is an exact expected-remote-head
-- compare-and-push; and the receipt/manifest finalize only after the
-- push proves out. A crash or uncertain push leaves one 'intended' row
-- that blocks further fixes until deterministic reconciliation reads
-- the remote and records exactly one receipt for the exact final SHA
-- ('recorded') or proves nothing landed ('aborted', retryable).
CREATE TABLE reviewer_fixes_v36 (
    fix_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    submitted_sha TEXT NOT NULL,
    final_sha TEXT NOT NULL,
    branch TEXT NOT NULL,
    expected_remote_sha TEXT NOT NULL,
    event_id TEXT,
    message TEXT NOT NULL,
    files_json TEXT NOT NULL,
    changed_lines INTEGER NOT NULL,
    verification_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('intended', 'recorded', 'aborted')
    ),
    reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO reviewer_fixes_v36(
    fix_id, project_key, issue_id, submitted_sha, final_sha, branch,
    expected_remote_sha, event_id, message, files_json, changed_lines,
    verification_json, state, created_at, updated_at
)
SELECT fix_id, project_key, issue_id, submitted_sha, final_sha, '',
    submitted_sha, event_id, message, files_json, changed_lines,
    verification_json, 'recorded', created_at, created_at
FROM reviewer_fixes;
DROP TABLE reviewer_fixes;
ALTER TABLE reviewer_fixes_v36 RENAME TO reviewer_fixes;
-- One live fix per submitted candidate; an aborted attempt may retry.
CREATE UNIQUE INDEX reviewer_fixes_live_candidate
    ON reviewer_fixes(project_key, submitted_sha)
    WHERE state != 'aborted';
INSERT INTO schema_migrations(version, applied_at)
VALUES (36, CURRENT_TIMESTAMP);
COMMIT;
