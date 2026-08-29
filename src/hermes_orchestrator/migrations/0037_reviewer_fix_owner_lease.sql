BEGIN IMMEDIATE;
-- INFRA-194 (Sol correction 48ac5268): exactly one owner may cross or
-- reconcile a reviewer-fix publication boundary. The intent row now
-- carries a durable owner token and lease; the owner transitions the
-- row to 'attempted' before the push, so reconciliation can always
-- distinguish a definitely unattempted intent (safe to abort after
-- lease expiry) from an attempted or unknown outcome. An attempted
-- push whose outcome cannot be proven either way parks durably in
-- 'reconciliation_required', which keeps blocking further fixes and
-- never aborts. The intent also binds the complete prevalidated
-- manifest — event_id, canonical payload, and digest — before the
-- push, so finalization after any crash rewrites or verifies exactly
-- that immutable snapshot and never regenerates identity from mutable
-- state. Migration 0036 is already applied to the live database, so
-- this ships as 0037.
CREATE TABLE reviewer_fixes_v37 (
    fix_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    submitted_sha TEXT NOT NULL,
    final_sha TEXT NOT NULL,
    branch TEXT NOT NULL,
    expected_remote_sha TEXT NOT NULL,
    owner_token TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    event_id TEXT,
    manifest_json TEXT,
    manifest_digest TEXT,
    message TEXT NOT NULL,
    files_json TEXT NOT NULL,
    changed_lines INTEGER NOT NULL,
    verification_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'intended', 'attempted', 'reconciliation_required',
            'recorded', 'aborted'
        )
    ),
    reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO reviewer_fixes_v37(
    fix_id, project_key, issue_id, submitted_sha, final_sha, branch,
    expected_remote_sha, owner_token, lease_expires_at, event_id,
    manifest_json, manifest_digest, message, files_json, changed_lines,
    verification_json, state, reason, created_at, updated_at
)
SELECT fix_id, project_key, issue_id, submitted_sha, final_sha, branch,
    expected_remote_sha, '', created_at, event_id, NULL, NULL, message,
    files_json, changed_lines, verification_json, state, reason,
    created_at, updated_at
FROM reviewer_fixes;
DROP TABLE reviewer_fixes;
ALTER TABLE reviewer_fixes_v37 RENAME TO reviewer_fixes;
-- One live fix per submitted candidate; an aborted attempt may retry.
CREATE UNIQUE INDEX reviewer_fixes_live_candidate
    ON reviewer_fixes(project_key, submitted_sha)
    WHERE state != 'aborted';
INSERT INTO schema_migrations(version, applied_at)
VALUES (37, CURRENT_TIMESTAMP);
COMMIT;
