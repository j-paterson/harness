BEGIN IMMEDIATE;
-- INFRA-194: Sol merges become durable settlements. A settlement row
-- binds the strict approved verdict to the project, issue,
-- repository, branch, pull request, base, candidate SHA, wake event,
-- reviewer thread and generation, and manifest version BEFORE any
-- GitHub mutation, and its owner-token lease is the exclusive
-- journaled merge claim. States advance only by compare-and-set:
-- recorded (verdict persisted, no mutation yet) -> merging (claimed,
-- crossing the GitHub boundary) -> merged (mutation proven) ->
-- settled (window journaled, review transitioned, Linear projected).
-- 'failed' is terminal with a reason. 'path' records whether
-- completion followed the normal guarded path or externally-merged
-- reconciliation. A crash at any boundary leaves a resumable row;
-- the deterministic effect ids downstream make every replay
-- exactly-once effective.
CREATE TABLE merge_settlements (
    settlement_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    base_sha TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    thread_generation INTEGER NOT NULL,
    manifest_version INTEGER NOT NULL,
    path TEXT NOT NULL CHECK (
        path IN ('guarded', 'externally_merged')
    ),
    state TEXT NOT NULL CHECK (
        state IN ('recorded', 'merging', 'merged', 'settled', 'failed')
    ),
    merge_sha TEXT,
    reason TEXT,
    owner_token TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (project_key, event_id)
);
CREATE INDEX merge_settlements_resume_idx
    ON merge_settlements(state, project_key);
INSERT INTO schema_migrations(version, applied_at)
VALUES (34, CURRENT_TIMESTAMP);
COMMIT;
