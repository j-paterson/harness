BEGIN IMMEDIATE;

-- Durable ledger of merged candidates whose CircleCI outcome is not yet
-- resolved (INFRA-165). Reconciled exactly once per eligible candidate
-- intake boundary; never polled. Each row binds the full reviewed
-- candidate identity (candidate SHA and feature branch) in addition to
-- the merge result identity.
CREATE TABLE ci_merge_ledger (
    project_key TEXT NOT NULL,
    merge_sha TEXT NOT NULL,
    repository TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    integration_branch TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    candidate_branch TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    packet_json TEXT,
    recorded_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_key, merge_sha)
);

-- One durable reconciliation claim per (project, wake event), bound to
-- the full immutable candidate identity: only the live claim owner may
-- query CircleCI for that event boundary; duplicates replay a decision
-- re-derived from committed ledger state with zero CI calls; an expired
-- lease permits exactly one atomic recovery; decision_json is an audit
-- record only.
CREATE TABLE ci_reconciliation_claims (
    project_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    identity_json TEXT NOT NULL,
    state TEXT NOT NULL,
    owner_token TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    decision_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_key, event_id)
);

INSERT INTO schema_migrations(version, applied_at)
VALUES (7, CURRENT_TIMESTAMP);

COMMIT;
