BEGIN IMMEDIATE;

-- Durable worktree leases and cleanup state (INFRA-171). A leased
-- worktree may be reclaimed only after its state is committed as an
-- explicit WIP checkpoint, pushed, and proven reachable on the remote;
-- every lifecycle transition is journaled to the event log. A path may
-- have at most one live (non-reclaimed) lease.
CREATE TABLE worktree_leases (
    lease_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    repo_path TEXT NOT NULL,
    path TEXT NOT NULL,
    branch TEXT NOT NULL,
    remote TEXT NOT NULL,
    state TEXT NOT NULL,
    checkpoint_id TEXT,
    checkpoint_sha TEXT,
    checkpoint_message TEXT,
    checkpointed_at TEXT,
    remote_verified_at TEXT,
    reclaimed_at TEXT,
    acquired_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX worktree_leases_live_path_idx ON worktree_leases(path)
    WHERE state != 'reclaimed';
CREATE INDEX worktree_leases_state_idx ON worktree_leases(state, project_key);

INSERT INTO schema_migrations(version, applied_at)
VALUES (18, CURRENT_TIMESTAMP);

COMMIT;
