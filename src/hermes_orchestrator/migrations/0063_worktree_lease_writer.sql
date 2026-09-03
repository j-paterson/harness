BEGIN IMMEDIATE;

-- INFRA-222 (Fable -> Sol issue-worktree lease transfer): the immutable
-- review artifact is the recorded candidate Git SHA; the worktree
-- directory is an operational resource whose EXCLUSIVE writer may
-- change over the lease's life. Fable commits/pushes, leaves the
-- worktree clean, and releases its writer role; Hermes atomically
-- transfers the lease to Sol's bound generation once the observed HEAD
-- matches the recorded submitted candidate SHA; for REWORK_REQUIRED
-- Sol makes no edits and the unchanged lease transfers back to Fable.
-- Never two writers at once. writer_generation increments on every
-- transfer so a compare-and-swap on (lease_id, writer_role,
-- writer_generation) makes a concurrent transfer lose instead of
-- silently clobbering the other. The writer role set ('fable', 'sol')
-- is enforced in code as WRITER_ROLES because SQLite's ALTER TABLE ADD
-- COLUMN cannot cheaply add a CHECK constraint alongside a NOT NULL
-- default across the SQLite versions this project supports.
ALTER TABLE worktree_leases ADD COLUMN writer_role TEXT NOT NULL DEFAULT 'fable';
ALTER TABLE worktree_leases ADD COLUMN writer_ref TEXT;
ALTER TABLE worktree_leases ADD COLUMN writer_generation INTEGER NOT NULL DEFAULT 1;
ALTER TABLE worktree_leases ADD COLUMN submitted_candidate_sha TEXT;
ALTER TABLE worktree_leases ADD COLUMN transferred_at TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (63, CURRENT_TIMESTAMP);

COMMIT;
