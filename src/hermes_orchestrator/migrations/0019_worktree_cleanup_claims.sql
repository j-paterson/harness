BEGIN IMMEDIATE;

-- Durable remote-proof identity and atomic cleanup claims (INFRA-171
-- correction e17c9a84). Remote verification records the exact remote,
-- branch, SHA, and checkpoint it proved at an authoritative timestamp;
-- reclaim authorizes only against this record, never against
-- caller-supplied proof fields. A reclaim first claims the lease in a
-- 'reclaiming' state so no new worker or process may attach to the
-- path; the claim carries its owner token and claim time so a crashed
-- cleanup is reconcilable instead of stuck.
ALTER TABLE worktree_leases ADD COLUMN verified_remote TEXT;
ALTER TABLE worktree_leases ADD COLUMN verified_branch TEXT;
ALTER TABLE worktree_leases ADD COLUMN verified_sha TEXT;
ALTER TABLE worktree_leases ADD COLUMN verified_checkpoint_id TEXT;
ALTER TABLE worktree_leases ADD COLUMN cleanup_owner TEXT;
ALTER TABLE worktree_leases ADD COLUMN cleanup_claimed_at TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (19, CURRENT_TIMESTAMP);

COMMIT;
