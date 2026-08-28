BEGIN IMMEDIATE;

-- Per-attempt execution lease for GitHub merge mutations (INFRA-164).
-- Exactly one live claim may send the mutation for an effect; only the
-- exact current token may complete or alter the row, and an expired lease
-- may be reclaimed once after a crash or restart.
ALTER TABLE github_merge_effects ADD COLUMN claim_token TEXT;
ALTER TABLE github_merge_effects ADD COLUMN claim_expires_at TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (6, CURRENT_TIMESTAMP);

COMMIT;
