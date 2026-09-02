BEGIN IMMEDIATE;
-- INFRA-187 (Sol model identity persisted and validated): the merge
-- lead model is explicitly gpt-5.6-sol, authenticated through the
-- ChatGPT provider boundary, and that identity must be validated at
-- creation and at every recovery -- a mismatched replacement fails
-- closed rather than silently continuing under a different model.
-- This lands ONLY the durable columns: every existing channel row
-- gets NULL model/provider/model_verified_at, which is the honest
-- "unproven" state for a channel created before this migration --
-- CodexMerger._resume_current performs the explicit reconciliation
-- (proof) for such a legacy channel the next time it is resumed,
-- rather than this migration guessing or backfilling a value it
-- cannot verify.
ALTER TABLE reviewer_channels ADD COLUMN model TEXT;
ALTER TABLE reviewer_channels ADD COLUMN provider TEXT;
ALTER TABLE reviewer_channels ADD COLUMN model_verified_at TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (59, CURRENT_TIMESTAMP);
COMMIT;
