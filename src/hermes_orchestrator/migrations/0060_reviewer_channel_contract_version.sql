BEGIN IMMEDIATE;
-- INFRA-200 (versioned forward-implementation-first contract delivery):
-- every project-scoped Sol Merger thread must durably receive the
-- versioned forward-implementation-first review contract exactly once
-- per (thread_id, generation) identity -- at launch for a brand-new
-- thread, or prepended to its next real candidate intake message for
-- an existing thread that predates this feature (or whose launch-time
-- delivery failed). This column is that durable receipt: NULL means
-- the contract has never been delivered to the exact live thread
-- generation; the delivered version string ("fif-1" at introduction)
-- means it has. Written by CodexMerger.record_contract_delivered under
-- the same compare-and-swap discipline as every other per-generation
-- delivery fact on this table, and reset to NULL by
-- complete_replacement so a replacement thread re-adopts the contract
-- at its own next real intake instead of inheriting the prior thread's
-- delivery receipt.
ALTER TABLE reviewer_channels ADD COLUMN contract_version TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (60, CURRENT_TIMESTAMP);
COMMIT;
