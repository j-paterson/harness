BEGIN IMMEDIATE;
-- INFRA-197 (Sol correction b4b545f3, packet 2): a rotation that crashed
-- after acknowledgement but before the transfer used to reselect capacity
-- and rerun the replacement's acknowledgement turn, because only the
-- replacement session — never the selected profile — was durable on the
-- handoffs row. Recovery must reconstruct the exact identities selected
-- before acknowledgement, so the selected profile becomes durable in the
-- same row the acknowledgement transition already writes. NULL means the
-- row predates this migration (or is not yet acknowledged). Additive
-- only: one nullable column on handoffs, no other table is altered.
ALTER TABLE handoffs ADD COLUMN replacement_profile_alias TEXT;
INSERT INTO schema_migrations(version, applied_at)
VALUES (51, CURRENT_TIMESTAMP);
COMMIT;
