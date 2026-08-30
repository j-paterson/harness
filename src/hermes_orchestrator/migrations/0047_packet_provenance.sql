BEGIN IMMEDIATE;
-- INFRA-194 (Sol reviewer, PR #30): the ledger recorded delegation
-- packets but carried no measured binding to the work actually
-- performed — a packet could be created, reserved, settled, and
-- accepted after the claimed changes already existed. Reservation now
-- carries a measured pre-work snapshot (sha256 of each allowed file's
-- raw bytes, or the sentinel "absent" for a missing file) and a
-- completed settlement carries the matching measured returned
-- snapshot, so the ledger's own rows attest to what changed, not just
-- what was claimed. Both columns are nullable: legacy rows accepted
-- before this migration are permanently without provenance, and a
-- failed/capped settlement tolerates a NULL returned snapshot.
ALTER TABLE subagent_packets ADD COLUMN reserved_blobs_json TEXT;
ALTER TABLE subagent_packets ADD COLUMN returned_blobs_json TEXT;
INSERT INTO schema_migrations(version, applied_at)
VALUES (47, CURRENT_TIMESTAMP);
COMMIT;
