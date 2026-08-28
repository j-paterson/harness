BEGIN IMMEDIATE;

-- Explicit durable binding between a failed merged candidate and the
-- authorized rework that corrected it (INFRA-166 correction). Populated
-- only when a validated, admitted FABLE_REWORK_READY candidate bound to the
-- failure's correction packet closes it; records the event, rework SHA,
-- and correction id so replays are idempotent and auditable.
ALTER TABLE ci_merge_ledger ADD COLUMN correction_json TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (10, CURRENT_TIMESTAMP);

COMMIT;
