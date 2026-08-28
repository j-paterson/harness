BEGIN IMMEDIATE;

-- Durable stop ownership for process leases (INFRA-167 correction). A
-- stop is a claimed, checkpoint-backed state machine: the claim is
-- journaled before any signal, exactly one owner token may advance each
-- phase (term_claimed -> term_sent -> kill_claimed -> kill_sent), and a
-- stale claim can be recovered only after its lease expires.
ALTER TABLE process_leases ADD COLUMN stop_owner TEXT;
ALTER TABLE process_leases ADD COLUMN stop_phase TEXT;
ALTER TABLE process_leases ADD COLUMN stop_claim_expires_at TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (12, CURRENT_TIMESTAMP);

COMMIT;
