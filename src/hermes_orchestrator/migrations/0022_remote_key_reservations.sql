BEGIN IMMEDIATE;

-- An idempotency key must identify at most one confirmed command per
-- session (INFRA-175 correction). The durable claim previously fenced only
-- confirmation_id, so two distinct pending confirmations in one session
-- could race the same key and both execute. This unique partial index
-- makes the claim UPDATE itself the atomic pre-mutation reservation:
-- pending rows hold a NULL key, claimed and executed rows retain theirs
-- forever, so a competing claim fails inside the claim transaction before
-- any executor runs. Pre-existing duplicate pairs would fail this CREATE
-- hard — intentional fail-closed; such state is corrupt and must not be
-- silently indexed around.
CREATE UNIQUE INDEX idx_remote_pending_session_key
    ON remote_pending_commands(session_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

INSERT INTO schema_migrations(version, applied_at)
VALUES (22, CURRENT_TIMESTAMP);

COMMIT;
