BEGIN IMMEDIATE;

-- Durable two-step remote command confirmation (INFRA-175). A prepared
-- command persists as a pending row bound to the preparing session with
-- an exact confirmation phrase and a 120-second expiry; a confirmed
-- command persists its sanitized result keyed by (idempotency_key,
-- session_id) so a repeated confirm returns the stored result without
-- re-execution while a replayed confirmation id is rejected via the
-- pending row's state. Audit events go through the events table in the
-- same transaction as these writes.
CREATE TABLE remote_pending_commands (
    confirmation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    target TEXT NOT NULL,
    confirmation_phrase TEXT NOT NULL,
    impact_summary TEXT NOT NULL,
    prepared_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'executed', 'expired'))
);

CREATE TABLE remote_command_results (
    idempotency_key TEXT NOT NULL,
    session_id TEXT NOT NULL,
    confirmation_id TEXT NOT NULL,
    code TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    state_json TEXT NOT NULL,
    executed_at TEXT NOT NULL,
    PRIMARY KEY (idempotency_key, session_id)
);

INSERT INTO schema_migrations(version, applied_at)
VALUES (20, CURRENT_TIMESTAMP);

COMMIT;
