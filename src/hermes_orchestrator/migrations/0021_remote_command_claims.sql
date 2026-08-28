BEGIN IMMEDIATE;

-- Exactly-once execution fence for remote confirmations (INFRA-175
-- correction). confirm() now claims a pending row durably before crossing
-- the mutation boundary, so the state machine gains 'claimed' plus the
-- claim's binding columns (claim time and idempotency key), and prepared
-- commands persist their validated intent-specific parameters. sqlite
-- cannot alter the CHECK constraint 0020 baked into
-- remote_pending_commands, so the table is recreated and existing rows
-- are copied forward unchanged.
CREATE TABLE remote_pending_commands_new (
    confirmation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    target TEXT NOT NULL,
    confirmation_phrase TEXT NOT NULL,
    impact_summary TEXT NOT NULL,
    prepared_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'claimed', 'executed', 'expired')),
    claimed_at TEXT,
    idempotency_key TEXT,
    parameters_json TEXT NOT NULL DEFAULT '{}'
);

INSERT INTO remote_pending_commands_new(
    confirmation_id, session_id, intent, target, confirmation_phrase,
    impact_summary, prepared_at, expires_at, state
)
SELECT confirmation_id, session_id, intent, target, confirmation_phrase,
    impact_summary, prepared_at, expires_at, state
FROM remote_pending_commands;

DROP TABLE remote_pending_commands;

ALTER TABLE remote_pending_commands_new RENAME TO remote_pending_commands;

INSERT INTO schema_migrations(version, applied_at)
VALUES (21, CURRENT_TIMESTAMP);

COMMIT;
