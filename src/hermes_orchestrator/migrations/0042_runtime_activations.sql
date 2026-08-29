BEGIN IMMEDIATE;
-- INFRA-195: which binary, checkout, git SHA, and schema the durable
-- orchestrator runs under used to live only in out-of-repo shell
-- resolution. Every activation is now a durable record: exactly one
-- row is 'active' at a time, a deliberate or startup activation
-- validates the checkout (clean tree, migration max equal to the live
-- database schema) before superseding the prior row, and a failed
-- activation is recorded 'failed' while the prior activation stays
-- active — that is the safe rollback. Generations are monotonic and
-- unique across every attempt.
CREATE TABLE runtime_activations (
    activation_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    generation INTEGER NOT NULL UNIQUE,
    binary_path TEXT NOT NULL,
    checkout_root TEXT NOT NULL,
    git_sha TEXT NOT NULL,
    database_schema INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('active', 'superseded', 'failed')
    ),
    reason TEXT,
    activated_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- At most one active runtime identity.
CREATE UNIQUE INDEX runtime_activations_one_active
    ON runtime_activations(state)
    WHERE state = 'active';
INSERT INTO schema_migrations(version, applied_at)
VALUES (42, CURRENT_TIMESTAMP);
COMMIT;
