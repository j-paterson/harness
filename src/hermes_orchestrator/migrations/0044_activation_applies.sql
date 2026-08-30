BEGIN IMMEDIATE;
-- INFRA-195 (Sol correction 032cd4a5): switching the supervised
-- daemon to a new runtime is a journaled protocol, never a bare
-- ledger update. The apply row records intent before any process
-- changes, each boundary (activated, restarted) as it is crossed,
-- and the outcome: 'verified' only after the freshly supervised
-- process durably reported the exact target generation; 'rolled_back'
-- only after the prior activation was reinstated, the process
-- restarted, and its health verified the same way; anything the
-- protocol cannot prove is 'ambiguous' and fails closed.
CREATE TABLE activation_applies (
    apply_id TEXT PRIMARY KEY,
    target_checkout TEXT NOT NULL,
    prior_generation INTEGER,
    target_generation INTEGER,
    state TEXT NOT NULL CHECK (
        state IN (
            'intended', 'activated', 'restarted', 'verified',
            'rolled_back', 'ambiguous', 'refused'
        )
    ),
    reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO schema_migrations(version, applied_at)
VALUES (44, CURRENT_TIMESTAMP);
COMMIT;
