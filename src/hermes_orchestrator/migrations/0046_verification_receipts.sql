BEGIN IMMEDIATE;
-- INFRA-186 v2 delta: trusted, content-addressed verification receipts.
-- A receipt attests that THIS Hermes-owned runner executed a specific
-- command against a specific tree (including any uncommitted diff),
-- with specific dependencies and a specific runner/environment, and
-- observed a specific exit code, test counts, duration, and output.
-- The receipt_id is the sha256 of that canonical binding document, so
-- an identical run always yields the identical row (INSERT OR REPLACE
-- is forbidden here — a receipt_id collision means byte-identical
-- content, never a legitimate overwrite). The signature is an
-- hmac-sha256 over the same canonical document, keyed by a secret this
-- runner alone holds; a model can request that a command be run, but
-- it can never author, forge, or backdate a receipt that validates,
-- because it never holds the signing key and never controls the
-- runner's own measurements of the tree, dependencies, or environment.
-- Receipts are reused while fresh and invalidated the instant any
-- bound fact changes (tree, dirty patch, dependencies, command,
-- runner fingerprint) or the run itself failed.
CREATE TABLE verification_receipts (
    receipt_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    gate_id TEXT NOT NULL,
    command TEXT NOT NULL,
    tree_hash TEXT NOT NULL,
    dirty_patch_hash TEXT NOT NULL,
    dependency_hash TEXT NOT NULL,
    runner_fingerprint TEXT NOT NULL,
    exit_code INTEGER NOT NULL,
    test_counts TEXT NOT NULL,
    duration_seconds REAL NOT NULL,
    output_hash TEXT NOT NULL,
    signature TEXT NOT NULL,
    created_at TEXT NOT NULL
);
-- Reuse and staleness checks both key on "receipts for this gate at
-- this tree state" — the runner's freshest-match lookup walks exactly
-- that shape.
CREATE INDEX verification_receipts_gate_tree
    ON verification_receipts(gate_id, tree_hash);
INSERT INTO schema_migrations(version, applied_at)
VALUES (46, CURRENT_TIMESTAMP);
COMMIT;
