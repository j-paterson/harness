BEGIN IMMEDIATE;

-- Dedicated GitHub merge-mutation journal (INFRA-164). Kept separate from
-- external_effects so the canonical one-owner-per-candidate invariant cannot
-- interact with Linear projection effects.
CREATE TABLE github_merge_effects (
    effect_id TEXT PRIMARY KEY,
    repository TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    head_sha TEXT NOT NULL,
    head_ref TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    merge_method TEXT NOT NULL,
    state TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- One mutation owner per canonical merge candidate across connections and
-- restarts: a competing effect id for the same repository/PR/exact head can
-- never journal a second mutation intent.
CREATE UNIQUE INDEX github_merge_candidate_owner
    ON github_merge_effects(repository, pr_number, head_sha);

INSERT INTO schema_migrations(version, applied_at)
VALUES (5, CURRENT_TIMESTAMP);

COMMIT;
