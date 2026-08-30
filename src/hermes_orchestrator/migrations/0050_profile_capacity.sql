BEGIN IMMEDIATE;
-- INFRA-197 (correction C2): an auth-healthy profile (max-a) was seated
-- while its weekly Fable budget sat at 100% — auth health alone is not
-- capacity evidence. Replacement selection must fail closed on current,
-- model-specific capacity evidence, so every capacity fact becomes a
-- durable append-only observation. The newest row per
-- (profile_alias, model) is the current evidence: 'available' rows come
-- from operator attestations, 'capped' rows from provider.limit events
-- (recording whatever reset horizon is available — the sanitized stream
-- exposes none today, so the writer records the documented worst-case
-- weekly horizon). resets_at NULL on a capped row means the cap only
-- clears through a newer observation. Additive only: one new table, no
-- existing table is altered.
CREATE TABLE profile_capacity_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_alias TEXT NOT NULL,
    model TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('available', 'capped')),
    source TEXT NOT NULL CHECK (
        source IN ('provider_limit', 'operator_attestation')
    ),
    observed_at TEXT NOT NULL,
    resets_at TEXT,
    detail TEXT
);
-- The one query shape this table serves: the newest observation for a
-- (profile_alias, model) pair.
CREATE INDEX profile_capacity_observations_current
    ON profile_capacity_observations(profile_alias, model, observation_id);
INSERT INTO schema_migrations(version, applied_at)
VALUES (50, CURRENT_TIMESTAMP);
COMMIT;
