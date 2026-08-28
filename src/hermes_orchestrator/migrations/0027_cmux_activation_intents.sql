BEGIN IMMEDIATE;
-- INFRA-185: write-ahead cmux workspace activation intents. One row is
-- committed BEFORE each external workspace create; its intent_id is the
-- durable unique operation identity that travels inside the requested
-- workspace title. A crash at any point in activation therefore leaves
-- durable evidence from which the exact created workspace (if any) can
-- be correlated and reclaimed — never adopting unrelated workspaces by
-- cwd or title heuristics. 'pending' rows drive reconciliation and block
-- hibernation; 'bound' rows carry the binding that took over ownership;
-- 'aborted' means no workspace ever carried the identity; 'reclaimed'
-- means the unbound workspace was closed by exact identity.
CREATE TABLE cmux_activation_intents (
    intent_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    profile_alias TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'bound', 'aborted', 'reclaimed')
    ),
    binding_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (state != 'bound' OR binding_id IS NOT NULL)
);
CREATE INDEX cmux_intents_state_idx
    ON cmux_activation_intents(state, cell_id);
INSERT INTO schema_migrations(version, applied_at)
VALUES (27, CURRENT_TIMESTAMP);
COMMIT;
