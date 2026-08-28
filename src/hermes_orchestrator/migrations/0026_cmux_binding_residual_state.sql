BEGIN IMMEDIATE;
-- INFRA-185: add the 'residual' binding state — durable ownership
-- evidence for a cmux workspace Hermes created or retired whose close
-- was never confirmed. A residual seat blocks hibernation and is
-- reclaimed (closed or recorded lost) by startup reconciliation, so no
-- live workspace can ever fall outside the bindings table. SQLite cannot
-- alter a CHECK constraint, so the table is rebuilt in place with rowids
-- preserved (reconciliation ordering depends on them) and its
-- partial-unique ownership indexes recreated unchanged.
CREATE TABLE cmux_surface_bindings_v26 (
    binding_id TEXT PRIMARY KEY,
    role TEXT NOT NULL CHECK (role IN ('orchestrator', 'lead')),
    project_key TEXT,
    cell_id TEXT,
    session_id TEXT,
    profile_alias TEXT,
    workspace_uuid TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    state TEXT NOT NULL CHECK (
        state IN ('active', 'stale', 'closed', 'lost', 'residual')
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        role != 'lead'
        OR (
            project_key IS NOT NULL
            AND cell_id IS NOT NULL
            AND session_id IS NOT NULL
            AND profile_alias IS NOT NULL
        )
    )
);
INSERT INTO cmux_surface_bindings_v26(
    rowid, binding_id, role, project_key, cell_id, session_id,
    profile_alias, workspace_uuid, surface_uuid, generation, state,
    created_at, updated_at
)
SELECT rowid, binding_id, role, project_key, cell_id, session_id,
    profile_alias, workspace_uuid, surface_uuid, generation, state,
    created_at, updated_at
FROM cmux_surface_bindings;
DROP TABLE cmux_surface_bindings;
ALTER TABLE cmux_surface_bindings_v26 RENAME TO cmux_surface_bindings;
CREATE UNIQUE INDEX cmux_bindings_single_active_orchestrator
    ON cmux_surface_bindings(role)
    WHERE role = 'orchestrator' AND state = 'active';
CREATE UNIQUE INDEX cmux_bindings_single_active_lead
    ON cmux_surface_bindings(cell_id)
    WHERE role = 'lead' AND state = 'active';
CREATE INDEX cmux_bindings_state_idx
    ON cmux_surface_bindings(state, role);
INSERT INTO schema_migrations(version, applied_at)
VALUES (26, CURRENT_TIMESTAMP);
COMMIT;
