BEGIN IMMEDIATE;
-- INFRA-185: durable cmux workspace/surface identity bindings. Each row
-- binds one orchestration seat (the Hermes Orchestrator pane or one
-- Claude project lead) to the exact cmux workspace and surface UUIDs it
-- owns, with a generation that increments on every explicit replacement.
-- Partial unique indexes enforce at most one active Orchestrator seat and
-- at most one active surface per project cell, so duplicate activation
-- must reuse the existing identity and ownership can never transfer
-- silently. Rows carry orchestration metadata only — never commands,
-- prompts, screen content, or credentials.
CREATE TABLE cmux_surface_bindings (
    binding_id TEXT PRIMARY KEY,
    role TEXT NOT NULL CHECK (role IN ('orchestrator', 'lead')),
    project_key TEXT,
    cell_id TEXT,
    session_id TEXT,
    profile_alias TEXT,
    workspace_uuid TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    state TEXT NOT NULL CHECK (state IN ('active', 'stale', 'closed', 'lost')),
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
CREATE UNIQUE INDEX cmux_bindings_single_active_orchestrator
    ON cmux_surface_bindings(role)
    WHERE role = 'orchestrator' AND state = 'active';
CREATE UNIQUE INDEX cmux_bindings_single_active_lead
    ON cmux_surface_bindings(cell_id)
    WHERE role = 'lead' AND state = 'active';
CREATE INDEX cmux_bindings_state_idx
    ON cmux_surface_bindings(state, role);
INSERT INTO schema_migrations(version, applied_at)
VALUES (25, CURRENT_TIMESTAMP);
COMMIT;
