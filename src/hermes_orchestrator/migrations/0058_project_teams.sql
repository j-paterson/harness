BEGIN IMMEDIATE;

-- INFRA-187 (project_teams coordinator): a durable pair coordinator
-- making one Fable work lead plus one Sol merge lead the execution
-- unit per project. Team identity is (project_key, generation); the
-- member columns here reference the exact durable identities already
-- owned elsewhere -- the fable_* columns name an exact ``project_cells``
-- row (cells.py) and the sol_* columns name an exact
-- ``reviewer_channels`` thread/generation (codex_merger.py) -- never a
-- duplicate of that state, only a pointer to it. Every write is a
-- compare-and-swap on the live (project_key, generation, state) row;
-- at most one live team (state not in 'superseded'/'retired') exists
-- per project at a time, enforced below exactly like project_cells'
-- own one-live-row-per-project index.
CREATE TABLE project_teams (
    project_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'reserved',
            'fable_bound',
            'sol_bound',
            'ready',
            'uncertain',
            'superseded',
            'retired'
        )
    ),
    repo_path TEXT NOT NULL,
    integration_branch TEXT NOT NULL,
    fable_cell_id TEXT,
    fable_session_id TEXT,
    fable_profile_alias TEXT,
    fable_generation INTEGER NOT NULL DEFAULT 0,
    sol_thread_id TEXT,
    sol_generation INTEGER,
    sol_model TEXT,
    sol_provider TEXT,
    reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    retired_at TEXT,
    PRIMARY KEY (project_key, generation)
);

CREATE UNIQUE INDEX one_live_team_per_project
    ON project_teams(project_key)
    WHERE state NOT IN ('superseded', 'retired');

INSERT INTO schema_migrations(version, applied_at)
VALUES (58, CURRENT_TIMESTAMP);

COMMIT;
