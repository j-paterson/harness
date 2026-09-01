BEGIN IMMEDIATE;
-- INFRA-219 (dual-lane cell model, packet L1): Hermes enforces one
-- active project cell per project today, but the dual-lane workflow
-- needs one continuous DEVELOPMENT cell plus one separate HARNESS
-- cell per project, each with its own durable identity. This lands
-- ONLY the durable lane-role model: a new column names which lane a
-- cell belongs to, defaulting every existing and future row to
-- 'development' so today's one-cell-per-project behavior is
-- unchanged while no harness cell exists, and the uniqueness this
-- schema already expressed per project becomes per (project_key,
-- lane_role) -- a harness cell may now coexist with an active
-- development cell for the same project, while two cells of the SAME
-- lane still refuse exactly as one-per-project refused before. The
-- harness launch/rotate/recover surface and dashboard are wave 2
-- (INFRA-219 L2), out of scope here.
ALTER TABLE project_cells
    ADD COLUMN lane_role TEXT NOT NULL DEFAULT 'development'
    CHECK (lane_role IN ('development', 'harness'));

DROP INDEX one_active_cell_per_project;

CREATE UNIQUE INDEX one_active_cell_per_project_lane
    ON project_cells(project_key, lane_role)
    WHERE state IN ('starting', 'active', 'handoff_required', 'paused');

INSERT INTO schema_migrations(version, applied_at)
VALUES (54, CURRENT_TIMESTAMP);
COMMIT;
