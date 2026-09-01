BEGIN IMMEDIATE;
-- INFRA-219 R5 (Sol correction 110ed759): "The active runtime and cmux
-- reconciler remain keyed only by project and development lead_cwd
-- (runtime.py:667-719,784-798), while CmuxBinding carries no lane
-- identity, so daemon recovery cannot restore a harness binding to its
-- dedicated worktree." This lands the durable lane identity on the
-- binding table itself: a lane_role column, defaulting every existing
-- and future row to 'development' so today's one-binding-per-cell
-- behavior is unchanged while no harness binding exists, with the
-- same CHECK grammar migration 0054 established for
-- project_cells.lane_role. The single-active-lead uniqueness this
-- schema already expressed per cell now folds lane_role in explicitly,
-- so a development and a harness binding for the same project are a
-- formally distinct durable identity -- never merely distinct by
-- accident of carrying different cell_id values.
ALTER TABLE cmux_surface_bindings
    ADD COLUMN lane_role TEXT NOT NULL DEFAULT 'development'
    CHECK (lane_role IN ('development', 'harness'));

DROP INDEX cmux_bindings_single_active_lead;

CREATE UNIQUE INDEX cmux_bindings_single_active_lead
    ON cmux_surface_bindings(cell_id, lane_role)
    WHERE role = 'lead' AND state = 'active';

-- Lane-scoped project lookup: restart recovery and any lane-aware
-- reader must be able to resolve exactly one project's binding within
-- one lane without scanning every role/state combination.
CREATE INDEX cmux_bindings_project_lane_idx
    ON cmux_surface_bindings(project_key, lane_role)
    WHERE role = 'lead';

-- INFRA-219 R5 (lead completion): the write-ahead activation intent is
-- what a crashed activation replays from, so it must carry the lane
-- too. Without it, an interrupted HARNESS activation would resume and
-- mint its successor binding as 'development' -- silently converting
-- the lane at exactly the recovery boundary this packet exists to make
-- lane-correct. Existing rows default to 'development', which is what
-- every pre-lane intent in fact was.
ALTER TABLE cmux_activation_intents
    ADD COLUMN lane_role TEXT NOT NULL DEFAULT 'development'
    CHECK (lane_role IN ('development', 'harness'));

INSERT INTO schema_migrations(version, applied_at)
VALUES (56, CURRENT_TIMESTAMP);
COMMIT;
