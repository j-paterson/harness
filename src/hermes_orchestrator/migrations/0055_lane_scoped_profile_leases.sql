BEGIN IMMEDIATE;
-- INFRA-219 (dual-lane cell model, packet L4): L1 gave project_cells a
-- lane_role dimension and L3 scoped PRODUCT-ISSUE occupancy to the
-- development lane, but profile_leases stayed keyed by project_key
-- alone (ProfilePool._leases and this table). So when a harness cell
-- was created alongside an active development cell for the same
-- project, ProfilePool.acquire returned the development lane's
-- existing lease verbatim -- the two lanes cannot hold distinct
-- profile leases the way the contract requires ("each cell has a
-- distinct lane role, worktree, Claude session, cmux workspace,
-- profile lease, and durable binding") -- and the harness cell's
-- durable insert then collided with the development lease's row and
-- died with sqlite3.IntegrityError. This lands the lane dimension on
-- profile_leases: a new column names which lane a lease belongs to,
-- defaulting every existing and future row to 'development' so
-- today's one-lease-per-project behavior is unchanged while no
-- harness lease exists, and a new unique index expresses one lease
-- per (project_key, lane_role) -- a harness lease may now coexist
-- with an active development lease for the same project, on a
-- DIFFERENT profile. profile_alias stays the table's primary key
-- untouched: that is the separate, still-global shared-resource limit
-- ("one profile serves one lease at a time"), unaffected by the lane
-- dimension -- no table rebuild is needed for either change.
ALTER TABLE profile_leases
    ADD COLUMN lane_role TEXT NOT NULL DEFAULT 'development'
    CHECK (lane_role IN ('development', 'harness'));

CREATE UNIQUE INDEX one_profile_lease_per_project_lane
    ON profile_leases(project_key, lane_role);

INSERT INTO schema_migrations(version, applied_at)
VALUES (55, CURRENT_TIMESTAMP);
COMMIT;
