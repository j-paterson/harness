BEGIN IMMEDIATE;
-- INFRA-222: Hermes conflates delivery and ownership today.
-- lead_assignments is a versioned packet-delivery record (published /
-- acknowledged / superseded) that also gets read as "what is this
-- cell working on", and project_cells carries the current worker
-- identity (session_id, profile_alias) directly on the cell row, so
-- rotating the worker means overwriting the only durable record of
-- who is bound to the cell. This migration lands the two tables that
-- separate those concerns, with zero behavior change: nothing yet
-- reads or writes them except the backfill below.
--
-- work_claims is cell OWNERSHIP: which cell is doing development,
-- harness, or review work on which issue, independent of how any one
-- delivery epoch turned out. A claim opens when work starts and stays
-- active across any number of superseded/re-published lead_assignment
-- packets; it closes only on an explicit ownership event (issue
-- completion, reassignment) never as a side effect of packet
-- lifecycle. The partial unique index enforces at most one active
-- claim per (issue_id, cell_id, role, child_lane) — a development
-- claim and a harness claim (and, in future, a review claim) for the
-- same issue coexist on purpose.
--
-- worker_bindings is CELL IDENTITY: which concrete worker (session,
-- profile, cmux surface) currently occupies a cell, as a monotonic
-- generation sequence per cell rather than a single mutable pair of
-- columns. Rotating the worker retires generation N and binds
-- generation N+1 in one compare-and-swap; the cell and any open
-- work_claims rows are untouched by the swap.
CREATE TABLE work_claims (
    claim_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('development', 'harness', 'review')),
    child_lane TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('active', 'closed')),
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    closed_reason TEXT,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX work_claims_active
    ON work_claims(issue_id, cell_id, role, child_lane)
    WHERE state = 'active';

CREATE INDEX work_claims_cell_state ON work_claims(cell_id, state);
CREATE INDEX work_claims_issue_state ON work_claims(issue_id, state);

CREATE TABLE worker_bindings (
    binding_id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    session_id TEXT NOT NULL,
    profile_alias TEXT NOT NULL,
    cmux_surface_uuid TEXT,
    state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    bound_at TEXT NOT NULL,
    retired_at TEXT,
    retired_reason TEXT,
    UNIQUE (cell_id, generation)
);

CREATE UNIQUE INDEX worker_bindings_active
    ON worker_bindings(cell_id)
    WHERE state = 'active';

-- Backfill: one active generation-1 binding per live project_cells row,
-- carrying forward its current worker identity exactly.
INSERT INTO worker_bindings(
    binding_id, cell_id, generation, session_id, profile_alias,
    cmux_surface_uuid, state, bound_at, retired_at, retired_reason
)
SELECT
    'wb-' || cell.cell_id,
    cell.cell_id,
    1,
    cell.session_id,
    cell.profile_alias,
    NULL,
    'active',
    cell.updated_at,
    NULL,
    NULL
FROM project_cells AS cell
WHERE cell.state IN ('starting', 'active', 'handoff_required', 'paused')
    AND cell.session_id IS NOT NULL
    AND cell.profile_alias IS NOT NULL;

-- Backfill: one active work claim per non-superseded lead_assignments
-- row bound to a non-terminal admitted issue, deduplicated to the
-- single newest-updated assignment per (issue_id, cell_id, role) so
-- the partial unique index holds. The cell's lane_role names the role
-- directly -- 'development' and 'harness' are the only lane_role
-- values that exist -- 'review' has no backfill source yet.
INSERT INTO work_claims(
    claim_id, project_key, issue_id, cell_id, role, child_lane, state,
    opened_at, closed_at, closed_reason, updated_at
)
SELECT
    'wc-' || live.cell_id || '-' || live.role || '-' || live.issue_id,
    live.project_key,
    live.issue_id,
    live.cell_id,
    live.role,
    '',
    'active',
    live.created_at,
    NULL,
    NULL,
    live.updated_at
FROM (
    SELECT
        assignment.project_key AS project_key,
        assignment.issue_id AS issue_id,
        assignment.cell_id AS cell_id,
        coalesce(cell.lane_role, 'development') AS role,
        assignment.created_at AS created_at,
        assignment.updated_at AS updated_at,
        row_number() OVER (
            PARTITION BY assignment.issue_id, assignment.cell_id,
                coalesce(cell.lane_role, 'development')
            ORDER BY assignment.updated_at DESC, assignment.rowid DESC
        ) AS rank
    FROM lead_assignments AS assignment
    JOIN admitted_issues AS issue ON issue.issue_id = assignment.issue_id
    LEFT JOIN project_cells AS cell ON cell.cell_id = assignment.cell_id
    WHERE assignment.state != 'superseded'
        AND issue.state != 'done'
) AS live
WHERE live.rank = 1;

INSERT INTO schema_migrations(version, applied_at)
VALUES (62, CURRENT_TIMESTAMP);
COMMIT;
