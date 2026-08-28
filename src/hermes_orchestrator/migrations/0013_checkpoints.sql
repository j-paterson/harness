BEGIN IMMEDIATE;

-- Durable checkpoint-safety evidence (INFRA-168 correction). One row per
-- project cell, bound to the exact lead session and the turn-completion
-- evidence that proved the safe boundary; invalidated the moment new work
-- is dispatched, the session rotates, or the cell leaves the active state.
CREATE TABLE checkpoint_safety (
    cell_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    state TEXT NOT NULL,
    boundary_kind TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    invalidated_at TEXT,
    invalidation_reason TEXT
);

-- Durable checkpoint-request lifecycle. request_id is idempotent
-- (cell, session, evidence); exactly one request may be pending across
-- the whole host; terminal states are completed, failed, and stale.
CREATE TABLE checkpoint_requests (
    request_id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT
);

CREATE UNIQUE INDEX one_pending_checkpoint_request
    ON checkpoint_requests(state)
    WHERE state = 'pending';

INSERT INTO schema_migrations(version, applied_at)
VALUES (13, CURRENT_TIMESTAMP);

COMMIT;
