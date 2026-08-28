BEGIN IMMEDIATE;

-- Forward-only upgrade for durable databases whose recorded migration 3
-- came from the older Merger implementation (INFRA-166). Those databases
-- have a `merger_threads` table and no `reviewer_channels` table, and an
-- already-recorded migration number is never re-run. This migration
-- establishes the current reviewer-channel schema when it is missing and
-- carries every legacy thread row forward fail-closed: a legacy thread id
-- lands as a `configuring` channel at generation 0, which becomes `ready`
-- only after the live thread is re-verified and re-configured; a legacy
-- row without a thread id lands as `uncertain` and requires operator
-- reconciliation. Legacy state is preserved in replacement_reason.
CREATE TABLE IF NOT EXISTS reviewer_channels (
    project_key TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    integration_branch TEXT NOT NULL DEFAULT '',
    prior_thread_id TEXT,
    replacement_reason TEXT,
    last_delivered_event_id TEXT,
    last_delivered_candidate_sha TEXT,
    last_delivery_failure_at TEXT,
    heartbeat_enabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- SQL migrations cannot branch on table existence, so the legacy table is
-- materialised empty when absent, its rows are carried forward, and it is
-- then removed: every legacy row now lives in reviewer_channels.
CREATE TABLE IF NOT EXISTS merger_threads (
    project_key TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT INTO reviewer_channels(
    project_key, thread_id, generation, state, integration_branch,
    prior_thread_id, replacement_reason, heartbeat_enabled,
    created_at, updated_at
)
SELECT
    project_key,
    thread_id,
    0,
    CASE WHEN thread_id <> '' THEN 'configuring' ELSE 'uncertain' END,
    '',
    NULL,
    'legacy_merger_threads:' || state,
    0,
    created_at,
    updated_at
FROM merger_threads
WHERE project_key NOT IN (SELECT project_key FROM reviewer_channels);

DROP TABLE merger_threads;

-- Durable outbox of correction packets returned to the Claude lead
-- (INFRA-166). Hermes reads pending packets and resumes the lead; the
-- orchestrator never talks to the lead session directly.
CREATE TABLE lead_corrections (
    correction_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    source TEXT NOT NULL,
    repository TEXT NOT NULL,
    branch TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    reviewed_sha TEXT NOT NULL,
    packets_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acknowledged_at TEXT
);

CREATE INDEX lead_corrections_pending_idx
    ON lead_corrections(project_key, state, created_at);

INSERT INTO schema_migrations(version, applied_at)
VALUES (9, CURRENT_TIMESTAMP);

COMMIT;
