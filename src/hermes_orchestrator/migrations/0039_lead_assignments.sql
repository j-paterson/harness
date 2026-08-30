BEGIN IMMEDIATE;
-- INFRA-195: dispatching a queued issue to an already-idle classic
-- lead previously transitioned the queue and Linear and stopped — no
-- durable assignment existed and the lead slept until an operator
-- bootstrap. The versioned assignment packet is the durable contract:
-- it is committed in the same transaction as the queue transition,
-- bound to the exact project, issue, cell, session, profile,
-- instruction id, and queue transition, and the dedicated channel
-- then wakes the exact lead, which fetches and ACKs it before work
-- begins. Linear and Hermes remain the payload sources of truth; the
-- packet carries binding and provenance, never an authoritative body.
CREATE TABLE lead_assignments (
    assignment_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    project_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    profile_alias TEXT NOT NULL,
    instruction_id TEXT NOT NULL,
    queue_transition TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('published', 'acknowledged', 'superseded')
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    acknowledged_at TEXT
);
-- Exactly one live assignment per issue and session; a superseded
-- assignment (stale session or generation) frees the slot.
CREATE UNIQUE INDEX lead_assignments_live
    ON lead_assignments(issue_id, session_id)
    WHERE state != 'superseded';
-- The channel-event and intake-delivery grammars admit the new kind.
CREATE TABLE channel_events_v39 (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'HERMES_CORRECTION_READY', 'HERMES_WORK_READY',
            'HERMES_ASSIGNMENT_READY'
        )
    ),
    packet_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'published', 'acked', 'superseded')
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    published_at TEXT,
    acked_at TEXT,
    UNIQUE (kind, packet_id, session_id)
);
INSERT INTO channel_events_v39(
    event_id, kind, packet_id, cell_id, session_id, state, attempts,
    created_at, updated_at, published_at, acked_at
)
SELECT event_id, kind, packet_id, cell_id, session_id, state, attempts,
    created_at, updated_at, published_at, acked_at
FROM channel_events;
DROP TABLE channel_events;
ALTER TABLE channel_events_v39 RENAME TO channel_events;
CREATE TABLE lead_intake_deliveries_v39 (
    delivery_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'HERMES_CORRECTION_READY', 'HERMES_WORK_READY',
            'HERMES_ASSIGNMENT_READY'
        )
    ),
    packet_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'claimed', 'attempted', 'announced', 'offered',
            'delivered', 'superseded'
        )
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    offer_token TEXT,
    offered_at TEXT,
    claimed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    delivered_at TEXT,
    UNIQUE (kind, packet_id, session_id)
);
INSERT INTO lead_intake_deliveries_v39(
    delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, offer_token, offered_at, claimed_at, updated_at,
    delivered_at
)
SELECT delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, offer_token, offered_at, claimed_at, updated_at,
    delivered_at
FROM lead_intake_deliveries;
DROP TABLE lead_intake_deliveries;
ALTER TABLE lead_intake_deliveries_v39 RENAME TO lead_intake_deliveries;
INSERT INTO schema_migrations(version, applied_at)
VALUES (39, CURRENT_TIMESTAMP);
COMMIT;
