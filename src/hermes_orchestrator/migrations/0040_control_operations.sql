BEGIN IMMEDIATE;
-- INFRA-195: recovery used to be provable only by watching a terminal.
-- A control operation is a versioned durable event + receipt for a
-- lifecycle action whose result can legitimately be an absence — the
-- daemon restarted, a channel re-registered, a replay that delivered
-- zero events, a dedup repair that superseded stale events. The exact
-- lead is woken through the dedicated channel, fetches the durable
-- row, and ACKs it; result_json encodes the outcome explicitly
-- (including negative results such as {"replay_count": 0}) so "no
-- event" is a recorded fact, never silence. At most one live
-- operation exists per dedup key, so repeated recovery cannot flood
-- the channel while an earlier receipt is still unacknowledged.
CREATE TABLE control_operations (
    operation_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    project_key TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    result_json TEXT NOT NULL,
    reason TEXT,
    state TEXT NOT NULL CHECK (
        state IN ('published', 'acknowledged', 'superseded')
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    acknowledged_at TEXT
);
CREATE UNIQUE INDEX control_operations_live
    ON control_operations(dedup_key)
    WHERE state = 'published';
-- The channel-event and intake-delivery grammars admit the new kind.
CREATE TABLE channel_events_v40 (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'HERMES_CORRECTION_READY', 'HERMES_WORK_READY',
            'HERMES_ASSIGNMENT_READY', 'HERMES_CONTROL_READY'
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
INSERT INTO channel_events_v40(
    event_id, kind, packet_id, cell_id, session_id, state, attempts,
    created_at, updated_at, published_at, acked_at
)
SELECT event_id, kind, packet_id, cell_id, session_id, state, attempts,
    created_at, updated_at, published_at, acked_at
FROM channel_events;
DROP TABLE channel_events;
ALTER TABLE channel_events_v40 RENAME TO channel_events;
CREATE TABLE lead_intake_deliveries_v40 (
    delivery_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'HERMES_CORRECTION_READY', 'HERMES_WORK_READY',
            'HERMES_ASSIGNMENT_READY', 'HERMES_CONTROL_READY'
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
INSERT INTO lead_intake_deliveries_v40(
    delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, offer_token, offered_at, claimed_at, updated_at,
    delivered_at
)
SELECT delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, offer_token, offered_at, claimed_at, updated_at,
    delivered_at
FROM lead_intake_deliveries;
DROP TABLE lead_intake_deliveries;
ALTER TABLE lead_intake_deliveries_v40 RENAME TO lead_intake_deliveries;
INSERT INTO schema_migrations(version, applied_at)
VALUES (40, CURRENT_TIMESTAMP);
COMMIT;
