BEGIN IMMEDIATE;
-- INFRA-190: durable delivery state machine for the lead-intake
-- transport. A delivery row is claimed (compare-and-swap insert) BEFORE
-- any external typing, marked attempted before each external sequence,
-- and delivered only after cmux acknowledged the full sequence — so
-- concurrent and restart retries produce at most one owner, and failed
-- or uncertain attempts stay durable and distinguishable from success.
-- Existing rows were recorded only after acknowledged delivery and are
-- carried over as delivered. Terminal wakes that predate the intake
-- router are out of its scope; fabricating delivered rows would falsely
-- record success, so they are backfilled as 'superseded' — durable,
-- distinguishable, and never typed.
CREATE TABLE lead_intake_deliveries_v29 (
    delivery_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('HERMES_CORRECTION_READY', 'HERMES_WORK_READY')
    ),
    packet_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('claimed', 'attempted', 'delivered', 'superseded')
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    delivered_at TEXT,
    UNIQUE (kind, packet_id, session_id)
);
INSERT INTO lead_intake_deliveries_v29(
    delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, claimed_at, updated_at, delivered_at
)
SELECT delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    'delivered', 1, delivered_at, delivered_at, delivered_at
FROM lead_intake_deliveries;
DROP TABLE lead_intake_deliveries;
ALTER TABLE lead_intake_deliveries_v29 RENAME TO lead_intake_deliveries;
INSERT INTO lead_intake_deliveries(
    delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, claimed_at, updated_at
)
SELECT lower(hex(randomblob(16))), 'HERMES_WORK_READY', w.wake_id,
    w.cell_id, w.session_id, '', 'superseded', 0,
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM lead_terminal_wakes AS w
WHERE NOT EXISTS (
    SELECT 1 FROM lead_intake_deliveries AS d
    WHERE d.kind = 'HERMES_WORK_READY'
    AND d.packet_id = w.wake_id
    AND d.session_id = w.session_id
);
INSERT INTO schema_migrations(version, applied_at)
VALUES (29, CURRENT_TIMESTAMP);
COMMIT;
