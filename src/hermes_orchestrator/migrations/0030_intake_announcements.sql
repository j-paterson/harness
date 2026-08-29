BEGIN IMMEDIATE;
-- INFRA-190: keystroke delivery is removed entirely; the intake
-- channel becomes metadata announcements plus a lead-owned poll. The
-- delivery state machine gains 'announced' (the announcer's terminal
-- state: workspace status and notification published); 'delivered'
-- now means the lead's own poll handed the envelope out through the
-- application layer. Existing rows keep their states unchanged.
CREATE TABLE lead_intake_deliveries_v30 (
    delivery_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('HERMES_CORRECTION_READY', 'HERMES_WORK_READY')
    ),
    packet_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'claimed', 'attempted', 'announced', 'delivered', 'superseded'
        )
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    delivered_at TEXT,
    UNIQUE (kind, packet_id, session_id)
);
INSERT INTO lead_intake_deliveries_v30(
    delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, claimed_at, updated_at, delivered_at
)
SELECT delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, claimed_at, updated_at, delivered_at
FROM lead_intake_deliveries;
DROP TABLE lead_intake_deliveries;
ALTER TABLE lead_intake_deliveries_v30 RENAME TO lead_intake_deliveries;
INSERT INTO schema_migrations(version, applied_at)
VALUES (30, CURRENT_TIMESTAMP);
COMMIT;
