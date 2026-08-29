BEGIN IMMEDIATE;
-- INFRA-190: the lead-owned poll becomes an offer/acknowledgement
-- lease. A poll compare-and-swaps a row into 'offered' under an opaque
-- offer token with an expiry; 'delivered' is recorded only by the
-- lead's explicit acknowledgement bound to the exact session, packet,
-- and offer token. A crash, broken pipe, or hook-host rejection after
-- the offer leaves the row 'offered' and it is safely re-offered
-- (under a fresh token) once the lease expires — a handed-out envelope
-- can never be lost by an unacknowledged transition. Existing rows are
-- carried over unchanged with no offer lease.
CREATE TABLE lead_intake_deliveries_v31 (
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
INSERT INTO lead_intake_deliveries_v31(
    delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, claimed_at, updated_at, delivered_at
)
SELECT delivery_id, kind, packet_id, cell_id, session_id, surface_uuid,
    state, attempts, claimed_at, updated_at, delivered_at
FROM lead_intake_deliveries;
DROP TABLE lead_intake_deliveries;
ALTER TABLE lead_intake_deliveries_v31 RENAME TO lead_intake_deliveries;
INSERT INTO schema_migrations(version, applied_at)
VALUES (31, CURRENT_TIMESTAMP);
COMMIT;
