BEGIN IMMEDIATE;
-- INFRA-190: durable evidence for classic in-pane lead seats and the
-- focus-preserving lead-intake transport.
--
-- cmux_classic_seats records that a seat's workspace was created with
-- the classic interactive Claude TUI as its command for exactly one
-- session; the intake transport refuses any surface without this
-- evidence (a non-classic surface must never receive an envelope).
--
-- lead_intake_deliveries deduplicates envelope delivery: one envelope
-- kind per durable packet id per session is delivered at most once
-- through the transport. Rows carry orchestration metadata only —
-- never packet contents, prompts, or credentials; the lead retrieves
-- the actual packet from durable state by its id.
CREATE TABLE cmux_classic_seats (
    binding_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE lead_intake_deliveries (
    delivery_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('HERMES_CORRECTION_READY', 'HERMES_WORK_READY')
    ),
    packet_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    delivered_at TEXT NOT NULL,
    UNIQUE (kind, packet_id, session_id)
);
INSERT INTO schema_migrations(version, applied_at)
VALUES (28, CURRENT_TIMESTAMP);
COMMIT;
