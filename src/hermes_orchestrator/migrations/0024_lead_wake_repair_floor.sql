BEGIN IMMEDIATE;

-- INFRA-181 correction: a wake lost between a terminal transition and the
-- outbox insert is reconstructed from durable terminal evidence events.
-- The floor records the journal position when this schema appeared, so
-- upgrading a long-lived database never manufactures wakes for historical
-- cells whose boundaries predate the outbox. Repair passes advance the
-- floor past evidence they have deterministically settled.
CREATE TABLE lead_wake_repair (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    floor_sequence INTEGER NOT NULL
);

INSERT INTO lead_wake_repair(id, floor_sequence)
SELECT 1, coalesce(max(sequence), 0) FROM events;

INSERT INTO schema_migrations(version, applied_at)
VALUES (24, CURRENT_TIMESTAMP);

COMMIT;
