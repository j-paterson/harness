BEGIN IMMEDIATE;
-- INFRA-197 (v4 amendment): the classic hermes-control development
-- channel cannot load unattended on a Max account during the channels
-- research preview, so the live idle-wake signal plane substitutes the
-- official fakechat channel plugin instead. Fakechat binds 127.0.0.1
-- on one port per host process with no sender authentication beyond
-- that bind, so Hermes must prove exact session/seat ownership of a
-- port durably before ever sending to it — a stale port left behind by
-- a prior seat could otherwise silently swallow, or misdirect, a wake.
-- One row per session names the exact cell, binding, and generation
-- that own its port; the partial unique index enforces at most one
-- live ('active') row per cell, so issuing a fresh seat's port always
-- retires whatever the prior seat left behind, in the same transaction
-- as the new row.
CREATE TABLE fakechat_signal_ports (
    session_id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    port INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    created_at TEXT NOT NULL,
    retired_at TEXT
);
CREATE UNIQUE INDEX fakechat_signal_ports_active_per_cell
    ON fakechat_signal_ports(cell_id)
    WHERE state = 'active';
INSERT INTO schema_migrations(version, applied_at)
VALUES (48, CURRENT_TIMESTAMP);
COMMIT;
