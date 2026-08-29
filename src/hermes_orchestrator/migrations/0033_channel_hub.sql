BEGIN IMMEDIATE;
-- INFRA-190: durable state for the dedicated Claude Code channel
-- (hermes-control). Capabilities store only the SHA-256 of each
-- session's channel token — the token itself lives in one mode-0600
-- file under the private state directory and never enters the
-- database, argv, logs, or events. Registrations record each socket
-- registration's exact identity; events are the bounded
-- work/correction notifications with compare-and-set delivery states
-- and per-(kind, packet, session) uniqueness so a replay can never
-- become a second effective intake.
CREATE TABLE channel_capabilities (
    session_id TEXT PRIMARY KEY,
    capability_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    retired_at TEXT
);
CREATE TABLE channel_registrations (
    registration_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    profile_alias TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('active', 'superseded', 'closed')
    ),
    connected_at TEXT NOT NULL,
    closed_at TEXT,
    close_reason TEXT
);
CREATE INDEX channel_registrations_session_idx
    ON channel_registrations(session_id, state);
CREATE TABLE channel_events (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('HERMES_CORRECTION_READY', 'HERMES_WORK_READY')
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
INSERT INTO schema_migrations(version, applied_at)
VALUES (33, CURRENT_TIMESTAMP);
COMMIT;
