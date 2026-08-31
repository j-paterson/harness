BEGIN IMMEDIATE;
-- INFRA-197 (v5.1 amendment, operator decision
-- infra-197-trusted-channel-auto-approval-20260830-v1): after ONE exact
-- manual trust event of ONE exact hermes-control development-channel
-- build, a narrowly scoped automatic confirmation may stand in for
-- Claude Code's per-launch development-channel dialog, and nothing
-- broader. One durable anchor row binds every fact that manual trust
-- event proved true — the canonical entry path and its owning uid (no
-- symlink substitution tolerated on either), the packaged content
-- (entry sha256 plus a deterministic dist-tree digest), the plugin's
-- own manifest name/version, the fixed channel entry, the build
-- timestamp, the exact launch argument template, the exact cmux
-- workspace/surface/session/profile binding, and the exact confirmation
-- prompt shape. A live re-derivation of every one of those fields
-- against this anchor is the only path to an automatic confirmation;
-- any drift fails closed. The partial unique index enforces at most one
-- live ('active') anchor per cell, so capturing a fresh anchor always
-- requires the prior one to be explicitly retired first — trust is
-- never silently superseded.
CREATE TABLE channel_trust_anchors (
    anchor_id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    profile_alias TEXT NOT NULL,
    canonical_entry_path TEXT NOT NULL,
    entry_owner_uid INTEGER NOT NULL,
    entry_sha256 TEXT NOT NULL,
    dist_tree_sha256 TEXT NOT NULL,
    manifest_name TEXT NOT NULL,
    manifest_version TEXT NOT NULL,
    channel_entry TEXT NOT NULL,
    build_mtime TEXT NOT NULL,
    launch_argv_template_json TEXT NOT NULL,
    workspace_uuid TEXT NOT NULL,
    surface_uuid TEXT NOT NULL,
    session_id TEXT NOT NULL,
    -- NULL means the confirmation prompt shape is not yet captured
    -- (the manual trust event's dialog text was unrecoverable);
    -- ChannelTrustAnchors.complete_prompt binds it exactly once, and
    -- the gate fails closed with "prompt_evidence_pending" until then.
    prompt_pattern TEXT,
    state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    created_at TEXT NOT NULL,
    retired_at TEXT
);
CREATE UNIQUE INDEX channel_trust_anchors_active_per_cell
    ON channel_trust_anchors(cell_id)
    WHERE state = 'active';
INSERT INTO schema_migrations(version, applied_at)
VALUES (49, CURRENT_TIMESTAMP);
COMMIT;
