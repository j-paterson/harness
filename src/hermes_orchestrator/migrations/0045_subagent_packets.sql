BEGIN IMMEDIATE;
-- INFRA-186: delegating implementation to disjoint Claude subagents
-- needs a durable ledger, not a prompt-carried plan. Each packet binds
-- one bounded unit of work (allowed files, a red test, verification
-- commands, invariants) to an issue, cell, and lead session before any
-- subagent is spawned. Reservation is an exactly-once compare-and-swap
-- keyed on the PreToolUse tool_use_id, so a crashed or duplicated
-- Agent invocation can never claim the same packet twice; settlement
-- must match that same tool_use_id, so only the reserving invocation
-- can report its own outcome. Acceptance is a separate, later step
-- gated on reviewed evidence (diff scope, red/green proof references)
-- — never the prompt or model output text, which this ledger never
-- stores. A reservation whose tool-use evidence is proven absent
-- (the invocation is no longer live) expires to 'failed' so a stuck
-- packet cannot block the issue forever.
CREATE TABLE subagent_packets (
    packet_id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    project_key TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    model_tier TEXT NOT NULL,
    effort TEXT NOT NULL,
    allowed_files_json TEXT NOT NULL,
    worktree TEXT NOT NULL,
    depends_on_json TEXT NOT NULL,
    red_test TEXT NOT NULL,
    verification_json TEXT NOT NULL,
    invariants TEXT NOT NULL,
    resource_note TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'planned', 'reserved', 'returned', 'failed', 'rejected',
            'accepted'
        )
    ),
    evidence_json TEXT,
    tool_use_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- The common lookup is "this issue's packets, filtered or grouped by
-- state" — the wave-integration view and the admission gate both walk
-- exactly that shape.
CREATE INDEX subagent_packets_issue_state
    ON subagent_packets(issue_id, state);
INSERT INTO schema_migrations(version, applied_at)
VALUES (45, CURRENT_TIMESTAMP);
COMMIT;
