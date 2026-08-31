BEGIN IMMEDIATE;
-- INFRA-198 (acceptance-completion policy root fix, packet J1): a merge
-- records implementation completion, not operator acceptance. An
-- acceptance-gated issue carries exactly one durable gate row naming the
-- operator instruction that required it and the predicates that must be
-- covered by evidence before the issue may complete. While the gate is
-- 'pending', the post-merge projection holds the issue short of Done;
-- 'satisfied' stores the covering evidence and restores the ordinary
-- completion path. Gate state advances only pending -> satisfied; the
-- satisfied row is immutable except for byte-identical replays.
CREATE TABLE acceptance_gates (
    issue_id TEXT PRIMARY KEY,
    instruction_id TEXT NOT NULL,
    predicates_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'satisfied')),
    evidence_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO schema_migrations(version, applied_at)
VALUES (53, CURRENT_TIMESTAMP);
COMMIT;
