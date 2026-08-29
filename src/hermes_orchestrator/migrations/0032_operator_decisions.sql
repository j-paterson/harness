BEGIN IMMEDIATE;
-- INFRA-190: durable operator decisions. A decision awaiting the
-- operator is enforceable state: while one is pending for an issue,
-- implementation dispatch and candidate publication are blocked. Only
-- an explicit, nonempty, schema-valid application command for the
-- exact pending decision id may resolve it — blank messages, hooks,
-- transcript replay, system events, and model assertions never
-- mutate anything. Rows are append-only except the single
-- pending -> approved/rejected transition, and an imported receipt
-- records its exact SHA-256.
CREATE TABLE operator_decisions (
    decision_id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    project_key TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    choice TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'approved', 'rejected')
    ),
    receipt_sha256 TEXT,
    source_message TEXT,
    recorded_at TEXT NOT NULL,
    applied_at TEXT
);
CREATE INDEX operator_decisions_issue_idx
    ON operator_decisions(issue_id, status);
INSERT INTO schema_migrations(version, applied_at)
VALUES (32, CURRENT_TIMESTAMP);
COMMIT;
