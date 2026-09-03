BEGIN IMMEDIATE;
-- INFRA-224: a global Hermes operator-decision inbox. Today an
-- operator decision only carries the fields needed to gate one
-- issue's dispatch; there is no durable, cross-project queue an
-- operator can work from, and no way to distinguish an authority-
-- worthy question from a routine reversible implementation choice an
-- agent should just make. This migration adds the inbox columns to
-- the existing operator_decisions table (additive only -- every
-- existing row, and every existing reader, keeps working unchanged)
-- plus the indexes the inbox needs: a partial unique index so a
-- deduplication key can never carry two live pending rows at once,
-- and a covering index for the urgency-then-recency inbox ordering.
ALTER TABLE operator_decisions ADD COLUMN question TEXT;
ALTER TABLE operator_decisions ADD COLUMN authority_reason TEXT;
ALTER TABLE operator_decisions ADD COLUMN requesting_role TEXT;
ALTER TABLE operator_decisions ADD COLUMN facts_json TEXT;
ALTER TABLE operator_decisions ADD COLUMN options_json TEXT;
ALTER TABLE operator_decisions ADD COLUMN recommendation TEXT;
ALTER TABLE operator_decisions ADD COLUMN delay_impact TEXT;
ALTER TABLE operator_decisions ADD COLUMN paused_scope TEXT;
ALTER TABLE operator_decisions ADD COLUMN urgency INTEGER NOT NULL DEFAULT 2;
ALTER TABLE operator_decisions ADD COLUMN request_key TEXT;
ALTER TABLE operator_decisions ADD COLUMN category TEXT;
ALTER TABLE operator_decisions ADD COLUMN answer TEXT;
ALTER TABLE operator_decisions ADD COLUMN next_action TEXT;

CREATE UNIQUE INDEX operator_decisions_pending_request
    ON operator_decisions(request_key)
    WHERE status = 'pending' AND request_key IS NOT NULL;

CREATE INDEX operator_decisions_inbox_idx
    ON operator_decisions(status, urgency, recorded_at);

INSERT INTO schema_migrations(version, applied_at)
VALUES (61, CURRENT_TIMESTAMP);
COMMIT;
