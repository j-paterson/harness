BEGIN IMMEDIATE;
-- INFRA-194 (Sol correction f732f35b): a schema-36 'intended' row is
-- semantically ambiguous — it may be a pre-push intent, an in-flight
-- or ambiguous failed push, or a landed push awaiting finalization —
-- while the schema-37 'intended' state is strictly stronger: provably
-- unattempted, safe to abort retryably once its lease expires.
-- Migration 0037 carried legacy rows forward with their state
-- unchanged (identifiable by the empty owner_token it assigned), so a
-- legacy publication could have been falsely aborted without ever
-- reading the remote. Remap every such row to the conservative
-- blocking 'reconciliation_required' state: reconciliation finalizes
-- it on exact final-SHA identity or proven ancestry and otherwise
-- keeps it blocking; a legacy row is never auto-aborted, because the
-- legacy protocol left no attempted-boundary evidence that its push
-- could not still have occurred. Recorded and aborted legacy rows are
-- terminal and stay untouched.
UPDATE reviewer_fixes
SET state = 'reconciliation_required',
    reason = 'schema-36 intent upgraded conservatively; the push may '
        || 'have been attempted and requires remote reconciliation'
WHERE state = 'intended' AND owner_token = '';
INSERT INTO schema_migrations(version, applied_at)
VALUES (38, CURRENT_TIMESTAMP);
COMMIT;
