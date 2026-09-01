BEGIN IMMEDIATE;

-- INFRA-223 item 1 (exact queue start and recovery). A successful
-- `codex queue` exit proves only that the message was accepted into the
-- resident app-server's thread queue -- never that Codex STARTED the
-- turn. Observed 2026-09-01: a required structured-submission turn and
-- later immutable candidates sat queued while the daemon-owned writer
-- was idle, because `wake_deliveries.state = 'delivered'` was written on
-- exit 0 and read everywhere as "the reviewer is working on it".
--
-- No new table and no new transport: the queued/started truth rides the
-- wake row that already carries this candidate's durable identity.
-- `state` keeps its exact meaning (which candidate holds the primary Sol
-- writer -- INFRA-221's one-at-a-time invariant) and `queue_state`
-- carries, independently, how far that candidate got inside Codex's own
-- thread queue:
--
--   NULL       not queued (the row is 'pending'/'claimed')
--   'queued'   `codex queue` exited 0; durably in the thread queue and
--              NOT started -- this is what a bare exit 0 now buys
--   'starting' an explicit `thread/queue/start` was claimed for the
--              bound queue item and its outcome is not yet observed;
--              ambiguous by design, so recovery re-drives it
--   'started'  an OBSERVED start of the bound queue item (our own
--              successful `thread/queue/start`, or Codex's auto-start
--              seen in `thread/queue/list`)
--   'settled'  the wake reached a terminal `state`
--
-- `queue_item_id` binds the row to the EXACT Codex queue item it was
-- observed at, so an interrupted or unstarted head is re-driven as the
-- same item after a restart rather than executed a second time.
ALTER TABLE wake_deliveries ADD COLUMN queue_state TEXT
    CHECK (
        queue_state IS NULL
        OR queue_state IN ('queued', 'starting', 'started', 'settled')
    );

ALTER TABLE wake_deliveries ADD COLUMN queue_item_id TEXT;

-- The explicit capability handshake's durable record. The experimental
-- thread-queue APIs are used only when the CONNECTED app-server
-- advertised them at initialize; when it does not, the adapter falls
-- back to today's behaviour rather than failing hard, and this column is
-- how that fallback is recorded instead of being silent.
ALTER TABLE reviewer_channels ADD COLUMN thread_queue_capability TEXT
    CHECK (
        thread_queue_capability IS NULL
        OR thread_queue_capability IN ('advertised', 'absent')
    );

INSERT INTO schema_migrations(version, applied_at)
VALUES (57, CURRENT_TIMESTAMP);

COMMIT;
