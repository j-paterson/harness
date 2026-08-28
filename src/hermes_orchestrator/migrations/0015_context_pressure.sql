BEGIN IMMEDIATE;

-- Active execution time (INFRA-169): closed and open intervals per worker
-- session. Idle time between turns never counts toward the six active
-- hours.
CREATE TABLE active_intervals (
    interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT
);

CREATE INDEX active_intervals_worker_idx ON active_intervals(worker_id, closed_at);

-- Durable context-pressure evidence per worker session: percentage when
-- available, compaction and rapid-refill counts, context errors, and
-- behavioural warnings, plus the sticky decision state.
CREATE TABLE context_evidence (
    worker_id TEXT PRIMARY KEY,
    last_percent REAL,
    compactions INTEGER NOT NULL DEFAULT 0,
    rapid_refills INTEGER NOT NULL DEFAULT 0,
    context_errors INTEGER NOT NULL DEFAULT 0,
    warnings INTEGER NOT NULL DEFAULT 0,
    active_hours REAL NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    reasons_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);

INSERT INTO schema_migrations(version, applied_at)
VALUES (15, CURRENT_TIMESTAMP);

COMMIT;
