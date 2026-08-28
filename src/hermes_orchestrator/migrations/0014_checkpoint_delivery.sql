BEGIN IMMEDIATE;

-- Delivery evidence for checkpoint requests (INFRA-168 correction). A
-- pending request is reserved first and delivered second; delivered_at
-- records the successful, revalidated delivery to the exact cell and
-- session. A reserved request that cannot be delivered transitions to
-- failed or stale — it never occupies the single pending slot forever.
ALTER TABLE checkpoint_requests ADD COLUMN delivered_at TEXT;
ALTER TABLE checkpoint_requests ADD COLUMN delivery_evidence TEXT;

INSERT INTO schema_migrations(version, applied_at)
VALUES (14, CURRENT_TIMESTAMP);

COMMIT;
