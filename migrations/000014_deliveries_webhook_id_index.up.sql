-- Add the missing deliveries(webhook_id) index (issue #99, finding B-2).
--
-- consecutiveWebhookFailures / batchConsecutiveWebhookFailures
-- (internal/outbox/breaker.go) join delivery_attempts to deliveries and filter
-- on deliveries.webhook_id to count failures since the last 2xx. Migration
-- 000006's comment assumed "the deliveries(webhook_id) filter" was index-backed,
-- but no such index ever existed (only the PK and customer_id-leading
-- composites). Without it the webhook_id filter degrades to a scan that grows
-- with a webhook's delivery history and runs on every retry/dead outcome —
-- i.e. precisely during a partner outage, when the breaker matters most.
--
-- Single-statement file with CONCURRENTLY: golang-migrate runs a multi-statement
-- migration inside an implicit transaction, and CREATE INDEX CONCURRENTLY cannot
-- run in a transaction block. Keeping this alone (as migration 000006 does)
-- builds the index without an ACCESS EXCLUSIVE lock on the hot deliveries table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deliveries_webhook_id
    ON deliveries (webhook_id);
