-- consecutiveWebhookFailures joins delivery_attempts ⋈ deliveries and ranges
-- over sent_at to count attempts since last 2xx. The existing index
-- idx_delivery_attempts_delivery_id_attempt_number only covers
-- (delivery_id, attempt_number) which does not help the sent_at range scan.
-- This index enables an efficient nested-loop range scan from the
-- deliveries(webhook_id) filter.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delivery_attempts_delivery_id_sent_at
    ON delivery_attempts (delivery_id, sent_at);
