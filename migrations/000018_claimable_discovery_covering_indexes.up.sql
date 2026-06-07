CREATE INDEX IF NOT EXISTS idx_deliveries_ready_discovery
    ON deliveries (customer_id, next_attempt_at, id)
    INCLUDE (webhook_id)
    WHERE status IN ('pending', 'failed_retry');

CREATE INDEX IF NOT EXISTS idx_webhooks_breaker_lookup
    ON webhooks (id)
    INCLUDE (breaker_state, breaker_probe_delivery_id, breaker_next_probe_at);
