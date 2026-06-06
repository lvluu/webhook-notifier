-- Cover the breaker-aware claimable-Customer discovery path.
--
-- The recursive discovery query walks ready Deliveries by Customer and joins
-- each candidate row to its Webhook to apply circuit-breaker eligibility. The
-- existing ready index does not carry webhook_id, and the Webhook primary key
-- index does not carry breaker state, so sparse discovery under many Customers
-- can devolve into heavy heap churn before workers are even notified.
CREATE INDEX IF NOT EXISTS idx_deliveries_ready_discovery
    ON deliveries (customer_id, next_attempt_at, id)
    INCLUDE (webhook_id)
    WHERE status IN ('pending', 'failed_retry');

CREATE INDEX IF NOT EXISTS idx_webhooks_breaker_lookup
    ON webhooks (id)
    INCLUDE (breaker_state, breaker_probe_delivery_id, breaker_next_probe_at);
