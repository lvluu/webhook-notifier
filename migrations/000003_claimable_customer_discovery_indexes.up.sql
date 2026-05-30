CREATE INDEX IF NOT EXISTS idx_deliveries_ready_customers
    ON deliveries (customer_id, next_attempt_at)
    WHERE status IN ('pending', 'failed_retry');

CREATE INDEX IF NOT EXISTS idx_deliveries_stale_inflight_customers
    ON deliveries (customer_id, claimed_at)
    WHERE status = 'in_flight';
