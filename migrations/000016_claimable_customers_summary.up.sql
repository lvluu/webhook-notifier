CREATE UNLOGGED TABLE IF NOT EXISTS claimable_customers (
    customer_id text PRIMARY KEY
);

INSERT INTO claimable_customers (customer_id)
SELECT DISTINCT d.customer_id
FROM deliveries d
JOIN webhooks w ON w.id = d.webhook_id
LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
WHERE d.status IN ('pending', 'failed_retry')
  AND d.next_attempt_at <= now()
  AND dc.delivery_id IS NULL
  AND w.active = true
  AND (
      w.breaker_state = 'closed'
      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
  )
ON CONFLICT (customer_id) DO NOTHING;