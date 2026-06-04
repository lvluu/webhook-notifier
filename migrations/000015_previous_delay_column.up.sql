ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS previous_delay_millis bigint;

-- Backfill from latest delivery_attempt for existing rows.
UPDATE deliveries d
SET previous_delay_millis = sub.delay_ms
FROM (
    SELECT DISTINCT ON (da.delivery_id)
        da.delivery_id,
        (EXTRACT(EPOCH FROM (d2.next_attempt_at - da.sent_at)) * 1000)::bigint AS delay_ms
    FROM delivery_attempts da
    JOIN deliveries d2 ON d2.id = da.delivery_id
    ORDER BY da.delivery_id, da.attempt_number DESC
) sub
WHERE d.id = sub.delivery_id
  AND d.previous_delay_millis IS NULL;