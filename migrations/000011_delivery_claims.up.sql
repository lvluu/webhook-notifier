CREATE TABLE delivery_claims (
    delivery_id uuid PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    claimed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
SELECT id, attempt_count, COALESCE(claimed_at, now())
FROM deliveries
WHERE status = 'in_flight';

UPDATE deliveries
SET status = 'pending',
    next_attempt_at = now()
WHERE status = 'in_flight';

ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'failed_retry', 'succeeded', 'dead'));
