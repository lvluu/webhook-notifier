DROP TABLE IF EXISTS delivery_claims;

ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'in_flight', 'failed_retry', 'succeeded', 'dead'));
