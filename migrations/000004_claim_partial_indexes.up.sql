-- Partial index for the ClaimNextN ready-candidate branch.
--
-- The base index (customer_id, status, next_attempt_at) covers the
-- pending/failed_retry branch of the claim WHERE clause but not the ORDER BY.
-- This partial index includes the ORDER BY columns (created_at, id) so the
-- claim scan stays index-friendly.
--
-- The stale-claim branch is no longer a deliveries(status='in_flight') scan:
-- claim ownership lives in delivery_claims (000011), and stale reclaim is
-- served by idx_delivery_claims_stale_by_claimed_at (000013).
CREATE INDEX IF NOT EXISTS idx_deliveries_ready
    ON deliveries (customer_id, next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'failed_retry');
