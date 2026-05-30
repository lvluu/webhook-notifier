-- Partial indexes for the ClaimNextN query (SCALABILITY_REVIEW P0).
--
-- The current index (customer_id, status, next_attempt_at) covers only the
-- first branch of the claim WHERE clause (pending/failed_retry statuses).
-- The second branch (in_flight with stale claimed_at) is not covered, forcing
-- a filter or seq scan. The OR clause also defeats single-index efficiency.
--
-- These partial indexes each cover one branch of the OR, supporting
-- index-only scans and including the ORDER BY columns (created_at, id).

CREATE INDEX IF NOT EXISTS idx_deliveries_ready
    ON deliveries (customer_id, next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'failed_retry');

CREATE INDEX IF NOT EXISTS idx_deliveries_inflight_stale
    ON deliveries (customer_id, claimed_at)
    WHERE status = 'in_flight';
