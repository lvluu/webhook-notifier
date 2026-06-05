-- Aggressive autovacuum for delivery_claims (finding B-6 / PR #111 item 6).
--
-- delivery_claims is the hottest churn table: every claim INSERTs a row and
-- every terminal/retry outcome DELETEs it. Under sustained load the default
-- autovacuum thresholds let dead tuples accumulate to >99% of the table, so
-- the claim anti-join (deliveries LEFT JOIN delivery_claims ... IS NULL) and
-- the stale-claim scan walk mostly-dead heap pages. Tighten autovacuum so the
-- table stays compact:
--   threshold=20 + scale_factor=0  -> vacuum after ~20 dead tuples, not a %
--   cost_limit=2000, cost_delay=1ms -> let it keep up under high churn
ALTER TABLE delivery_claims SET (
    autovacuum_vacuum_threshold = 20,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 1
);
