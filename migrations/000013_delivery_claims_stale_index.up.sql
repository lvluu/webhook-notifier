CREATE INDEX IF NOT EXISTS idx_delivery_claims_stale_by_claimed_at
    ON delivery_claims (claimed_at, delivery_id);
