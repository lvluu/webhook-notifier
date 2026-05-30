DROP INDEX IF EXISTS idx_webhooks_breaker_probe;

ALTER TABLE webhooks
DROP COLUMN IF EXISTS breaker_probe_delivery_id,
DROP COLUMN IF EXISTS breaker_cooldown_ms,
DROP COLUMN IF EXISTS breaker_next_probe_at,
DROP COLUMN IF EXISTS breaker_opened_at,
DROP COLUMN IF EXISTS breaker_failure_count,
DROP COLUMN IF EXISTS breaker_state;
