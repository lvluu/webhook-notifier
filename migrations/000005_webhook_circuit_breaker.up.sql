ALTER TABLE webhooks
ADD COLUMN breaker_state text NOT NULL DEFAULT 'closed'
    CHECK (breaker_state IN ('closed', 'open', 'half_open')),
ADD COLUMN breaker_failure_count integer NOT NULL DEFAULT 0
    CHECK (breaker_failure_count >= 0),
ADD COLUMN breaker_opened_at timestamptz,
ADD COLUMN breaker_next_probe_at timestamptz,
ADD COLUMN breaker_cooldown_ms bigint NOT NULL DEFAULT 300000
    CHECK (breaker_cooldown_ms >= 0),
ADD COLUMN breaker_probe_delivery_id uuid;

CREATE INDEX idx_webhooks_breaker_probe
    ON webhooks (breaker_state, breaker_next_probe_at)
    WHERE breaker_state = 'open';
