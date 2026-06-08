CREATE INDEX IF NOT EXISTS idx_webhooks_event_types_gin
    ON webhooks USING GIN (event_types);
