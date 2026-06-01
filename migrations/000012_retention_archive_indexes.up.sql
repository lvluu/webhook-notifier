-- Retention archive query support indexes (issue #45).
--
-- 1) Fast terminal candidate scan for succeeded/dead rows ordered by final-time fallback.
-- 2) Fast final_at lookup for dead rows when dead_letters.final_at is present.

CREATE INDEX IF NOT EXISTS idx_deliveries_terminal_by_updated_at
    ON deliveries (updated_at, id)
    WHERE status IN ('succeeded', 'dead');

CREATE INDEX IF NOT EXISTS idx_dead_letters_final_at_delivery
    ON dead_letters (final_at, delivery_id);
