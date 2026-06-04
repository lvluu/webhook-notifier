-- Persist the last applied retry delay on the deliveries row (issue #112,
-- finding B-2).
--
-- The shared claim projection (internal/outbox/sql_fragments.go) computed
-- previous_delay_millis with a correlated subquery into delivery_attempts per
-- claimed row: the most recent attempt's (next_attempt_at - sent_at). On
-- retry-heavy batches that adds a per-row indexed lookup + sort-limit to the
-- hottest query in the system, precisely during a partner degradation when
-- claim volume peaks.
--
-- MarkRetryBatch sets next_attempt_at = now() + delay_ms and inserts the
-- attempt with sent_at = now() in the same statement; now() is
-- transaction-constant, so (next_attempt_at - sent_at) recovers exactly
-- delay_ms. Storing delay_ms here lets the claim path read it back directly
-- with no delivery_attempts access and no change to the jitter value.
--
-- Nullable with no default: never-retried deliveries (attempt_count = 0) carry
-- NULL, matching the old CASE gate.
ALTER TABLE deliveries
    ADD COLUMN last_delay_millis bigint;
