package outbox

import (
	"context"
	"fmt"
	"time"
)

// ArchiveTerminalDeliveries copies old terminal Deliveries into Delivery History
// and removes them from the live queue in one transaction.
func (s *Store) ArchiveTerminalDeliveries(ctx context.Context, retention time.Duration, limit int) (int, error) {
	return s.ArchiveTerminalDeliveriesWithMode(ctx, retention, limit, ArchiveEnrichmentFull)
}

// ArchiveTerminalDeliveriesWithMode copies old terminal Deliveries into Delivery
// History and removes them from the live queue in one transaction.
func (s *Store) ArchiveTerminalDeliveriesWithMode(ctx context.Context, retention time.Duration, limit int, mode ArchiveEnrichmentMode) (int, error) {
	if retention <= 0 {
		return 0, fmt.Errorf("retention must be > 0")
	}
	if limit < 1 {
		return 0, fmt.Errorf("limit must be >= 1")
	}
	if mode != ArchiveEnrichmentFull && mode != ArchiveEnrichmentDeferred {
		return 0, fmt.Errorf("archive mode must be one of %q or %q", ArchiveEnrichmentFull, ArchiveEnrichmentDeferred)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin archive transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var archived int
	const baseArchiveQueryPrefix = `
		WITH terminal_candidates AS (
			SELECT d.id AS delivery_id, COALESCE(dl.final_at, d.updated_at) AS final_at
			FROM deliveries d
			LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
			WHERE d.status IN ('succeeded', 'dead')
			  AND d.updated_at < now() - ($1 * interval '1 millisecond')
			ORDER BY d.updated_at, d.id
			LIMIT $2
			FOR UPDATE OF d SKIP LOCKED
		),`
	const fullCandidatesCTE = `
		candidates AS (
			SELECT
			    tc.delivery_id,
			    d.event_id,
			    d.webhook_id,
			    d.customer_id,
			    e.type AS event_type,
			    COALESCE(substring(w.url from '^[a-zA-Z][a-zA-Z0-9+.-]*://([^/:?#]+)'), w.url) AS url_host,
			    d.status AS final_status,
			    dl.reason AS terminal_reason,
			    d.attempt_count,
			    attempts.first_attempt_at,
			    attempts.last_attempt_at,
			    attempts.last_status_code,
			    attempts.last_error_class,
			    d.last_error,
			    d.created_at AS delivery_created_at,
			    tc.final_at
			FROM terminal_candidates tc
			JOIN deliveries d ON d.id = tc.delivery_id
			JOIN events e ON e.id = d.event_id
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
			LEFT JOIN LATERAL (
			    SELECT
			        MIN(da.sent_at) AS first_attempt_at,
			        MAX(da.sent_at) AS last_attempt_at,
			        (ARRAY_AGG(da.status_code ORDER BY da.attempt_number DESC))[1] AS last_status_code,
			        (ARRAY_AGG(da.error_class ORDER BY da.attempt_number DESC))[1] AS last_error_class
			    FROM delivery_attempts da
			    WHERE da.delivery_id = d.id
			) attempts ON true
		),`
	const deferredCandidatesCTE = `
		candidates AS (
			SELECT
			    tc.delivery_id,
			    d.event_id,
			    d.webhook_id,
			    d.customer_id,
			    e.type AS event_type,
			    COALESCE(substring(w.url from '^[a-zA-Z][a-zA-Z0-9+.-]*://([^/:?#]+)'), w.url) AS url_host,
			    d.status AS final_status,
			    dl.reason AS terminal_reason,
			    d.attempt_count,
			    NULL::timestamptz AS first_attempt_at,
			    NULL::timestamptz AS last_attempt_at,
			    NULL::integer AS last_status_code,
			    NULL::text AS last_error_class,
			    d.last_error,
			    d.created_at AS delivery_created_at,
			    tc.final_at
			FROM terminal_candidates tc
			JOIN deliveries d ON d.id = tc.delivery_id
			JOIN events e ON e.id = d.event_id
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
		),`
	const baseArchiveQuerySuffix = `
		inserted AS (
			INSERT INTO delivery_history (
			    delivery_id,
			    event_id,
			    webhook_id,
			    customer_id,
			    event_type,
			    url_host,
			    final_status,
			    terminal_reason,
			    attempt_count,
			    first_attempt_at,
			    last_attempt_at,
			    last_status_code,
			    last_error_class,
			    last_error,
			    delivery_created_at,
			    final_at
			)
			SELECT
			    delivery_id,
			    event_id,
			    webhook_id,
			    customer_id,
			    event_type,
			    url_host,
			    final_status,
			    terminal_reason,
			    attempt_count,
			    first_attempt_at,
			    last_attempt_at,
			    last_status_code,
			    last_error_class,
			    last_error,
			    delivery_created_at,
			    final_at
			FROM candidates
			ON CONFLICT (delivery_id, final_at) DO NOTHING
			RETURNING delivery_id
		),
		deleted AS (
			DELETE FROM deliveries d
			USING inserted i
			WHERE d.id = i.delivery_id
			RETURNING d.id
		)
		SELECT COUNT(*) FROM deleted
	`
	candidatesCTE := deferredCandidatesCTE
	if mode == ArchiveEnrichmentFull {
		candidatesCTE = fullCandidatesCTE
	}
	query := baseArchiveQueryPrefix + candidatesCTE + baseArchiveQuerySuffix
	err = tx.QueryRowContext(ctx, query, retention.Milliseconds(), limit).Scan(&archived)
	if err != nil {
		return 0, fmt.Errorf("archive terminal deliveries: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit archive transaction: %w", err)
	}

	return archived, nil
}
