package outbox

import (
	"context"
	"fmt"
	"time"
)

// ArchiveTerminalDeliveries copies old terminal Deliveries into Delivery History
// and removes them from the live queue in two transactions: a short scan-tx that
// locks and identifies candidate IDs, then a separate delete-tx that enriches
// and moves them. This prevents a long-running transaction from pinning the
// MVCC horizon across the enrichment joins.
func (s *Store) ArchiveTerminalDeliveries(ctx context.Context, retention time.Duration, limit int) (int, error) {
	return s.ArchiveTerminalDeliveriesWithMode(ctx, retention, limit, ArchiveEnrichmentFull)
}

// ArchiveTerminalDeliveriesWithMode copies old terminal Deliveries into Delivery
// History and removes them from the live queue in two transactions.
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

	ids, err := s.scanTerminalCandidates(ctx, retention, limit)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	return s.deleteTerminalCandidates(ctx, ids, mode)
}

// Lock ordering note: this function locks only deliveries (via FOR UPDATE OF d),
// never delivery_claims. Both the claim and archive paths acquire deliveries first,
// consistent with the ordering documented in DESIGN.md:290-291. No lock-order
// deadlock (40P01) risk exists between archive and concurrent claim operations.
func (s *Store) scanTerminalCandidates(ctx context.Context, retention time.Duration, limit int) ([]string, error) {
	const query = `
		SELECT d.id
		FROM deliveries d
		WHERE d.status IN ('succeeded', 'dead')
		  AND d.updated_at < now() - ($1 * interval '1 millisecond')
		ORDER BY d.updated_at, d.id
		LIMIT $2
		FOR UPDATE OF d SKIP LOCKED
	`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin archive scan: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, query, retention.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("scan terminal candidates: %w", err)
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan delivery id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close scan rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit archive scan: %w", err)
	}

	return ids, nil
}

func (s *Store) deleteTerminalCandidates(ctx context.Context, ids []string, mode ArchiveEnrichmentMode) (int, error) {
	const fullCandidatesCTE = `
		WITH candidates AS (
			SELECT
			    d.id AS delivery_id,
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
			    COALESCE(dl.final_at, d.updated_at) AS final_at
			FROM deliveries d
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
			WHERE d.id = ANY($1)
		),`
	const deferredCandidatesCTE = `
		WITH candidates AS (
			SELECT
			    d.id AS delivery_id,
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
			    COALESCE(dl.final_at, d.updated_at) AS final_at
			FROM deliveries d
			JOIN events e ON e.id = d.event_id
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
			WHERE d.id = ANY($1)
		),`
	const deleteSuffix = `
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin archive delete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var archived int
	if err := tx.QueryRowContext(ctx, candidatesCTE+deleteSuffix, ids).Scan(&archived); err != nil {
		return 0, fmt.Errorf("archive delete: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit archive delete: %w", err)
	}

	return archived, nil
}
