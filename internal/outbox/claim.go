package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

// claimCtx returns a child context with s.claimTimeout applied, plus a cancel
// func. When claimTimeout is zero the original ctx and a no-op cancel are
// returned so callers can unconditionally defer cancel().
func (s *Store) claimCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if s.claimTimeout > 0 {
		return context.WithTimeout(parent, s.claimTimeout)
	}
	return parent, func() {}
}

// ClaimNextBatch claims up to limit ready Deliveries across the given customer
// IDs. It is a multi-customer generalization of ClaimNextN that amortises the
// DB round-trip when individual customers are sparse — the sparse-Customer
// workload #83 would otherwise pay one claim query per Customer.
//
// The return includes deliveries from any of the provided customers, without a
// per-Customer guarantee. Callers must re-notify every customer in the input
// set to keep them in rotation; empty-claim removal is left to the normal
// discovery/claim-evaluate path.
func (s *Store) ClaimNextBatch(ctx context.Context, customerIDs []string, limit int) ([]Delivery, error) {
	if len(customerIDs) == 0 {
		return nil, nil
	}
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}
	if !s.breakerFiltering {
		return s.claimNextBatchWithoutBreaker(ctx, customerIDs, limit)
	}
	return s.claimNextBatchWithBreaker(ctx, customerIDs, limit)
}

// perCustomerClaimLimit bounds how many candidate rows a single Customer may
// contribute to a multi-customer claim. Without it, each per-Customer LATERAL
// would lock up to the full batch limit, so n co-claimed Customers could lock
// up to n*limit rows FOR UPDATE only to discard most of them at the final cut —
// amplifying lock work and contention under whale bursts. The fair share is the
// batch limit spread across the input Customers, floored at 1.
func perCustomerClaimLimit(limit, numCustomers int) int {
	if numCustomers < 1 {
		return limit
	}
	share := limit / numCustomers
	if share < 1 {
		return 1
	}
	return share
}

func (s *Store) claimNextBatchWithoutBreaker(ctx context.Context, customerIDs []string, limit int) ([]Delivery, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()
	claimStarted := time.Now()
	perCustomerLimit := perCustomerClaimLimit(limit, len(customerIDs))

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH input_customers AS (
			SELECT DISTINCT customer_id
			FROM unnest($1::text[]) AS input(customer_id)
		),
		ready_candidate AS (
			SELECT picked.id, picked.attempt_count, picked.next_attempt_at, picked.created_at
			FROM input_customers ic
			CROSS JOIN LATERAL (
				SELECT d.id, d.attempt_count, d.next_attempt_at, d.created_at
				FROM deliveries d
				LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
				WHERE d.customer_id = ic.customer_id
				  AND %s
				ORDER BY d.next_attempt_at, d.created_at, d.id
				LIMIT $4
				FOR UPDATE OF d SKIP LOCKED
			) picked
		),
		stale_candidate AS (
			SELECT picked.id, picked.attempt_count, picked.next_attempt_at, picked.created_at
			FROM input_customers ic
			CROSS JOIN LATERAL (
				SELECT d.id, d.attempt_count, d.next_attempt_at, d.created_at
				FROM delivery_claims dc
				JOIN deliveries d ON d.id = dc.delivery_id
				WHERE d.customer_id = ic.customer_id
				  AND %s
				ORDER BY d.next_attempt_at, d.created_at, d.id
				LIMIT $4
				FOR UPDATE OF d SKIP LOCKED
			) picked
		),
		candidate_pool AS (
			SELECT id, attempt_count, next_attempt_at, created_at
			FROM ready_candidate
			UNION ALL
			SELECT id, attempt_count, next_attempt_at, created_at
			FROM stale_candidate
		),
		candidate AS (
			SELECT id, attempt_count
			FROM candidate_pool
			ORDER BY next_attempt_at, created_at, id
			LIMIT $3
		),
		claimed AS (
			INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
			SELECT c.id, c.attempt_count + 1, now()
			FROM candidate c
			ON CONFLICT (delivery_id) DO UPDATE
			    SET attempt_number = EXCLUDED.attempt_number,
			        claimed_at = now()
			    WHERE delivery_claims.claimed_at < now() - ($2 * interval '1 millisecond')
			RETURNING delivery_id, attempt_number, claimed_at
		)%s`, sqlReady, s.staleSQL, sqlClaimedDeliveryProjection), customerIDs, s.claimLease.Milliseconds(), limit, perCustomerLimit)
	if err != nil {
		return nil, fmt.Errorf("cte claim batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

	deliveries, err := scanClaimedDeliveries(rows)
	if err != nil {
		return nil, err
	}

	if len(deliveries) == 0 {
		observe.IncClaimEmpty()
	}
	observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
	observe.ObserveClaimBatchSize(float64(len(deliveries)))
	return deliveries, nil
}

func (s *Store) claimNextBatchWithBreaker(ctx context.Context, customerIDs []string, limit int) ([]Delivery, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()
	claimStarted := time.Now()
	perCustomerLimit := perCustomerClaimLimit(limit, len(customerIDs))

	breakerEligible := sqlBreakerEligible
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH input_customers AS (
			SELECT DISTINCT customer_id
			FROM unnest($1::text[]) AS input(customer_id)
		),
		ready_candidate AS (
			SELECT picked.id, picked.webhook_id, picked.attempt_count, picked.next_attempt_at, picked.created_at
			FROM input_customers ic
			CROSS JOIN LATERAL (
				SELECT d.id, d.webhook_id, d.attempt_count, d.next_attempt_at, d.created_at
				FROM deliveries d
				JOIN webhooks w ON w.id = d.webhook_id
				LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
				WHERE d.customer_id = ic.customer_id
				  AND %s
				  AND %s
				ORDER BY d.next_attempt_at, d.created_at, d.id
				LIMIT $4
				FOR UPDATE OF d SKIP LOCKED
			) picked
		),
		stale_candidate AS (
			SELECT picked.id, picked.webhook_id, picked.attempt_count, picked.next_attempt_at, picked.created_at
			FROM input_customers ic
			CROSS JOIN LATERAL (
				SELECT d.id, d.webhook_id, d.attempt_count, d.next_attempt_at, d.created_at
				FROM delivery_claims dc
				JOIN deliveries d ON d.id = dc.delivery_id
				JOIN webhooks w ON w.id = d.webhook_id
				WHERE d.customer_id = ic.customer_id
				  AND %s
				  AND %s
				ORDER BY d.next_attempt_at, d.created_at, d.id
				LIMIT $4
				FOR UPDATE OF d SKIP LOCKED
			) picked
		),
		candidate_pool AS (
			SELECT id, webhook_id, attempt_count, next_attempt_at, created_at
			FROM ready_candidate
			UNION ALL
			SELECT id, webhook_id, attempt_count, next_attempt_at, created_at
			FROM stale_candidate
		),
		candidate AS (
			SELECT id, webhook_id, attempt_count
			FROM candidate_pool
			ORDER BY next_attempt_at, created_at, id
			LIMIT $3
		),
		probe AS (
			UPDATE webhooks w
			SET breaker_state = 'half_open',
			    breaker_probe_delivery_id = c.id
			FROM candidate c
			WHERE w.id = c.webhook_id
			  AND w.breaker_state = 'open'
			  AND w.breaker_next_probe_at <= now()
			RETURNING c.id
		),
		claimed AS (
			INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
			SELECT c.id, c.attempt_count + 1, now()
			FROM candidate c
			JOIN webhooks w ON w.id = c.webhook_id
			WHERE w.breaker_state = 'closed'
			   OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = c.id)
			   OR c.id IN (SELECT id FROM probe)
			ON CONFLICT (delivery_id) DO UPDATE
			    SET attempt_number = EXCLUDED.attempt_number,
			        claimed_at = now()
			    WHERE delivery_claims.claimed_at < now() - ($2 * interval '1 millisecond')
			RETURNING delivery_id, attempt_number, claimed_at
		)
		%s`, sqlReady, breakerEligible, s.staleSQL, breakerEligible, sqlClaimedDeliveryProjection), customerIDs, s.claimLease.Milliseconds(), limit, perCustomerLimit)
	if err != nil {
		return nil, fmt.Errorf("cte claim batch with breaker: %w", err)
	}
	defer func() { _ = rows.Close() }()

	deliveries, err := scanClaimedDeliveries(rows)
	if err != nil {
		return nil, err
	}

	if len(deliveries) == 0 {
		observe.IncClaimEmpty()
	}
	observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
	observe.ObserveClaimBatchSize(float64(len(deliveries)))
	return deliveries, nil
}

// ClaimNext claims one ready Delivery for customerID using SELECT FOR UPDATE
// SKIP LOCKED. It returns ok=false when no ready Delivery exists.
func (s *Store) ClaimNext(ctx context.Context, customerID string) (delivery Delivery, ok bool, err error) {
	deliveries, err := s.ClaimNextN(ctx, customerID, 1)
	if err != nil {
		return Delivery{}, false, err
	}
	if len(deliveries) == 0 {
		return Delivery{}, false, nil
	}
	return deliveries[0], true, nil
}

// ClaimNextN claims up to limit ready Deliveries for customerID using a
// single query that atomically inserts a delivery_claims row and returns the
// claimed deliveries with their joined data. Eliminates the N+1 loop overhead
// and the dead-tuple bloat from the old UPDATE status='in_flight' approach.
func (s *Store) ClaimNextN(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}
	if !s.breakerFiltering {
		return s.claimNextNWithoutBreaker(ctx, customerID, limit)
	}

	ctx, cancel := s.claimCtx(ctx)
	defer cancel()

	claimStarted := time.Now()

	breakerEligible := sqlBreakerEligible
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH ready_candidate AS (
			SELECT d.id, d.webhook_id, d.attempt_count
			FROM deliveries d
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = $1
			  AND %s
			  AND %s
			ORDER BY d.next_attempt_at, d.created_at, d.id
			LIMIT $3
			FOR UPDATE OF d SKIP LOCKED
		),
		stale_candidate AS (
			SELECT d.id, d.webhook_id, d.attempt_count
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			JOIN webhooks w ON w.id = d.webhook_id
			WHERE d.customer_id = $1
			  AND %s
			  AND %s
			ORDER BY d.next_attempt_at, d.created_at, d.id
			LIMIT $3
			FOR UPDATE OF d SKIP LOCKED
		),
		candidate AS (
			SELECT * FROM ready_candidate
			UNION ALL
			SELECT * FROM stale_candidate
		),
		probe AS (
			UPDATE webhooks w
			SET breaker_state = 'half_open',
			    breaker_probe_delivery_id = c.id
			FROM candidate c
			WHERE w.id = c.webhook_id
			  AND w.breaker_state = 'open'
			  AND w.breaker_next_probe_at <= now()
			RETURNING c.id
		),
		claimed AS (
			INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
			SELECT c.id, c.attempt_count + 1, now()
			FROM candidate c
			JOIN webhooks w ON w.id = c.webhook_id
			WHERE w.breaker_state = 'closed'
			   OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = c.id)
			   OR c.id IN (SELECT id FROM probe)
			ON CONFLICT (delivery_id) DO UPDATE
			    SET attempt_number = EXCLUDED.attempt_number,
			        claimed_at = now()
			    WHERE delivery_claims.claimed_at < now() - ($2 * interval '1 millisecond')
			RETURNING delivery_id, attempt_number, claimed_at
		)
		%s`, sqlReady, breakerEligible, s.staleSQL, breakerEligible, sqlClaimedDeliveryProjection), customerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("cte claim: %w", err)
	}
	defer func() { _ = rows.Close() }()

	deliveries, err := scanClaimedDeliveries(rows)
	if err != nil {
		return nil, err
	}

	if len(deliveries) == 0 {
		observe.IncClaimEmpty()
	}
	observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
	observe.ObserveClaimBatchSize(float64(len(deliveries)))
	return deliveries, nil
}

func (s *Store) claimNextNWithoutBreaker(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()
	claimStarted := time.Now()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH ready_candidate AS (
			SELECT d.id, d.attempt_count
			FROM deliveries d
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = $1
			  AND %s
			ORDER BY d.next_attempt_at, d.created_at, d.id
			LIMIT $3
			FOR UPDATE OF d SKIP LOCKED
		),
		stale_candidate AS (
			SELECT d.id, d.attempt_count
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			WHERE d.customer_id = $1
			  AND %s
			ORDER BY d.next_attempt_at, d.created_at, d.id
			LIMIT $3
			FOR UPDATE OF d SKIP LOCKED
		),
		candidate AS (
			SELECT * FROM ready_candidate
			UNION ALL
			SELECT * FROM stale_candidate
		),
		claimed AS (
			INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
			SELECT c.id, c.attempt_count + 1, now()
			FROM candidate c
			ON CONFLICT (delivery_id) DO UPDATE
			    SET attempt_number = EXCLUDED.attempt_number,
			        claimed_at = now()
			    WHERE delivery_claims.claimed_at < now() - ($2 * interval '1 millisecond')
			RETURNING delivery_id, attempt_number, claimed_at
		)%s`, sqlReady, s.staleSQL, sqlClaimedDeliveryProjection), customerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim without breaker: %w", err)
	}
	defer func() { _ = rows.Close() }()

	deliveries, err := scanClaimedDeliveries(rows)
	if err != nil {
		return nil, err
	}
	if len(deliveries) == 0 {
		observe.IncClaimEmpty()
	}
	observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
	observe.ObserveClaimBatchSize(float64(len(deliveries)))
	return deliveries, nil
}

func scanClaimedDeliveries(rows *sql.Rows) ([]Delivery, error) {
	var deliveries []Delivery
	for rows.Next() {
		var d Delivery
		var previousDelayMillis sql.NullInt64
		if err := rows.Scan(
			&d.ID, &d.EventID, &d.WebhookID, &d.CustomerID,
			&d.CustomerClass,
			&d.EventType, &d.Payload, &d.URL, &d.URLHost, &d.Secret,
			&d.Status, &d.AttemptNumber, &d.NextAttemptAt, &d.ClaimedAt,
			&d.LastError, &d.CreatedAt, &d.UpdatedAt,
			&previousDelayMillis, &d.BreakerProbe,
		); err != nil {
			return nil, fmt.Errorf("scan claimed delivery: %w", err)
		}
		if previousDelayMillis.Valid {
			d.PreviousDelay = time.Duration(previousDelayMillis.Int64) * time.Millisecond
		}
		deliveries = append(deliveries, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed deliveries: %w", err)
	}
	return deliveries, nil
}

// DiscoverClaimableCustomers returns up to limit Customer IDs greater than
// afterCustomerID that have at least one ready, breaker-eligible Delivery.
//
// This is a claim-agnostic rotation hint, not an exact claimability check: it
// ignores whether ready Deliveries already carry an active claim. ClaimNextN
// (FOR UPDATE SKIP LOCKED) remains the authority on what is actually claimable;
// the worker loop drops customers that yield no claim via HasClaimableWork. The
// claim-agnostic predicate is what keeps discovery cheap under a large in-flight
// backlog (issue #124): claimed Deliveries stay status='pending' and remain in
// idx_deliveries_ready, so an anti-join against delivery_claims forced the
// recursive LIMIT-1 scan to walk every claimed row (collapsing into a full hash
// anti-join once unclaimed rows went sparse). Dropping the anti-join lets each
// step stop at the first ready row per customer.
func (s *Store) DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	ready := sqlReadyWithoutClaim
	breakerEligible := sqlBreakerEligible
	// Enumerate distinct claimable-hint customers with a recursive loose
	// index-scan (the PostgreSQL "loose indexscan" pattern) over
	// idx_deliveries_ready so that finding the next distinct customer costs
	// ~one index probe per customer instead of scanning the whole ready
	// backlog. A plain UNION/DISTINCT would materialise every ready delivery
	// row before the outer LIMIT, so its cost grows with the backlog row count
	// rather than the number of distinct customers returned (finding B-5). Each
	// step's LIMIT-1 subquery also naturally skips customers whose ready
	// deliveries are all breaker-ineligible. Stale-claim recovery needs no
	// dedicated branch here: a stale-claimed Delivery is still status='pending'
	// and so is already covered by the ready predicate. depth is capped at the
	// limit so the recursion stops once enough customers are found.
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH RECURSIVE ready_customers AS (
			SELECT seed.customer_id, 1 AS depth
			FROM (
				SELECT d.customer_id
				FROM deliveries d
				JOIN webhooks w ON w.id = d.webhook_id
				WHERE d.customer_id > $1
				  AND %s
				  AND %s
				ORDER BY d.customer_id
				LIMIT 1
			) seed
			UNION ALL
			SELECT
				(
					SELECT d.customer_id
					FROM deliveries d
					JOIN webhooks w ON w.id = d.webhook_id
					WHERE d.customer_id > rc.customer_id
					  AND %s
					  AND %s
					ORDER BY d.customer_id
					LIMIT 1
				),
				rc.depth + 1
			FROM ready_customers rc
			WHERE rc.customer_id IS NOT NULL
			  AND rc.depth < $2
		)
		SELECT customer_id
		FROM ready_customers
		WHERE customer_id IS NOT NULL
		ORDER BY customer_id
		LIMIT $2
	`, ready, breakerEligible, ready, breakerEligible), afterCustomerID, limit)
	if err != nil {
		return nil, fmt.Errorf("discover claimable customers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var customerIDs []string
	for rows.Next() {
		var customerID string
		if err := rows.Scan(&customerID); err != nil {
			return nil, fmt.Errorf("scan claimable customer: %w", err)
		}
		customerIDs = append(customerIDs, customerID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable customers: %w", err)
	}

	return customerIDs, nil
}

// HasClaimableWork reports whether the customer has any deliveries that are
// currently eligible for claim, matching ClaimNextN and discovery semantics.
func (s *Store) HasClaimableWork(ctx context.Context, customerID string) (bool, error) {
	return s.hasClaimableWorkSplit(ctx, customerID)
}

// HasClaimableWorkBatch is a multi-customer variant of HasClaimableWork that
// issues a single query. The returned map contains only customers that have
// at least one claimable Delivery; absent keys should be removed from
// rotation.
func (s *Store) HasClaimableWorkBatch(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	if len(customerIDs) == 0 {
		return nil, nil
	}
	if !s.breakerFiltering {
		return s.hasClaimableWorkBatchWithoutBreaker(ctx, customerIDs)
	}
	return s.hasClaimableWorkBatchWithBreaker(ctx, customerIDs)
}

func (s *Store) hasClaimableWorkBatchWithoutBreaker(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH claimable AS (
			SELECT DISTINCT d.customer_id
			FROM deliveries d
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = ANY($1)
			  AND %s
			UNION
			SELECT DISTINCT d.customer_id
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			WHERE d.customer_id = ANY($1)
			  AND %s
		)
		SELECT customer_id FROM claimable
	`, sqlReady, s.staleSQL), customerIDs)
	if err != nil {
		return nil, fmt.Errorf("has claimable work batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanClaimableWorkBatch(rows, len(customerIDs))
}

func (s *Store) hasClaimableWorkBatchWithBreaker(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()
	breakerEligible := sqlBreakerEligible
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH claimable AS (
			SELECT DISTINCT d.customer_id
			FROM deliveries d
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = ANY($1)
			  AND %s
			  AND %s
			UNION
			SELECT DISTINCT d.customer_id
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			JOIN webhooks w ON w.id = d.webhook_id
			WHERE d.customer_id = ANY($1)
			  AND %s
			  AND %s
		)
		SELECT customer_id FROM claimable
	`, sqlReady, breakerEligible, s.staleSQL, breakerEligible), customerIDs)
	if err != nil {
		return nil, fmt.Errorf("has claimable work batch with breaker: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanClaimableWorkBatch(rows, len(customerIDs))
}

func scanClaimableWorkBatch(rows *sql.Rows, capacity int) (map[string]bool, error) {
	result := make(map[string]bool, capacity)
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			return nil, fmt.Errorf("scan claimable work batch: %w", err)
		}
		result[cid] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable work batch: %w", err)
	}
	return result, nil
}

func (s *Store) hasClaimableWorkSplit(ctx context.Context, customerID string) (bool, error) {
	ctx, cancel := s.claimCtx(ctx)
	defer cancel()
	var exists bool
	if !s.breakerFiltering {
		err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT EXISTS (
				SELECT 1
				FROM deliveries d
				LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
				WHERE d.customer_id = $1
				  AND %s
			)
		`, s.anySQL), customerID).Scan(&exists)
		return exists, err
	}

	breakerEligible := sqlBreakerEligible
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM deliveries d
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = $1
			  AND %s
			  AND %s
		)
	`, s.anySQL, breakerEligible), customerID).Scan(&exists)
	return exists, err
}
