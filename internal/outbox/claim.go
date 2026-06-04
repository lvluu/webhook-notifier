package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

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
	if s.breakerConfig.FailureThreshold == 0 {
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
	claimStarted := time.Now()
	perCustomerLimit := perCustomerClaimLimit(limit, len(customerIDs))

	rows, err := s.db.QueryContext(ctx, `
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
				  AND d.status IN ('pending', 'failed_retry')
				  AND d.next_attempt_at <= now()
				  AND dc.delivery_id IS NULL
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
				  AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
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
		)
		SELECT
		    d.id, d.event_id, d.webhook_id, d.customer_id,
		    COALESCE(cust.class, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    'in_flight' AS status,
		    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
		    d.last_error, d.created_at, d.updated_at,
		    CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) END AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
	`, customerIDs, s.claimLease.Milliseconds(), limit, perCustomerLimit)
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
	claimStarted := time.Now()
	perCustomerLimit := perCustomerClaimLimit(limit, len(customerIDs))

	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
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
				  AND d.status IN ('pending', 'failed_retry')
				  AND d.next_attempt_at <= now()
				  AND dc.delivery_id IS NULL
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
				  AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
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
		SELECT
		    d.id, d.event_id, d.webhook_id, d.customer_id,
		    COALESCE(cust.class, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    'in_flight' AS status,
		    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
		    d.last_error, d.created_at, d.updated_at,
		    CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) END AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
	`, breakerEligible, breakerEligible), customerIDs, s.claimLease.Milliseconds(), limit, perCustomerLimit)
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
	if s.claimMode == ClaimQueryModeLegacy {
		return s.claimNextNLegacy(ctx, customerID, limit)
	}
	if s.breakerConfig.FailureThreshold == 0 {
		return s.claimNextNWithoutBreaker(ctx, customerID, limit)
	}

	claimStarted := time.Now()

	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH ready_candidate AS (
			SELECT d.id, d.webhook_id, d.attempt_count
			FROM deliveries d
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = $1
			  AND d.status IN ('pending', 'failed_retry')
			  AND d.next_attempt_at <= now()
			  AND dc.delivery_id IS NULL
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
			  AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
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
		SELECT
		    d.id, d.event_id, d.webhook_id, d.customer_id,
		    COALESCE(cust.class, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    'in_flight' AS status,
		    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
		    d.last_error, d.created_at, d.updated_at,
		    CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) END AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
	`, breakerEligible, breakerEligible), customerID, s.claimLease.Milliseconds(), limit)
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

func (s *Store) claimNextNLegacy(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	if s.breakerConfig.FailureThreshold == 0 {
		return s.claimNextNLegacyWithoutBreaker(ctx, customerID, limit)
	}

	claimStarted := time.Now()

	claimable := sqlClaimablePredicate("d", "dc")
	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH candidate AS (
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
		SELECT
		    d.id, d.event_id, d.webhook_id, d.customer_id,
		    COALESCE(cust.class, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    'in_flight' AS status,
		    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
		    d.last_error, d.created_at, d.updated_at,
		    CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) END AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
	`, claimable, breakerEligible), customerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("legacy claim: %w", err)
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
	claimStarted := time.Now()

	rows, err := s.db.QueryContext(ctx, `
		WITH ready_candidate AS (
			SELECT d.id, d.attempt_count
			FROM deliveries d
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = $1
			  AND d.status IN ('pending', 'failed_retry')
			  AND d.next_attempt_at <= now()
			  AND dc.delivery_id IS NULL
			ORDER BY d.next_attempt_at, d.created_at, d.id
			LIMIT $3
			FOR UPDATE OF d SKIP LOCKED
		),
		stale_candidate AS (
			SELECT d.id, d.attempt_count
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			WHERE d.customer_id = $1
			  AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
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
		)
		SELECT
		    d.id, d.event_id, d.webhook_id, d.customer_id,
		    COALESCE(cust.class, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    'in_flight' AS status,
		    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
		d.last_error, d.created_at, d.updated_at,
		CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
		    SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		    FROM delivery_attempts da
		    WHERE da.delivery_id = d.id
		    ORDER BY da.attempt_number DESC
		    LIMIT 1
		) END AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
		`, customerID, s.claimLease.Milliseconds(), limit)
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
			&d.EventType, &d.Payload, &d.URL, &d.Secret,
			&d.Status, &d.AttemptNumber, &d.NextAttemptAt, &d.ClaimedAt,
			&d.LastError, &d.CreatedAt, &d.UpdatedAt,
			&previousDelayMillis,
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
// afterCustomerID that have at least one Delivery eligible for claim.
//
// Uses two branch-specific scans - one for ready rows without an active claim
// and one for stale claims - so discovery stays index-friendly without a
// recursive loose-index walk.
func (s *Store) DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}
	if s.claimMode == ClaimQueryModeLegacy {
		return s.discoverClaimableCustomersLegacy(ctx, afterCustomerID, limit)
	}

	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH claimable_customers AS (
			SELECT d.customer_id
			FROM deliveries d
			JOIN webhooks w ON w.id = d.webhook_id
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id > $1
			  AND d.status IN ('pending', 'failed_retry')
			  AND d.next_attempt_at <= now()
			  AND dc.delivery_id IS NULL
			  AND %s
			UNION
			SELECT d.customer_id
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			JOIN webhooks w ON w.id = d.webhook_id
			WHERE d.customer_id > $1
			  AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
			  AND %s
		)
		SELECT customer_id
		FROM claimable_customers
		ORDER BY customer_id
		LIMIT $3
	`, breakerEligible, breakerEligible), afterCustomerID, s.claimLease.Milliseconds(), limit)
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

func (s *Store) claimNextNLegacyWithoutBreaker(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	claimStarted := time.Now()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH candidate AS (
			SELECT d.id, d.attempt_count
			FROM deliveries d
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = $1
			  AND %s
			ORDER BY d.next_attempt_at, d.created_at, d.id
			LIMIT $3
			FOR UPDATE OF d SKIP LOCKED
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
		)
		SELECT
		    d.id, d.event_id, d.webhook_id, d.customer_id,
		    COALESCE(cust.class, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    'in_flight' AS status,
		    dc.attempt_number, d.next_attempt_at, dc.claimed_at,
		    d.last_error, d.created_at, d.updated_at,
		    CASE WHEN d.attempt_count = 0 THEN NULL ELSE (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) END AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
		`, sqlClaimablePredicate("d", "dc")), customerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("legacy claim without breaker: %w", err)
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

func (s *Store) discoverClaimableCustomersLegacy(ctx context.Context, afterCustomerID string, limit int) ([]string, error) {
	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT d.customer_id
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
		WHERE d.customer_id > $1
		  AND %s
		  AND %s
		ORDER BY d.customer_id
		LIMIT $3
	`, sqlClaimablePredicate("d", "dc"), breakerEligible), afterCustomerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("discover claimable customers legacy: %w", err)
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
// The split and legacy query modes share this semantic check on purpose even
// though their claim/discovery SQL shapes differ.
func (s *Store) HasClaimableWork(ctx context.Context, customerID string) (bool, error) {
	if s.claimMode == ClaimQueryModeLegacy {
		return s.hasClaimableWorkLegacy(ctx, customerID)
	}
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

	rows, err := s.db.QueryContext(ctx, `
		WITH claimable AS (
			SELECT DISTINCT d.customer_id
			FROM deliveries d
			LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.customer_id = ANY($1)
			  AND d.status IN ('pending', 'failed_retry')
			  AND d.next_attempt_at <= now()
			  AND dc.delivery_id IS NULL
			UNION
			SELECT DISTINCT d.customer_id
			FROM delivery_claims dc
			JOIN deliveries d ON d.id = dc.delivery_id
			WHERE d.customer_id = ANY($1)
			  AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
		)
		SELECT customer_id FROM claimable
	`, customerIDs, s.claimLease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("has claimable work batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]bool, len(customerIDs))
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
	var exists bool
	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM deliveries d
			WHERE d.customer_id = $1
			  AND (
			      (
			          d.status IN ('pending', 'failed_retry')
			          AND d.next_attempt_at <= now()
			          AND NOT EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id)
			      )
			      OR EXISTS (
			          SELECT 1
			          FROM delivery_claims dc
			          WHERE dc.delivery_id = d.id
			            AND dc.claimed_at < now() - ($2 * interval '1 millisecond')
			      )
			  )
			  AND EXISTS (
			      SELECT 1
			      FROM webhooks w
			      WHERE w.id = d.webhook_id
			        AND %s
			  )
		)
	`, breakerEligible), customerID, s.claimLease.Milliseconds()).Scan(&exists)
	return exists, err
}

func (s *Store) hasClaimableWorkLegacy(ctx context.Context, customerID string) (bool, error) {
	var exists bool
	claimable := sqlClaimablePredicate("d", "dc")
	breakerEligible := sqlBreakerEligiblePredicate("w", "d")
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM deliveries d
			WHERE d.customer_id = $1
			  AND %s
			  AND EXISTS (
			      SELECT 1
			      FROM webhooks w
			      WHERE w.id = d.webhook_id
			        AND %s
			  )
		)
	`, claimable, breakerEligible), customerID, s.claimLease.Milliseconds()).Scan(&exists)
	return exists, err
}
