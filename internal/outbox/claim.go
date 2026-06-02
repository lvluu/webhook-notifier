package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

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
		    (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) AS previous_delay_millis
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
		    (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) AS previous_delay_millis
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

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
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
		(
		    SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		    FROM delivery_attempts da
		    WHERE da.delivery_id = d.id
		    ORDER BY da.attempt_number DESC
		    LIMIT 1
		) AS previous_delay_millis
		FROM claimed dc
		JOIN deliveries d ON d.id = dc.delivery_id
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN customers cust ON cust.id = d.customer_id
		ORDER BY d.next_attempt_at, d.created_at, d.id
		`), customerID, s.claimLease.Milliseconds(), limit)
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
		    (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (d.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = d.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) AS previous_delay_millis
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
func (s *Store) HasClaimableWork(ctx context.Context, customerID string) (bool, error) {
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
