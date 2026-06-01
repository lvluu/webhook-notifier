package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lib/pq"
	"github.com/lvluu/webhook-notifier/internal/observe"
)

// DeliveryStatus is the lifecycle state stored on a Delivery row.
type DeliveryStatus string

const (
	// DefaultClaimLease is the period a worker has to finish a claimed Delivery
	// before another worker may recover it.
	DefaultClaimLease = 5 * time.Minute

	// StatusPending means the Delivery has not been attempted yet.
	StatusPending DeliveryStatus = "pending"
	// StatusInFlight means a worker has claimed the Delivery (logical status
	// returned to Go code; the delivery row itself never stores 'in_flight').
	StatusInFlight DeliveryStatus = "in_flight"
	// StatusFailedRetry means the Delivery is waiting for a retry.
	StatusFailedRetry DeliveryStatus = "failed_retry"
	// StatusSucceeded means the partner accepted the Delivery with 2xx.
	StatusSucceeded DeliveryStatus = "succeeded"
	// StatusDead means the Delivery reached a terminal failure.
	StatusDead DeliveryStatus = "dead"
)

// ArchiveEnrichmentMode controls how much related data retention loads during
// archive passes.
type ArchiveEnrichmentMode string

const (
	// ArchiveEnrichmentFull archives attempt-summary and related metadata.
	ArchiveEnrichmentFull ArchiveEnrichmentMode = "full"
	// ArchiveEnrichmentDeferred archives core fields and defers heavy enrichment.
	ArchiveEnrichmentDeferred ArchiveEnrichmentMode = "deferred"
)

const (
	// BreakerStateClosed means the Webhook is claimable normally.
	BreakerStateClosed = "closed"
	// BreakerStateOpen means Deliveries for the Webhook are skipped until the next probe time.
	BreakerStateOpen = "open"
	// BreakerStateHalfOpen means exactly one Delivery is allowed as a recovery probe.
	BreakerStateHalfOpen = "half_open"
)

// BreakerConfig controls persisted per-Webhook circuit breaker transitions.
// A FailureThreshold of 0 disables breaker tripping entirely.
type BreakerConfig struct {
	FailureThreshold int
	InitialCooldown  time.Duration
	MaxCooldown      time.Duration
}

func defaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold: 20,
		InitialCooldown:  5 * time.Minute,
		MaxCooldown:      15 * time.Minute,
	}
}

func normalizeBreakerConfig(config BreakerConfig) BreakerConfig {
	defaults := defaultBreakerConfig()
	if config.FailureThreshold < 0 {
		config.FailureThreshold = defaults.FailureThreshold
	}
	if config.InitialCooldown <= 0 {
		config.InitialCooldown = defaults.InitialCooldown
	}
	if config.MaxCooldown <= 0 {
		config.MaxCooldown = defaults.MaxCooldown
	}
	if config.MaxCooldown < config.InitialCooldown {
		config.MaxCooldown = config.InitialCooldown
	}
	return config
}

// Delivery is a claimed outbox row with enough data for the worker to POST it.
type Delivery struct {
	ID            string
	EventID       string
	WebhookID     string
	CustomerID    string
	CustomerClass uint32
	EventType     string
	Payload       []byte
	URL           string
	Secret        string
	Status        DeliveryStatus
	AttemptNumber int
	PreviousDelay time.Duration
	NextAttemptAt time.Time
	ClaimedAt     time.Time
	LastError     sql.NullString
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Store claims and mutates Delivery rows in Postgres.
type Store struct {
	db            *sql.DB
	claimLease    time.Duration
	breakerConfig BreakerConfig
}

// Option configures a Store.
type Option func(*Store)

// WithClaimLease configures how long an in-flight Delivery can remain claimed
// before another worker may recover it.
func WithClaimLease(claimLease time.Duration) Option {
	return func(s *Store) {
		if claimLease > 0 {
			s.claimLease = claimLease
		}
	}
}

// WithBreakerConfig configures per-Webhook circuit breaker transitions.
func WithBreakerConfig(config BreakerConfig) Option {
	return func(s *Store) {
		s.breakerConfig = normalizeBreakerConfig(config)
	}
}

// New returns an outbox store backed by db.
func New(db *sql.DB, opts ...Option) *Store {
	store := &Store{
		db:            db,
		claimLease:    DefaultClaimLease,
		breakerConfig: defaultBreakerConfig(),
	}
	for _, opt := range opts {
		opt(store)
	}

	return store
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

// ClaimNextN claims up to limit ready Deliveries for customerID using a single
// CTE that atomically inserts a delivery_claims row and returns the claimed
// deliveries with their joined data. Eliminates the N+1 loop overhead and the
// dead-tuple bloat from the old UPDATE status='in_flight' approach.
func (s *Store) ClaimNextN(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}
	if s.breakerConfig.FailureThreshold == 0 {
		return s.claimNextNWithoutBreaker(ctx, customerID, limit)
	}

	claimStarted := time.Now()

	rows, err := s.db.QueryContext(ctx, `
		WITH candidate AS (
			SELECT d.id, d.webhook_id, d.attempt_count
			FROM deliveries d
			JOIN webhooks w ON w.id = d.webhook_id
			WHERE d.customer_id = $1
			  AND (
			      (d.status IN ('pending', 'failed_retry') AND d.next_attempt_at <= now()
			       AND NOT EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id))
			      OR EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id AND dc.claimed_at < now() - ($2 * interval '1 millisecond'))
			  )
			  AND (
			      w.breaker_state = 'closed'
			      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
			      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
			)
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
	`, customerID, s.claimLease.Milliseconds(), limit)
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
	claimStarted := time.Now()

	rows, err := s.db.QueryContext(ctx, `
		WITH candidate AS (
			SELECT d.id, d.attempt_count
			FROM deliveries d
			WHERE d.customer_id = $1
			  AND (
			      (d.status IN ('pending', 'failed_retry') AND d.next_attempt_at <= now()
			       AND NOT EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id))
			      OR EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id AND dc.claimed_at < now() - ($2 * interval '1 millisecond'))
			  )
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
// Uses a recursive CTE loose index scan to jump between distinct customer_id
// values rather than scanning every row for SELECT DISTINCT, which is
// critical when whales have 50K+ deliveries.
func (s *Store) DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	rows, err := s.db.QueryContext(ctx, `
		WITH RECURSIVE distinct_customers AS (
			(
				SELECT d.customer_id, 1 AS rn
				FROM deliveries d
				JOIN webhooks w ON w.id = d.webhook_id
				WHERE d.customer_id > $1
				  AND (
				      (d.status IN ('pending', 'failed_retry') AND d.next_attempt_at <= now()
				       AND NOT EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id))
				      OR EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id AND dc.claimed_at < now() - ($2 * interval '1 millisecond'))
				  )
				  AND (
				      w.breaker_state = 'closed'
				      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
				      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
				  )
				ORDER BY d.customer_id
				LIMIT 1
			)
			UNION ALL
			(
				SELECT d.customer_id, dc.rn + 1
				FROM distinct_customers dc
				JOIN deliveries d ON d.customer_id > dc.customer_id
				JOIN webhooks w ON w.id = d.webhook_id
				WHERE d.customer_id > dc.customer_id
				  AND (
				      (d.status IN ('pending', 'failed_retry') AND d.next_attempt_at <= now()
				       AND NOT EXISTS (SELECT 1 FROM delivery_claims dc2 WHERE dc2.delivery_id = d.id))
				      OR EXISTS (SELECT 1 FROM delivery_claims dc2 WHERE dc2.delivery_id = d.id AND dc2.claimed_at < now() - ($2 * interval '1 millisecond'))
				  )
				  AND (
				      w.breaker_state = 'closed'
				      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
				      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
				  )
				ORDER BY d.customer_id
				LIMIT 1
			)
		)
		SELECT customer_id FROM distinct_customers
		WHERE rn <= $3
	`, afterCustomerID, s.claimLease.Milliseconds(), limit)
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
	query := `
		WITH terminal_candidates AS (
			SELECT d.id AS delivery_id, COALESCE(dl.final_at, d.updated_at) AS final_at
			FROM deliveries d
			LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
			WHERE d.status IN ('succeeded', 'dead')
			  AND COALESCE(dl.final_at, d.updated_at) < now() - ($1 * interval '1 millisecond')
			ORDER BY COALESCE(dl.final_at, d.updated_at), d.id
			LIMIT $2
			FOR UPDATE OF d SKIP LOCKED
		),`
	if mode == ArchiveEnrichmentFull {
		query += `
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
	} else {
		query += `
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
	}
	query += `
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
	err = tx.QueryRowContext(ctx, query, retention.Milliseconds(), limit).Scan(&archived)
	if err != nil {
		return 0, fmt.Errorf("archive terminal deliveries: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit archive transaction: %w", err)
	}

	return archived, nil
}

// MarkSucceeded records the successful Attempt and moves the Delivery to
// succeeded.
func (s *Store) MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}

	var webhookID string
	var breakerClosed bool
	err := s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, wid string) error {
		webhookID = wid
		if _, err := tx.ExecContext(ctx, `DELETE FROM delivery_claims WHERE delivery_id = $1`, deliveryID); err != nil {
			return fmt.Errorf("delete claim: %w", err)
		}

		_, err := tx.ExecContext(ctx, `
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, duration_ms, sent_at)
			VALUES ($1, $2, $3, $4, now())
		`, deliveryID, attemptNumber, statusCode, duration.Milliseconds())
		if err != nil {
			return fmt.Errorf("insert delivery attempt: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE deliveries
			SET status = 'succeeded',
			    attempt_count = $2,
			    updated_at = now(),
			    last_error = NULL
			WHERE id = $1
		`, deliveryID, attemptNumber)
		if err != nil {
			return fmt.Errorf("mark delivery succeeded: %w", err)
		}

		closed, err := s.recordBreakerSuccess(ctx, tx, wid, deliveryID)
		if closed {
			breakerClosed = true
		}
		return err
	})
	if err != nil {
		return err
	}
	if breakerClosed {
		observe.SetBreakerState(webhookID, BreakerStateClosed)
	}
	return nil
}

// MarkRetry records a failed Attempt and schedules the Delivery for retry.
func (s *Store) MarkRetry(ctx context.Context, deliveryID string, statusCode int, errorClass string, delay time.Duration, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	var webhookID string
	var breakerTripped bool
	var tripReason string
	err := s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, wid string) error {
		webhookID = wid
		tripReason = errorClass
		if _, err := tx.ExecContext(ctx, `DELETE FROM delivery_claims WHERE delivery_id = $1`, deliveryID); err != nil {
			return fmt.Errorf("delete claim: %w", err)
		}

		if err := insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, errorClass, duration); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET status = 'failed_retry',
			    attempt_count = $4,
			    next_attempt_at = now() + ($2 * interval '1 millisecond'),
			    last_error = $3,
			    updated_at = now()
			WHERE id = $1
		`, deliveryID, delay.Milliseconds(), nullIfEmpty(errorClass), attemptNumber)
		if err != nil {
			return fmt.Errorf("mark delivery failed retry: %w", err)
		}

		tripped, err := s.recordBreakerFailure(ctx, tx, wid, errorClass)
		if tripped {
			breakerTripped = true
		}
		return err
	})
	if err != nil {
		return err
	}
	if breakerTripped {
		slog.Default().Warn("webhook circuit breaker opened", "webhook_id", webhookID, "reason", tripReason)
		observe.SetBreakerState(webhookID, BreakerStateOpen)
		observe.IncBreakerTrip(tripReason)
	}
	return nil
}

// MarkDead records the final failed Attempt, moves the Delivery to dead, and
// inserts a DLQ row with the terminal reason.
func (s *Store) MarkDead(ctx context.Context, deliveryID string, statusCode int, errorClass string, reason string, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if reason == "" {
		return fmt.Errorf("reason is required")
	}

	var webhookID string
	var breakerTripped bool
	var tripReason string
	err := s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, wid string) error {
		webhookID = wid
		tripReason = reason
		if _, err := tx.ExecContext(ctx, `DELETE FROM delivery_claims WHERE delivery_id = $1`, deliveryID); err != nil {
			return fmt.Errorf("delete claim: %w", err)
		}

		if err := insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, errorClass, duration); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'dead',
		    attempt_count = $2,
		    last_error = $3,
		    updated_at = now()
		WHERE id = $1
	`, deliveryID, attemptNumber, nullIfEmpty(errorClass))
		if err != nil {
			return fmt.Errorf("mark delivery dead: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
		INSERT INTO dead_letters (delivery_id, last_error, attempts, reason, final_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (delivery_id) DO UPDATE
		SET last_error = EXCLUDED.last_error,
		    attempts = EXCLUDED.attempts,
		    reason = EXCLUDED.reason,
		    final_at = EXCLUDED.final_at
	`, deliveryID, nullIfEmpty(errorClass), attemptNumber, reason)
		if err != nil {
			return fmt.Errorf("insert dead letter: %w", err)
		}

		tripped, err := s.recordBreakerFailure(ctx, tx, wid, reason)
		if tripped {
			breakerTripped = true
		}
		return err
	})
	if err != nil {
		return err
	}
	if breakerTripped {
		slog.Default().Warn("webhook circuit breaker opened", "webhook_id", webhookID, "reason", tripReason)
		observe.SetBreakerState(webhookID, BreakerStateOpen)
		observe.IncBreakerTrip(tripReason)
	}
	return nil
}

// BatchMarkItem is one delivery outcome to be applied in a batched mark operation.
type BatchMarkItem struct {
	DeliveryID string
	StatusCode int
	Duration   time.Duration
	ErrorClass string
	Delay      time.Duration
	Reason     string

	// Observation-only fields (not used in batch SQL).
	Lifetime      time.Duration
	AttemptNumber int
}

// MarkSucceededBatch marks multiple deliveries as succeeded in one round trip.
// Updates delivery status and inserts attempt records via a single CTE.
func (s *Store) MarkSucceededBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}

	ids := make([]string, len(items))
	statusCodes := make([]int, len(items))
	durationsMs := make([]int64, len(items))
	for i, item := range items {
		if item.DeliveryID == "" {
			return fmt.Errorf("delivery_id is required at index %d", i)
		}
		ids[i] = item.DeliveryID
		statusCodes[i] = item.StatusCode
		durationsMs[i] = item.Duration.Milliseconds()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch success transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var marked int
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[]
			) AS t(id, status_code, duration_ms)
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id::uuid
			ORDER BY d.id
			FOR UPDATE OF d
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id::uuid
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id, dc.attempt_number, b.status_code, b.duration_ms
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'succeeded',
			    attempt_count = dc.attempt_number,
			    updated_at = now(),
			    last_error = NULL
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.attempt_number, dc.status_code, dc.duration_ms
		),
		inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, duration_ms, now() FROM updated
			RETURNING delivery_id
		)
		SELECT COUNT(*) FROM inserted
	`, pq.Array(ids), pq.Array(statusCodes), pq.Array(durationsMs)).Scan(&marked)
	if err != nil {
		return fmt.Errorf("batch mark succeeded: %w", err)
	}
	if marked != len(items) {
		return fmt.Errorf("batch mark succeeded: marked %d of %d deliveries", marked, len(items))
	}
	if s.breakerConfig.FailureThreshold == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit batch success transaction: %w", err)
		}
		return nil
	}
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, ids)
	if err != nil {
		return err
	}
	var closedBreakers []string
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		closed, err := s.recordBreakerSuccess(ctx, tx, wid, item.DeliveryID)
		if err != nil {
			return err
		}
		if closed {
			closedBreakers = append(closedBreakers, wid)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch success transaction: %w", err)
	}
	for _, wid := range closedBreakers {
		observe.SetBreakerState(wid, BreakerStateClosed)
	}
	return nil
}

// MarkRetryBatch schedules multiple deliveries for retry in one round trip.
func (s *Store) MarkRetryBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}

	ids := make([]string, len(items))
	statusCodes := make([]int, len(items))
	durationsMs := make([]int64, len(items))
	errorClasses := make([]string, len(items))
	delaysMs := make([]int64, len(items))
	for i, item := range items {
		if item.DeliveryID == "" {
			return fmt.Errorf("delivery_id is required at index %d", i)
		}
		ids[i] = item.DeliveryID
		statusCodes[i] = item.StatusCode
		durationsMs[i] = item.Duration.Milliseconds()
		errorClasses[i] = item.ErrorClass
		delaysMs[i] = item.Delay.Milliseconds()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch retry transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var marked int
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[], $4::text[], $5::bigint[]
			) AS t(id, status_code, duration_ms, error_class, delay_ms)
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id::uuid
			ORDER BY d.id
			FOR UPDATE OF d
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id::uuid
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id, dc.attempt_number, b.status_code, b.duration_ms, b.error_class, b.delay_ms
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'failed_retry',
			    attempt_count = dc.attempt_number,
			    next_attempt_at = now() + (dc.delay_ms * interval '1 millisecond'),
			    last_error = NULLIF(dc.error_class, ''),
			    updated_at = now()
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.attempt_number, dc.status_code, dc.duration_ms, dc.error_class
		),
		inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, NULLIF(error_class, ''), duration_ms, now() FROM updated
			RETURNING delivery_id
		)
		SELECT COUNT(*) FROM inserted
	`, pq.Array(ids), pq.Array(statusCodes), pq.Array(durationsMs), pq.Array(errorClasses), pq.Array(delaysMs)).Scan(&marked)
	if err != nil {
		return fmt.Errorf("batch mark retry: %w", err)
	}
	if marked != len(items) {
		return fmt.Errorf("batch mark retry: marked %d of %d deliveries", marked, len(items))
	}
	if s.breakerConfig.FailureThreshold == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit batch retry transaction: %w", err)
		}
		return nil
	}
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, ids)
	if err != nil {
		return err
	}
	var trippedBreakers []struct {
		webhookID string
		reason    string
	}
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		tripped, err := s.recordBreakerFailure(ctx, tx, wid, item.ErrorClass)
		if err != nil {
			return err
		}
		if tripped {
			trippedBreakers = append(trippedBreakers, struct {
				webhookID string
				reason    string
			}{wid, item.ErrorClass})
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch retry transaction: %w", err)
	}
	for _, ev := range trippedBreakers {
		slog.Default().Warn("webhook circuit breaker opened", "webhook_id", ev.webhookID, "reason", ev.reason)
		observe.SetBreakerState(ev.webhookID, BreakerStateOpen)
		observe.IncBreakerTrip(ev.reason)
	}
	return nil
}

// MarkDeadBatch marks multiple deliveries as dead in one round trip, recording
// dead_letter rows for each.
func (s *Store) MarkDeadBatch(ctx context.Context, items []BatchMarkItem) error {
	if len(items) == 0 {
		return nil
	}

	ids := make([]string, len(items))
	statusCodes := make([]int, len(items))
	durationsMs := make([]int64, len(items))
	errorClasses := make([]string, len(items))
	reasons := make([]string, len(items))
	for i, item := range items {
		if item.DeliveryID == "" {
			return fmt.Errorf("delivery_id is required at index %d", i)
		}
		if item.Reason == "" {
			return fmt.Errorf("reason is required at index %d", i)
		}
		ids[i] = item.DeliveryID
		statusCodes[i] = item.StatusCode
		durationsMs[i] = item.Duration.Milliseconds()
		errorClasses[i] = item.ErrorClass
		reasons[i] = item.Reason
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch dead transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var attemptsMarked, deadLettersMarked int
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[], $4::text[], $5::text[]
			) AS t(id, status_code, duration_ms, error_class, reason)
		),
		locked_deliveries AS (
			SELECT d.id
			FROM deliveries d
			JOIN batch b ON d.id = b.id::uuid
			ORDER BY d.id
			FOR UPDATE OF d
		),
		deleted_claims AS (
			DELETE FROM delivery_claims dc
			USING batch b
			JOIN locked_deliveries ld ON ld.id = b.id::uuid
			WHERE dc.delivery_id = ld.id
			RETURNING dc.delivery_id, dc.attempt_number, b.status_code, b.duration_ms, b.error_class, b.reason
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'dead',
			    attempt_count = dc.attempt_number,
			    last_error = NULLIF(dc.error_class, ''),
			    updated_at = now()
			FROM deleted_claims dc
			WHERE d.id = dc.delivery_id
			RETURNING d.id, dc.attempt_number, dc.status_code, dc.duration_ms, dc.error_class, dc.reason
		),
		attempts_inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_number, status_code, NULLIF(error_class, ''), duration_ms, now() FROM updated
			RETURNING delivery_id
		),
		dead_letters_inserted AS (
			INSERT INTO dead_letters (delivery_id, last_error, attempts, reason, final_at)
			SELECT id, NULLIF(error_class, ''), attempt_number, reason, now() FROM updated
			RETURNING delivery_id
		)
		SELECT
			(SELECT COUNT(*) FROM attempts_inserted),
			(SELECT COUNT(*) FROM dead_letters_inserted)
	`, pq.Array(ids), pq.Array(statusCodes), pq.Array(durationsMs), pq.Array(errorClasses), pq.Array(reasons)).Scan(&attemptsMarked, &deadLettersMarked)
	if err != nil {
		return fmt.Errorf("batch mark dead: %w", err)
	}
	if attemptsMarked != len(items) || deadLettersMarked != len(items) {
		return fmt.Errorf("batch mark dead: marked %d attempts and %d dead letters of %d deliveries", attemptsMarked, deadLettersMarked, len(items))
	}
	if s.breakerConfig.FailureThreshold == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit batch dead transaction: %w", err)
		}
		return nil
	}
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, ids)
	if err != nil {
		return err
	}
	var trippedBreakers []struct {
		webhookID string
		reason    string
	}
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		tripped, err := s.recordBreakerFailure(ctx, tx, wid, item.Reason)
		if err != nil {
			return err
		}
		if tripped {
			trippedBreakers = append(trippedBreakers, struct {
				webhookID string
				reason    string
			}{wid, item.Reason})
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch dead transaction: %w", err)
	}
	for _, ev := range trippedBreakers {
		slog.Default().Warn("webhook circuit breaker opened", "webhook_id", ev.webhookID, "reason", ev.reason)
		observe.SetBreakerState(ev.webhookID, BreakerStateOpen)
		observe.IncBreakerTrip(ev.reason)
	}
	return nil
}

// HasClaimableWork reports whether the customer has any deliveries that are
// currently eligible for claim, matching ClaimNextN and discovery semantics.
func (s *Store) HasClaimableWork(ctx context.Context, customerID string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM deliveries d
			WHERE d.customer_id = $1
			  AND (
			      (d.status IN ('pending', 'failed_retry') AND d.next_attempt_at <= now()
			       AND NOT EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id))
			      OR EXISTS (SELECT 1 FROM delivery_claims dc WHERE dc.delivery_id = d.id AND dc.claimed_at < now() - ($2 * interval '1 millisecond'))
			  )
			  AND EXISTS (
			      SELECT 1
			      FROM webhooks w
			      WHERE w.id = d.webhook_id
			        AND (
			            w.breaker_state = 'closed'
			            OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
			            OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
			        )
			  )
		)
	`, customerID, s.claimLease.Milliseconds()).Scan(&exists)
	return exists, err
}

// Requeue returns a claimed Delivery to the retry pool without recording a
// delivery_attempt row. The claim row is deleted and the delivery stays at its
// current attempt_count because the retry budget only advances when a real HTTP
// POST is recorded in delivery_attempts.
func (s *Store) Requeue(ctx context.Context, deliveryID string, delay time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	return s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int, _ string) error {
		_, err := tx.ExecContext(ctx, `
			DELETE FROM delivery_claims WHERE delivery_id = $1
		`, deliveryID)
		if err != nil {
			return fmt.Errorf("delete claim: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE deliveries
			SET status = 'failed_retry',
			    next_attempt_at = now() + ($2 * interval '1 millisecond'),
			    updated_at = now()
			WHERE id = $1
		`, deliveryID, delay.Milliseconds())
		if err != nil {
			return fmt.Errorf("requeue delivery: %w", err)
		}
		return nil
	})
}

func (s *Store) withInFlightDelivery(ctx context.Context, deliveryID string, mutate func(*sql.Tx, int, string) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var attemptNumber int
	var webhookID string
	err = tx.QueryRowContext(ctx, `
		SELECT dc.attempt_number, d.webhook_id::text
		FROM delivery_claims dc
		JOIN deliveries d ON d.id = dc.delivery_id
		WHERE dc.delivery_id = $1
		FOR UPDATE OF dc, d
	`, deliveryID).Scan(&attemptNumber, &webhookID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("delivery %s is not in flight", deliveryID)
	}
	if err != nil {
		return fmt.Errorf("load in-flight delivery: %w", err)
	}

	if err := mutate(tx, attemptNumber, webhookID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delivery mutation: %w", err)
	}

	return nil
}

func insertAttempt(ctx context.Context, tx *sql.Tx, deliveryID string, attemptNumber int, statusCode int, errorClass string, duration time.Duration) error {
	var nullableStatus sql.NullInt64
	if statusCode > 0 {
		nullableStatus = sql.NullInt64{Int64: int64(statusCode), Valid: true}
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
		VALUES ($1, $2, $3, $4, $5, now())
	`, deliveryID, attemptNumber, nullableStatus, nullIfEmpty(errorClass), duration.Milliseconds())
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}

	return nil
}

// recordBreakerSuccess updates the breaker to closed. Returns true if the
// breaker state actually changed (the UPDATE matched a row).
func (s *Store) recordBreakerSuccess(ctx context.Context, tx *sql.Tx, webhookID string, deliveryID string) (bool, error) {
	if s.breakerConfig.FailureThreshold == 0 {
		return false, nil
	}

	var updated string
	err := tx.QueryRowContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'closed',
		    breaker_failure_count = 0,
		    breaker_opened_at = NULL,
		    breaker_next_probe_at = NULL,
		    breaker_cooldown_ms = $2,
		    breaker_probe_delivery_id = NULL
		WHERE id = $1::uuid
		  AND breaker_state <> 'closed'
		  AND NOT (
		      breaker_state = 'half_open'
		      AND breaker_probe_delivery_id IS DISTINCT FROM $3::uuid
		  )
		RETURNING id
	`, webhookID, s.breakerConfig.InitialCooldown.Milliseconds(), deliveryID).Scan(&updated)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record breaker success: %w", err)
	}
	slog.Default().Info("webhook circuit breaker closed", "webhook_id", webhookID)
	return true, nil
}

// recordBreakerFailure assesses the failure against the breaker state and
// opens the breaker if the consecutive-failure threshold is met. Returns true
// if the breaker was opened as a result of this call.
func (s *Store) recordBreakerFailure(ctx context.Context, tx *sql.Tx, webhookID string, reason string) (bool, error) {
	if s.breakerConfig.FailureThreshold == 0 {
		return false, nil
	}

	var state string
	var failureCount int
	var cooldownMillis int64
	err := tx.QueryRowContext(ctx, `
		SELECT breaker_state, breaker_failure_count, breaker_cooldown_ms
		FROM webhooks
		WHERE id = $1::uuid
	`, webhookID).Scan(&state, &failureCount, &cooldownMillis)
	if err != nil {
		return false, fmt.Errorf("load webhook breaker: %w", err)
	}

	switch state {
	case BreakerStateHalfOpen:
		cooldown := nextBreakerCooldown(time.Duration(cooldownMillis)*time.Millisecond, s.breakerConfig)
		return s.openBreaker(ctx, tx, webhookID, failureCount+1, cooldown, reason)
	case BreakerStateClosed:
		failureCount, err = consecutiveWebhookFailures(ctx, tx, webhookID, s.breakerConfig.FailureThreshold+1)
		if err != nil {
			return false, err
		}
		if failureCount >= s.breakerConfig.FailureThreshold {
			return s.openBreaker(ctx, tx, webhookID, failureCount, s.breakerConfig.InitialCooldown, reason)
		}
	}
	return false, nil
}

// batchDeliveryWebhookIDs returns a map of deliveryID → webhookID for all
// given IDs in a single round trip, avoiding N+1 in batch mark paths.
func batchDeliveryWebhookIDs(ctx context.Context, tx *sql.Tx, deliveryIDs []string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, webhook_id::text
		FROM deliveries
		WHERE id = ANY($1)
	`, pq.Array(deliveryIDs))
	if err != nil {
		return nil, fmt.Errorf("load delivery webhook ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	ids := make(map[string]string, len(deliveryIDs))
	for rows.Next() {
		var id, webhookID string
		if err := rows.Scan(&id, &webhookID); err != nil {
			return nil, fmt.Errorf("scan delivery webhook id: %w", err)
		}
		ids[id] = webhookID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery webhook ids: %w", err)
	}
	return ids, nil
}

func consecutiveWebhookFailures(ctx context.Context, tx *sql.Tx, webhookID string, limit int) (int, error) {
	var failures int
	err := tx.QueryRowContext(ctx, `
		WITH latest_success AS (
			SELECT max(da.sent_at) AS sent_at
			FROM delivery_attempts da
			JOIN deliveries d ON d.id = da.delivery_id
			WHERE d.webhook_id = $1::uuid
			  AND da.status_code >= 200
			  AND da.status_code < 300
		)
		SELECT count(*) FROM (
			SELECT 1
			FROM delivery_attempts da
			JOIN deliveries d ON d.id = da.delivery_id
			CROSS JOIN latest_success ls
			WHERE d.webhook_id = $1::uuid
			  AND da.sent_at > COALESCE(ls.sent_at, '-infinity'::timestamptz)
			  AND (
			      da.error_class IS NOT NULL
			      OR da.status_code IS NULL
			      OR da.status_code < 200
			      OR da.status_code >= 300
			  )
			LIMIT $2
		) sub
	`, webhookID, limit).Scan(&failures)
	if err != nil {
		return 0, fmt.Errorf("count consecutive webhook failures: %w", err)
	}
	return failures, nil
}

// openBreaker transitions a webhook breaker from non-open to open. Returns
// true if the state actually changed (the row was updated). Callers are
// responsible for emitting log/metrics after the enclosing transaction commits.
func (s *Store) openBreaker(ctx context.Context, tx *sql.Tx, webhookID string, failureCount int, cooldown time.Duration, reason string) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'open',
		    breaker_failure_count = $2,
		    breaker_opened_at = now(),
		    breaker_next_probe_at = now() + ($3 * interval '1 millisecond'),
		    breaker_cooldown_ms = $3,
		    breaker_probe_delivery_id = NULL
		WHERE id = $1::uuid
		  AND breaker_state <> 'open'
	`, webhookID, failureCount, cooldown.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("open breaker: %w", err)
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

func nextBreakerCooldown(current time.Duration, config BreakerConfig) time.Duration {
	if current <= 0 {
		return config.InitialCooldown
	}
	next := current * 2
	if next > config.MaxCooldown {
		return config.MaxCooldown
	}
	return next
}

func nullIfEmpty(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}
