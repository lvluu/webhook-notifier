package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
	// StatusInFlight means a worker has claimed the Delivery.
	StatusInFlight DeliveryStatus = "in_flight"
	// StatusFailedRetry means the Delivery is waiting for a retry.
	StatusFailedRetry DeliveryStatus = "failed_retry"
	// StatusSucceeded means the partner accepted the Delivery with 2xx.
	StatusSucceeded DeliveryStatus = "succeeded"
	// StatusDead means the Delivery reached a terminal failure.
	StatusDead DeliveryStatus = "dead"
)

// Delivery is a claimed outbox row with enough data for the worker to POST it.
type Delivery struct {
	ID             string
	EventID        string
	WebhookID      string
	CustomerID     string
	CustomerWeight uint32
	EventType      string
	Payload        []byte
	URL            string
	Secret         string
	Status         DeliveryStatus
	AttemptNumber  int
	PreviousDelay  time.Duration
	NextAttemptAt  time.Time
	ClaimedAt      time.Time
	LastError      sql.NullString
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Store claims and mutates Delivery rows in Postgres.
type Store struct {
	db         *sql.DB
	claimLease time.Duration
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

// New returns an outbox store backed by db.
func New(db *sql.DB, opts ...Option) *Store {
	store := &Store{
		db:         db,
		claimLease: DefaultClaimLease,
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

// ClaimNextN claims up to limit ready Deliveries for customerID using SELECT
// FOR UPDATE SKIP LOCKED. Returns an empty slice when no ready Delivery exists.
func (s *Store) ClaimNextN(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	claimStarted := time.Now()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	deliveryIDs, err := s.selectClaimableDeliveryIDs(ctx, tx, customerID, limit)
	if err != nil {
		return nil, err
	}
	if len(deliveryIDs) == 0 {
		observe.IncClaimEmpty()
		observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
		observe.ObserveClaimBatchSize(0)
		return nil, nil
	}

	var deliveries []Delivery
	for _, id := range deliveryIDs {
		if err := s.markInFlight(ctx, tx, id); err != nil {
			return nil, err
		}
		delivery, err := loadDelivery(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	observe.ObserveClaimDuration(time.Since(claimStarted).Seconds())
	observe.ObserveClaimBatchSize(float64(len(deliveries)))
	return deliveries, nil
}

// DiscoverClaimableCustomers returns up to limit Customer IDs greater than
// afterCustomerID that have at least one Delivery eligible for claim.
func (s *Store) DiscoverClaimableCustomers(ctx context.Context, afterCustomerID string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT customer_id
		FROM deliveries
		WHERE customer_id > $1
		  AND (
		      (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
		      OR (status = 'in_flight' AND claimed_at < now() - ($2 * interval '1 millisecond'))
		  )
		ORDER BY customer_id
		LIMIT $3
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

func (s *Store) selectClaimableDeliveryIDs(ctx context.Context, tx *sql.Tx, customerID string, limit int) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM deliveries
		WHERE customer_id = $1
		  AND (
		      (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
		      OR (status = 'in_flight' AND claimed_at < now() - ($2 * interval '1 millisecond'))
		  )
		ORDER BY next_attempt_at, created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $3
	`, customerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("select deliveries for claim: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan delivery id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery ids: %w", err)
	}

	return ids, nil
}

func (s *Store) markInFlight(ctx context.Context, tx *sql.Tx, deliveryID string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'in_flight',
		    attempt_count = attempt_count + 1,
		    claimed_at = now(),
		    updated_at = now()
		WHERE id = $1
	`, deliveryID)
	if err != nil {
		return fmt.Errorf("mark delivery in flight: %w", err)
	}

	return nil
}

func loadDelivery(ctx context.Context, tx *sql.Tx, deliveryID string) (Delivery, error) {
	var delivery Delivery
	var previousDelayMillis sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT
		    deliveries.id,
		    deliveries.event_id,
		    deliveries.webhook_id,
		    deliveries.customer_id,
		    COALESCE(customers.weight, 1),
		    events.type,
		    events.payload,
		    webhooks.url,
		    webhooks.secret_encrypted,
		    deliveries.status,
		    deliveries.attempt_count,
		    deliveries.next_attempt_at,
		    deliveries.claimed_at,
		    deliveries.last_error,
		    deliveries.created_at,
		    deliveries.updated_at,
		    (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (deliveries.next_attempt_at - delivery_attempts.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts
		        WHERE delivery_attempts.delivery_id = deliveries.id
		        ORDER BY delivery_attempts.attempt_number DESC
		        LIMIT 1
		    ) AS previous_delay_millis
		FROM deliveries
		JOIN events ON events.id = deliveries.event_id
		JOIN webhooks ON webhooks.id = deliveries.webhook_id
		LEFT JOIN customers ON customers.id = deliveries.customer_id
		WHERE deliveries.id = $1
	`, deliveryID).Scan(
		&delivery.ID,
		&delivery.EventID,
		&delivery.WebhookID,
		&delivery.CustomerID,
		&delivery.CustomerWeight,
		&delivery.EventType,
		&delivery.Payload,
		&delivery.URL,
		&delivery.Secret,
		&delivery.Status,
		&delivery.AttemptNumber,
		&delivery.NextAttemptAt,
		&delivery.ClaimedAt,
		&delivery.LastError,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
		&previousDelayMillis,
	)
	if err != nil {
		return Delivery{}, fmt.Errorf("load claimed delivery: %w", err)
	}
	if previousDelayMillis.Valid {
		delivery.PreviousDelay = time.Duration(previousDelayMillis.Int64) * time.Millisecond
	}

	return delivery, nil
}

// MarkSucceeded records the successful Attempt and moves the Delivery to
// succeeded.
func (s *Store) MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var attemptNumber int
	err = tx.QueryRowContext(ctx, `
		SELECT attempt_count
		FROM deliveries
		WHERE id = $1
		  AND status = 'in_flight'
		FOR UPDATE
	`, deliveryID).Scan(&attemptNumber)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("delivery %s is not in flight", deliveryID)
	}
	if err != nil {
		return fmt.Errorf("load in-flight delivery: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, duration_ms, sent_at)
		VALUES ($1, $2, $3, $4, now())
	`, deliveryID, attemptNumber, statusCode, duration.Milliseconds())
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'succeeded',
		    updated_at = now(),
		    last_error = NULL
		WHERE id = $1
	`, deliveryID)
	if err != nil {
		return fmt.Errorf("mark delivery succeeded: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit success: %w", err)
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

	return s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int) error {
		if err := insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, errorClass, duration); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET status = 'failed_retry',
			    next_attempt_at = now() + ($2 * interval '1 millisecond'),
			    claimed_at = NULL,
			    last_error = $3,
			    updated_at = now()
			WHERE id = $1
		`, deliveryID, delay.Milliseconds(), nullIfEmpty(errorClass))
		if err != nil {
			return fmt.Errorf("mark delivery failed retry: %w", err)
		}

		return nil
	})
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

	return s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, attemptNumber int) error {
		if err := insertAttempt(ctx, tx, deliveryID, attemptNumber, statusCode, errorClass, duration); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'dead',
		    claimed_at = NULL,
		    last_error = $2,
		    updated_at = now()
		WHERE id = $1
	`, deliveryID, nullIfEmpty(errorClass))
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

		return nil
	})
}

// Requeue returns a claimed (in_flight) Delivery to the retry pool without
// recording a delivery_attempt row. The attempt_count is decremented to cancel
// the claim-time increment, preserving the invariant that attempt_count
// reflects real HTTP POSTs against the partner endpoint (ADR-0004).
func (s *Store) Requeue(ctx context.Context, deliveryID string, delay time.Duration) error {
	if deliveryID == "" {
		return fmt.Errorf("delivery_id is required")
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	return s.withInFlightDelivery(ctx, deliveryID, func(tx *sql.Tx, _ int) error {
		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET status = 'failed_retry',
			    attempt_count = attempt_count - 1,
			    next_attempt_at = now() + ($2 * interval '1 millisecond'),
			    claimed_at = NULL,
			    updated_at = now()
			WHERE id = $1
		`, deliveryID, delay.Milliseconds())
		if err != nil {
			return fmt.Errorf("requeue delivery: %w", err)
		}
		return nil
	})
}

func (s *Store) withInFlightDelivery(ctx context.Context, deliveryID string, mutate func(*sql.Tx, int) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var attemptNumber int
	err = tx.QueryRowContext(ctx, `
		SELECT attempt_count
		FROM deliveries
		WHERE id = $1
		  AND status = 'in_flight'
		FOR UPDATE
	`, deliveryID).Scan(&attemptNumber)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("delivery %s is not in flight", deliveryID)
	}
	if err != nil {
		return fmt.Errorf("load in-flight delivery: %w", err)
	}

	if err := mutate(tx, attemptNumber); err != nil {
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

func nullIfEmpty(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}
