package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

// ClaimNextN claims up to limit ready Deliveries for customerID using a single
// CTE that atomically marks rows in_flight and returns them with their joined
// data. Eliminates the N+1 loop overhead of the original per-row update+select.
func (s *Store) ClaimNextN(ctx context.Context, customerID string, limit int) ([]Delivery, error) {
	if customerID == "" {
		return nil, fmt.Errorf("customer_id is required")
	}
	if limit < 1 {
		return nil, fmt.Errorf("limit must be >= 1")
	}

	claimStarted := time.Now()

	rows, err := s.db.QueryContext(ctx, `
		WITH claimed AS (
			UPDATE deliveries
			SET status = 'in_flight',
			    attempt_count = attempt_count + 1,
			    claimed_at = now(),
			    updated_at = now()
			WHERE id IN (
				SELECT id FROM deliveries
				WHERE customer_id = $1
				  AND (
				      (status IN ('pending', 'failed_retry') AND next_attempt_at <= now())
				      OR (status = 'in_flight' AND claimed_at < now() - ($2 * interval '1 millisecond'))
				  )
				ORDER BY next_attempt_at, created_at, id
				LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			RETURNING *
		)
		SELECT
		    c.id, c.event_id, c.webhook_id, c.customer_id,
		    COALESCE(cust.weight, 1),
		    e.type, e.payload, w.url, w.secret_encrypted,
		    c.status, c.attempt_count, c.next_attempt_at, c.claimed_at,
		    c.last_error, c.created_at, c.updated_at,
		    (
		        SELECT GREATEST(EXTRACT(EPOCH FROM (c.next_attempt_at - da.sent_at)) * 1000, 0)::bigint
		        FROM delivery_attempts da
		        WHERE da.delivery_id = c.id
		        ORDER BY da.attempt_number DESC
		        LIMIT 1
		    ) AS previous_delay_millis
		FROM claimed c
		JOIN events e ON e.id = c.event_id
		JOIN webhooks w ON w.id = c.webhook_id
		LEFT JOIN customers cust ON cust.id = c.customer_id
		ORDER BY c.next_attempt_at, c.created_at, c.id
	`, customerID, s.claimLease.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("cte claim: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var deliveries []Delivery
	for rows.Next() {
		var d Delivery
		var previousDelayMillis sql.NullInt64
		if err := rows.Scan(
			&d.ID, &d.EventID, &d.WebhookID, &d.CustomerID,
			&d.CustomerWeight,
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

	if len(deliveries) == 0 {
		observe.IncClaimEmpty()
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

	var marked int
	err := s.db.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[]
			) AS t(id, status_code, duration_ms)
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'succeeded',
			    updated_at = now(),
			    last_error = NULL
			FROM batch b
			WHERE d.id = b.id::uuid
			  AND d.status = 'in_flight'
			RETURNING d.id, d.attempt_count, b.status_code, b.duration_ms
		)
		, inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, duration_ms, sent_at)
			SELECT id, attempt_count, status_code, duration_ms, now() FROM updated
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

	var marked int
	err := s.db.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[], $4::text[], $5::bigint[]
			) AS t(id, status_code, duration_ms, error_class, delay_ms)
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'failed_retry',
			    next_attempt_at = now() + (b.delay_ms * interval '1 millisecond'),
			    claimed_at = NULL,
			    last_error = NULLIF(b.error_class, ''),
			    updated_at = now()
			FROM batch b
			WHERE d.id = b.id::uuid
			  AND d.status = 'in_flight'
			RETURNING d.id, d.attempt_count, b.status_code, b.duration_ms, b.error_class
		)
		, inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_count, status_code, NULLIF(error_class, ''), duration_ms, now() FROM updated
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

	var attemptsMarked, deadLettersMarked int
	err := s.db.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT * FROM unnest(
				$1::text[], $2::int[], $3::bigint[], $4::text[], $5::text[]
			) AS t(id, status_code, duration_ms, error_class, reason)
		),
		updated AS (
			UPDATE deliveries d
			SET status = 'dead',
			    claimed_at = NULL,
			    last_error = NULLIF(b.error_class, ''),
			    updated_at = now()
			FROM batch b
			WHERE d.id = b.id::uuid
			  AND d.status = 'in_flight'
			RETURNING d.id, d.attempt_count, b.status_code, b.duration_ms, b.error_class, b.reason
		),
		attempts_inserted AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at)
			SELECT id, attempt_count, status_code, NULLIF(error_class, ''), duration_ms, now() FROM updated
			RETURNING delivery_id
		),
		dead_letters_inserted AS (
			INSERT INTO dead_letters (delivery_id, last_error, attempts, reason, final_at)
			SELECT id, NULLIF(error_class, ''), attempt_count, reason, now() FROM updated
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
	return nil
}

// HasPendingWork reports whether the customer has any deliveries in a
// non-terminal state (pending, in_flight, or failed_retry). Used by workers to
// decide whether to keep a customer in scheduler rotation after a claim batch
// returns empty — e.g. when all remaining deliveries are in retry backoff.
func (s *Store) HasPendingWork(ctx context.Context, customerID string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM deliveries
			WHERE customer_id = $1
			  AND status NOT IN ('succeeded', 'dead')
		)
	`, customerID).Scan(&exists)
	return exists, err
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
