package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// DeliveryStatus is the lifecycle state stored on a Delivery row.
type DeliveryStatus string

const (
	// StatusPending means the Delivery has not been attempted yet.
	StatusPending DeliveryStatus = "pending"
	// StatusInFlight means a worker has claimed the Delivery.
	StatusInFlight DeliveryStatus = "in_flight"
	// StatusSucceeded means the partner accepted the Delivery with 2xx.
	StatusSucceeded DeliveryStatus = "succeeded"
)

// Delivery is a claimed outbox row with enough data for the worker to POST it.
type Delivery struct {
	ID            string
	EventID       string
	WebhookID     string
	CustomerID    string
	EventType     string
	Payload       []byte
	URL           string
	Secret        string
	Status        DeliveryStatus
	AttemptNumber int
	NextAttemptAt time.Time
	ClaimedAt     time.Time
	LastError     sql.NullString
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Store claims and mutates Delivery rows in Postgres.
type Store struct {
	db *sql.DB
}

// New returns an outbox store backed by db.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// ClaimNext claims one ready Delivery for customerID using SELECT FOR UPDATE
// SKIP LOCKED. It returns ok=false when no ready Delivery exists.
func (s *Store) ClaimNext(ctx context.Context, customerID string) (delivery Delivery, ok bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Delivery{}, false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var deliveryID string
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM deliveries
		WHERE customer_id = $1
		  AND status IN ('pending', 'failed_retry')
		  AND next_attempt_at <= now()
		ORDER BY next_attempt_at, created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, customerID).Scan(&deliveryID)
	if errors.Is(err, sql.ErrNoRows) {
		return Delivery{}, false, nil
	}
	if err != nil {
		return Delivery{}, false, fmt.Errorf("select delivery for claim: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'in_flight',
		    attempt_count = attempt_count + 1,
		    claimed_at = now(),
		    updated_at = now()
		WHERE id = $1
	`, deliveryID)
	if err != nil {
		return Delivery{}, false, fmt.Errorf("mark delivery in flight: %w", err)
	}

	err = tx.QueryRowContext(ctx, `
		SELECT
		    deliveries.id,
		    deliveries.event_id,
		    deliveries.webhook_id,
		    deliveries.customer_id,
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
		    deliveries.updated_at
		FROM deliveries
		JOIN events ON events.id = deliveries.event_id
		JOIN webhooks ON webhooks.id = deliveries.webhook_id
		WHERE deliveries.id = $1
	`, deliveryID).Scan(
		&delivery.ID,
		&delivery.EventID,
		&delivery.WebhookID,
		&delivery.CustomerID,
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
	)
	if err != nil {
		return Delivery{}, false, fmt.Errorf("load claimed delivery: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return Delivery{}, false, fmt.Errorf("commit claim: %w", err)
	}

	return delivery, true, nil
}

// MarkSucceeded records the successful Attempt and moves the Delivery to
// succeeded.
func (s *Store) MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error {
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
