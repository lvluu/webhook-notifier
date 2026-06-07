package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

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
		WITH locked_delivery AS (
			SELECT id, webhook_id::text
			FROM deliveries
			WHERE id = $1
			FOR UPDATE
		),
		locked_claim AS (
			SELECT dc.attempt_number, ld.webhook_id
			FROM locked_delivery ld
			JOIN delivery_claims dc ON dc.delivery_id = ld.id
			FOR UPDATE OF dc
		)
		SELECT attempt_number, webhook_id
		FROM locked_claim
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
