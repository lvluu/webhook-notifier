package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

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
		emitBreakerClosed(webhookID)
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
			    previous_delay_millis = $2,
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
		emitBreakerOpened(webhookID, tripReason)
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
		emitBreakerOpened(webhookID, tripReason)
	}
	return nil
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

// RequeueBatch returns a set of claimed Deliveries to the retry pool in a
// single transaction, collapsing what would otherwise be N per-delivery
// Requeue transactions during host-cap backpressure. It mirrors Requeue's
// semantics: claim rows are deleted, deliveries move to 'failed_retry' with
// next_attempt_at = now() + delay, and attempt_count is not advanced (no
// delivery_attempts row is inserted) because the retry budget only advances
// when a real HTTP POST is recorded.
//
// The implementation locks both the delivery and claim rows FOR UPDATE before
// touching them, matching the row-locking discipline of withInFlightDelivery.
// Deliveries that no longer hold a live claim (already finished or taken over
// by a stale-claim reclaim) are silently skipped — correct for the
// backpressure path where losing the race is harmless.
func (s *Store) RequeueBatch(ctx context.Context, deliveryIDs []string, delay time.Duration) error {
	if len(deliveryIDs) == 0 {
		return nil
	}
	if delay < 0 {
		return fmt.Errorf("delay must be non-negative")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, `
		WITH locked AS (
			SELECT d.id
			FROM deliveries d
			JOIN delivery_claims dc ON dc.delivery_id = d.id
			WHERE d.id = ANY($1)
			FOR UPDATE OF d, dc
		),
		deleted AS (
			DELETE FROM delivery_claims
			WHERE delivery_id IN (SELECT id FROM locked)
		)
		UPDATE deliveries
		SET status = 'failed_retry',
		    next_attempt_at = now() + ($2 * interval '1 millisecond'),
		    updated_at = now()
		WHERE id IN (SELECT id FROM locked)
	`, deliveryIDs, delay.Milliseconds()); err != nil {
		return fmt.Errorf("requeue batch: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit requeue batch: %w", err)
	}
	return nil
}
