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
	`, deliveryIDs)
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
