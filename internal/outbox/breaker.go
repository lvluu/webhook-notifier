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

type breakerStateRow struct {
	webhookID       string
	state           string
	failureCount    int
	cooldownMillis  int64
	probeDeliveryID sql.NullString
}

func batchLoadBreakerStates(ctx context.Context, tx *sql.Tx, webhookIDs []string) (map[string]breakerStateRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, breaker_state, breaker_failure_count,
		       breaker_cooldown_ms, breaker_probe_delivery_id::text
		FROM webhooks
		WHERE id = ANY($1::uuid[])
	`, webhookIDs)
	if err != nil {
		return nil, fmt.Errorf("batch load breaker states: %w", err)
	}
	defer func() { _ = rows.Close() }()

	states := make(map[string]breakerStateRow, len(webhookIDs))
	for rows.Next() {
		var r breakerStateRow
		if err := rows.Scan(&r.webhookID, &r.state, &r.failureCount, &r.cooldownMillis, &r.probeDeliveryID); err != nil {
			return nil, fmt.Errorf("scan breaker state: %w", err)
		}
		states[r.webhookID] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate breaker states: %w", err)
	}
	return states, nil
}

func batchConsecutiveWebhookFailures(ctx context.Context, tx *sql.Tx, webhookIDs []string, limit int) (map[string]int, error) {
	rows, err := tx.QueryContext(ctx, `
		WITH latest_success AS (
			SELECT d.webhook_id, max(da.sent_at) AS last_success_at
			FROM delivery_attempts da
			JOIN deliveries d ON d.id = da.delivery_id
			WHERE d.webhook_id = ANY($1::uuid[])
			  AND da.status_code >= 200
			  AND da.status_code < 300
			GROUP BY d.webhook_id
		),
		failure_attempts AS (
			SELECT d.webhook_id, da.sent_at
			FROM delivery_attempts da
			JOIN deliveries d ON d.id = da.delivery_id
			LEFT JOIN latest_success ls ON ls.webhook_id = d.webhook_id
			WHERE d.webhook_id = ANY($1::uuid[])
			  AND da.sent_at > COALESCE(ls.last_success_at, '-infinity'::timestamptz)
			  AND (
			      da.error_class IS NOT NULL
			      OR da.status_code IS NULL
			      OR da.status_code < 200
			      OR da.status_code >= 300
			  )
		),
		ranked AS (
			SELECT webhook_id,
			       ROW_NUMBER() OVER (PARTITION BY webhook_id ORDER BY sent_at DESC) AS rn
			FROM failure_attempts
		)
		SELECT webhook_id::text, count(*)::int
		FROM ranked
		WHERE rn <= $2
		GROUP BY webhook_id
	`, webhookIDs, limit)
	if err != nil {
		return nil, fmt.Errorf("batch count consecutive webhook failures: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[string]int, len(webhookIDs))
	for rows.Next() {
		var wid string
		var n int
		if err := rows.Scan(&wid, &n); err != nil {
			return nil, fmt.Errorf("scan failure count: %w", err)
		}
		counts[wid] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate failure counts: %w", err)
	}
	return counts, nil
}

func batchOpenHalfOpenBreakers(ctx context.Context, tx *sql.Tx, webhookIDs []string, maxCooldownMs int64) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'open',
		    breaker_failure_count = breaker_failure_count + 1,
		    breaker_opened_at = now(),
		    breaker_next_probe_at = now() + (LEAST(breaker_cooldown_ms * 2, $2) * interval '1 millisecond'),
		    breaker_cooldown_ms = LEAST(breaker_cooldown_ms * 2, $2),
		    breaker_probe_delivery_id = NULL
		WHERE id = ANY($1::uuid[])
		  AND breaker_state = 'half_open'
		RETURNING id::text
	`, webhookIDs, maxCooldownMs)
	if err != nil {
		return nil, fmt.Errorf("batch open half-open breakers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var opened []string
	for rows.Next() {
		var wid string
		if err := rows.Scan(&wid); err != nil {
			return nil, fmt.Errorf("scan opened webhook: %w", err)
		}
		opened = append(opened, wid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate opened webhooks: %w", err)
	}
	return opened, nil
}

func batchOpenClosedBreakers(ctx context.Context, tx *sql.Tx, failureCounts map[string]int, initialCooldownMs int64) ([]string, error) {
	webhookIDs := make([]string, 0, len(failureCounts))
	counts := make([]int, 0, len(failureCounts))
	for wid, n := range failureCounts {
		webhookIDs = append(webhookIDs, wid)
		counts = append(counts, n)
	}

	rows, err := tx.QueryContext(ctx, `
		WITH input AS (
			SELECT * FROM unnest($1::text[], $2::int[]) AS t(webhook_id, failure_count)
		)
		UPDATE webhooks w
		SET breaker_state = 'open',
		    breaker_failure_count = input.failure_count,
		    breaker_opened_at = now(),
		    breaker_next_probe_at = now() + ($3 * interval '1 millisecond'),
		    breaker_cooldown_ms = $3,
		    breaker_probe_delivery_id = NULL
		FROM input
		WHERE w.id = input.webhook_id::uuid
		  AND w.breaker_state = 'closed'
		RETURNING w.id::text
	`, webhookIDs, counts, initialCooldownMs)
	if err != nil {
		return nil, fmt.Errorf("batch open closed breakers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var opened []string
	for rows.Next() {
		var wid string
		if err := rows.Scan(&wid); err != nil {
			return nil, fmt.Errorf("scan opened webhook: %w", err)
		}
		opened = append(opened, wid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate opened webhooks: %w", err)
	}
	return opened, nil
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

func (s *Store) batchRecordBreakerSuccess(ctx context.Context, tx *sql.Tx, deliveryIDs []string) ([]string, error) {
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, deliveryIDs)
	if err != nil {
		return nil, err
	}

	var uniqueWIDs []string
	seen := make(map[string]bool, len(webhookIDs))
	for _, wid := range webhookIDs {
		if !seen[wid] {
			seen[wid] = true
			uniqueWIDs = append(uniqueWIDs, wid)
		}
	}

	rows, err := tx.QueryContext(ctx, `
		UPDATE webhooks
		SET breaker_state = 'closed',
		    breaker_failure_count = 0,
		    breaker_opened_at = NULL,
		    breaker_next_probe_at = NULL,
		    breaker_cooldown_ms = $2,
		    breaker_probe_delivery_id = NULL
		WHERE id = ANY($1::uuid[])
		  AND breaker_state <> 'closed'
		  AND NOT (
		      breaker_state = 'half_open'
		      AND breaker_probe_delivery_id IS NOT NULL
		      AND breaker_probe_delivery_id <> ALL($3::uuid[])
		  )
		RETURNING id::text
	`, uniqueWIDs, s.breakerConfig.InitialCooldown.Milliseconds(), deliveryIDs)
	if err != nil {
		return nil, fmt.Errorf("batch record breaker success: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var closed []string
	for rows.Next() {
		var wid string
		if err := rows.Scan(&wid); err != nil {
			return nil, fmt.Errorf("scan closed webhook: %w", err)
		}
		closed = append(closed, wid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate closed webhooks: %w", err)
	}
	return closed, nil
}

type trippedBreaker struct {
	webhookID string
	reason    string
}

func (s *Store) batchRecordBreakerFailure(ctx context.Context, tx *sql.Tx, deliveryIDs []string, items []BatchMarkItem, reasonFn func(BatchMarkItem) string) ([]trippedBreaker, error) {
	webhookIDs, err := batchDeliveryWebhookIDs(ctx, tx, deliveryIDs)
	if err != nil {
		return nil, err
	}

	type webhookInfo struct {
		webhookID string
		reason    string
	}
	deduped := make(map[string]webhookInfo, len(webhookIDs))
	for _, item := range items {
		wid, ok := webhookIDs[item.DeliveryID]
		if !ok {
			return nil, fmt.Errorf("delivery %s not found", item.DeliveryID)
		}
		if _, exists := deduped[wid]; !exists {
			deduped[wid] = webhookInfo{webhookID: wid, reason: reasonFn(item)}
		}
	}

	var tripped []trippedBreaker

	uniqueIDs := make([]string, 0, len(deduped))
	for wid := range deduped {
		uniqueIDs = append(uniqueIDs, wid)
	}

	states, err := batchLoadBreakerStates(ctx, tx, uniqueIDs)
	if err != nil {
		return nil, err
	}

	var halfOpenIDs []string
	var closedIDs []string
	for wid, state := range states {
		switch state.state {
		case BreakerStateHalfOpen:
			halfOpenIDs = append(halfOpenIDs, wid)
		case BreakerStateClosed:
			closedIDs = append(closedIDs, wid)
		}
	}

	if len(halfOpenIDs) > 0 {
		opened, err := batchOpenHalfOpenBreakers(ctx, tx, halfOpenIDs, s.breakerConfig.MaxCooldown.Milliseconds())
		if err != nil {
			return nil, err
		}
		for _, wid := range opened {
			tripped = append(tripped, trippedBreaker{webhookID: wid, reason: deduped[wid].reason})
		}
	}

	if len(closedIDs) > 0 {
		counts, err := batchConsecutiveWebhookFailures(ctx, tx, closedIDs, s.breakerConfig.FailureThreshold+1)
		if err != nil {
			return nil, err
		}
		exceeded := make(map[string]int, len(counts))
		for wid, n := range counts {
			if n >= s.breakerConfig.FailureThreshold {
				exceeded[wid] = n
			}
		}
		if len(exceeded) > 0 {
			opened, err := batchOpenClosedBreakers(ctx, tx, exceeded, s.breakerConfig.InitialCooldown.Milliseconds())
			if err != nil {
				return nil, err
			}
			for _, wid := range opened {
				tripped = append(tripped, trippedBreaker{webhookID: wid, reason: deduped[wid].reason})
			}
		}
	}

	return tripped, nil
}
