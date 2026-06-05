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
	// FOR NO KEY UPDATE locks the webhook row for this transaction so the
	// read-then-guarded-UPDATE sequence below is race-free: a concurrent
	// failure/transition cannot slip between the SELECT and the UPDATE and drop
	// this increment. NO KEY UPDATE (not FOR UPDATE) stays compatible with the
	// FK FOR KEY SHARE that delivery inserts take on the parent webhook.
	err := tx.QueryRowContext(ctx, `
		SELECT breaker_state, breaker_failure_count, breaker_cooldown_ms
		FROM webhooks
		WHERE id = $1::uuid
		FOR NO KEY UPDATE
	`, webhookID).Scan(&state, &failureCount, &cooldownMillis)
	if err != nil {
		return false, fmt.Errorf("load webhook breaker: %w", err)
	}

	switch state {
	case BreakerStateHalfOpen:
		cooldown := nextBreakerCooldown(time.Duration(cooldownMillis)*time.Millisecond, s.breakerConfig)
		return s.openBreaker(ctx, tx, webhookID, failureCount+1, cooldown, reason)
	case BreakerStateClosed:
		return s.incrementClosedBreaker(ctx, tx, webhookID)
	}
	return false, nil
}

// incrementClosedBreaker adds one failure to a closed webhook's persisted
// breaker_failure_count and opens the breaker in the same statement if the
// running count reaches the threshold. Because success marks reset the count to
// zero, the counter equals the consecutive-failure count without scanning
// delivery_attempts history (issue #126). Returns true only when this call
// transitioned the breaker to open.
func (s *Store) incrementClosedBreaker(ctx context.Context, tx *sql.Tx, webhookID string) (bool, error) {
	var nowOpen bool
	err := tx.QueryRowContext(ctx, `
		UPDATE webhooks
		SET breaker_failure_count = breaker_failure_count + 1,
		    breaker_state = CASE WHEN breaker_failure_count + 1 >= $2
		                         THEN 'open' ELSE breaker_state END,
		    breaker_opened_at = CASE WHEN breaker_failure_count + 1 >= $2
		                             THEN now() ELSE breaker_opened_at END,
		    breaker_next_probe_at = CASE WHEN breaker_failure_count + 1 >= $2
		                                 THEN now() + ($3 * interval '1 millisecond')
		                                 ELSE breaker_next_probe_at END,
		    breaker_cooldown_ms = CASE WHEN breaker_failure_count + 1 >= $2
		                               THEN $3 ELSE breaker_cooldown_ms END
		WHERE id = $1::uuid
		  AND breaker_state = 'closed'
		RETURNING breaker_state = 'open'
	`, webhookID, s.breakerConfig.FailureThreshold, s.breakerConfig.InitialCooldown.Milliseconds()).Scan(&nowOpen)
	if errors.Is(err, sql.ErrNoRows) {
		// Breaker is no longer closed (raced with another mark); nothing to do.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("increment closed breaker: %w", err)
	}
	if !nowOpen {
		return false, nil
	}
	if err := deferBlockedDeliveries(ctx, tx, []string{webhookID}); err != nil {
		return false, err
	}
	return true, nil
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
	if rows == 0 {
		return false, nil
	}
	if err := deferBlockedDeliveries(ctx, tx, []string{webhookID}); err != nil {
		return false, err
	}
	return true, nil
}

// deferBlockedDeliveries pushes the ready deliveries of newly-opened breakers
// out to their breaker_next_probe_at. While a breaker is open its deliveries
// are unclaimable, but they otherwise keep next_attempt_at <= now() and pile up
// as unfilterable ballast in idx_deliveries_ready: the claim ready-scan must
// then read and discard the whole backlog via the breaker-eligibility join on
// every cycle, because the breaker predicate sits past the LIMIT (finding B-3).
//
// Aligning their next_attempt_at with the probe schedule removes them from the
// ready set until the breaker is eligible to probe again; at probe time they
// become ready again and one is claimed as the half-open probe. GREATEST
// preserves any delivery already scheduled past the probe time. Bounded by the
// webhook's ready backlog and runs only at the (rare) open transition, so it
// trades an occasional bulk write for read savings on every claim during an
// outage.
func deferBlockedDeliveries(ctx context.Context, tx *sql.Tx, webhookIDs []string) error {
	if len(webhookIDs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE deliveries d
		SET next_attempt_at = GREATEST(d.next_attempt_at, w.breaker_next_probe_at),
		    updated_at = now()
		FROM webhooks w
		WHERE w.id = ANY($1::uuid[])
		  AND d.webhook_id = w.id
		  AND d.status IN ('pending', 'failed_retry')
		  AND w.breaker_next_probe_at IS NOT NULL
		  AND d.next_attempt_at < w.breaker_next_probe_at
	`, webhookIDs); err != nil {
		return fmt.Errorf("defer blocked deliveries: %w", err)
	}
	return nil
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

// batchIncrementClosedBreakers adds each webhook's failing-attempt count from
// the current mark batch to its persisted breaker_failure_count and, in the
// same set-based statement, opens any breaker whose running count reaches the
// threshold.
//
// This replaces the previous recompute-from-history approach, which counted
// "failures since the last 2xx attempt" by scanning delivery_attempts for every
// webhook on every mark batch. During a sustained partner outage that scan
// walked the full per-webhook attempt history (the last-success lookup matched
// no rows, so the window spanned everything) and tripped statement_timeout —
// SQLSTATE 57014 — which the worker treated as fatal and which took down the
// whole notifier fleet (issue #126).
//
// Correctness: success marks reset breaker_failure_count to 0
// (batchRecordBreakerSuccess / recordBreakerSuccess), so the running counter is
// exactly the number of consecutive failing attempts since the last success —
// the same quantity the history scan computed — without touching
// delivery_attempts. Increments are per failing attempt in this batch, so a
// batch of N failures for one webhook advances the counter by N. The UPDATE is
// row-locked, so concurrent mark batches for the same webhook accumulate
// correctly. Returns the webhook IDs this call transitioned to open.
func batchIncrementClosedBreakers(ctx context.Context, tx *sql.Tx, increments map[string]int, threshold int, initialCooldownMs int64) ([]string, error) {
	if len(increments) == 0 {
		return nil, nil
	}

	webhookIDs := make([]string, 0, len(increments))
	incs := make([]int, 0, len(increments))
	for wid, inc := range increments {
		webhookIDs = append(webhookIDs, wid)
		incs = append(incs, inc)
	}

	rows, err := tx.QueryContext(ctx, `
		WITH input AS (
			SELECT * FROM unnest($1::text[], $2::int[]) AS t(webhook_id, inc)
		)
		UPDATE webhooks w
		SET breaker_failure_count = w.breaker_failure_count + input.inc,
		    breaker_state = CASE WHEN w.breaker_failure_count + input.inc >= $3
		                         THEN 'open' ELSE w.breaker_state END,
		    breaker_opened_at = CASE WHEN w.breaker_failure_count + input.inc >= $3
		                             THEN now() ELSE w.breaker_opened_at END,
		    breaker_next_probe_at = CASE WHEN w.breaker_failure_count + input.inc >= $3
		                                 THEN now() + ($4 * interval '1 millisecond')
		                                 ELSE w.breaker_next_probe_at END,
		    breaker_cooldown_ms = CASE WHEN w.breaker_failure_count + input.inc >= $3
		                               THEN $4 ELSE w.breaker_cooldown_ms END
		FROM input
		WHERE w.id = input.webhook_id::uuid
		  AND w.breaker_state = 'closed'
		RETURNING w.id::text, (w.breaker_state = 'open')
	`, webhookIDs, incs, threshold, initialCooldownMs)
	if err != nil {
		return nil, fmt.Errorf("batch increment closed breakers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var opened []string
	for rows.Next() {
		var wid string
		var nowOpen bool
		if err := rows.Scan(&wid, &nowOpen); err != nil {
			return nil, fmt.Errorf("scan incremented breaker: %w", err)
		}
		if nowOpen {
			opened = append(opened, wid)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incremented breakers: %w", err)
	}
	if err := deferBlockedDeliveries(ctx, tx, opened); err != nil {
		return nil, err
	}
	return opened, nil
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
	if err := deferBlockedDeliveries(ctx, tx, opened); err != nil {
		return nil, err
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
		// Count failing attempts in this batch per closed webhook. Each item is
		// one failing delivery attempt; multiple deliveries for the same webhook
		// advance its running failure counter by that many.
		closedSet := make(map[string]struct{}, len(closedIDs))
		for _, wid := range closedIDs {
			closedSet[wid] = struct{}{}
		}
		increments := make(map[string]int, len(closedIDs))
		for _, item := range items {
			wid, ok := webhookIDs[item.DeliveryID]
			if !ok {
				return nil, fmt.Errorf("delivery %s not found", item.DeliveryID)
			}
			if _, isClosed := closedSet[wid]; isClosed {
				increments[wid]++
			}
		}

		opened, err := batchIncrementClosedBreakers(ctx, tx, increments, s.breakerConfig.FailureThreshold, s.breakerConfig.InitialCooldown.Milliseconds())
		if err != nil {
			return nil, err
		}
		for _, wid := range opened {
			tripped = append(tripped, trippedBreaker{webhookID: wid, reason: deduped[wid].reason})
		}
	}

	return tripped, nil
}
