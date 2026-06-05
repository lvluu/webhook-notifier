package outbox_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

// TestBreakerFailureMarkCostIndependentOfAttemptHistory reproduces issue #126:
// the failure-mark path used to recompute "consecutive failures since the last
// 2xx" by scanning the full per-webhook delivery_attempts history on every mark
// batch. During a sustained partner outage that scan walked the entire history
// and tripped statement_timeout (SQLSTATE 57014); the worker treated the
// timeout as fatal and the whole notifier fleet exited.
//
// The fix maintains the persisted breaker_failure_count incrementally (success
// resets to 0, each failure adds one), so marking a failure no longer reads
// delivery_attempts. This test seeds a large failing history for one webhook
// and asserts that marking a fresh failure stays fast and bumps the counter by
// exactly one — independent of how deep the history is. Before the fix this
// mark scaled with the seeded history and would blow the latency bound (and, in
// production, the statement timeout).
func TestBreakerFailureMarkCostIndependentOfAttemptHistory(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_breaker_history"
	deliveryID := submitOneDelivery(t, ctx, db, customerID)

	// Seed a deep failing history for this webhook on terminal ('dead')
	// deliveries so they neither block the claim of the fresh delivery nor
	// participate in the counter, but would dominate any history scan.
	const historyDepth = 200_000
	seedFailingAttemptHistory(t, ctx, db, deliveryID, historyDepth)

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  time.Hour,
		MaxCooldown:      time.Hour,
	}))

	// Claim the one fresh, ready delivery (the seeded history rows are 'dead').
	claimed := claimDelivery(t, ctx, store, customerID, deliveryID)

	start := time.Now()
	err := store.MarkRetryBatch(ctx, []outbox.BatchMarkItem{
		{
			DeliveryID:    claimed.ID,
			StatusCode:    500,
			ErrorClass:    retry.ReasonRetryable5xx,
			Delay:         time.Millisecond,
			AttemptNumber: claimed.AttemptNumber,
		},
	})
	markDuration := time.Since(start)
	if err != nil {
		t.Fatalf("MarkRetryBatch: %v", err)
	}

	t.Logf("#126 failure mark with %d historical attempts took %s", historyDepth, markDuration)

	// Counter-based: one failure advances breaker_failure_count from 0 to 1 and
	// stays below the threshold of 2, regardless of the 200k historical
	// failures (which the old recompute would have counted).
	assertWebhookBreaker(t, ctx, db, claimed.ID, "closed", 1)

	// Regression guard: the mark must not scan the attempt history. The counter
	// update is sub-millisecond; bound generously to stay non-flaky on noisy CI
	// while still catching a return to the O(history) recompute, which at this
	// depth ran into hundreds of milliseconds and tripped statement_timeout.
	const maxMarkLatency = 500 * time.Millisecond
	if markDuration > maxMarkLatency {
		t.Errorf("failure mark took %s (> %s): breaker failure counting likely regressed to scanning delivery_attempts history",
			markDuration, maxMarkLatency)
	}
}

// seedFailingAttemptHistory inserts n terminal ('dead') deliveries for the
// webhook behind existingDeliveryID, each with one failing delivery_attempts
// row, building a deep per-webhook attempt history without affecting the
// breaker counter or the claimability of the original delivery.
func seedFailingAttemptHistory(t *testing.T, ctx context.Context, db *sql.DB, existingDeliveryID string, n int) {
	t.Helper()

	var eventID, webhookID, customerID string
	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text, webhook_id::text, customer_id
		FROM deliveries
		WHERE id = $1::uuid
	`, existingDeliveryID).Scan(&eventID, &webhookID, &customerID); err != nil {
		t.Fatalf("load delivery for history seed: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		WITH new_deliveries AS (
			INSERT INTO deliveries (
				id, event_id, webhook_id, customer_id, status,
				next_attempt_at, attempt_count, created_at, updated_at
			)
			SELECT gen_random_uuid(), $1::uuid, $2::uuid, $3, 'dead',
			       now(), 1, now(), now()
			FROM generate_series(1, $4)
			RETURNING id
		)
		INSERT INTO delivery_attempts (
			delivery_id, attempt_number, status_code, error_class, duration_ms, sent_at
		)
		SELECT id, 1, 500, 'retryable_5xx', 1, now() - (random() * interval '1 hour')
		FROM new_deliveries
	`, eventID, webhookID, customerID, n); err != nil {
		t.Fatalf("seed failing attempt history: %v", err)
	}
}
