package outbox_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

// TestDeferredBreakerSkipsInlineMarkAndAppliesViaOutcomes verifies the Tier-2
// split (ADR-0015): with WithDeferredBreaker, a batch mark does NOT touch the
// webhook breaker row, and the breaker only transitions when the equivalent
// outcomes are applied out of band via ApplyBreakerOutcomes (what the
// per-process coalescer calls).
func TestDeferredBreakerSkipsInlineMarkAndAppliesViaOutcomes(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	deliveryID := submitOneDelivery(t, ctx, db, "cus_defer_breaker")
	d2, err := appendDeliveryForExistingWebhook(t, ctx, db, deliveryID)
	if err != nil {
		t.Fatalf("append delivery 2: %v", err)
	}
	d3, err := appendDeliveryForExistingWebhook(t, ctx, db, deliveryID)
	if err != nil {
		t.Fatalf("append delivery 3: %v", err)
	}

	store := outbox.New(db,
		outbox.WithDeferredBreaker(),
		outbox.WithBreakerConfig(outbox.BreakerConfig{
			FailureThreshold: 3,
			InitialCooldown:  time.Hour,
			MaxCooldown:      time.Hour,
		}),
	)

	c1 := claimDelivery(t, ctx, store, "cus_defer_breaker", deliveryID)
	_ = claimDelivery(t, ctx, store, "cus_defer_breaker", d2)
	_ = claimDelivery(t, ctx, store, "cus_defer_breaker", d3)
	webhookID := c1.WebhookID

	if err := store.MarkRetryBatch(ctx, []outbox.BatchMarkItem{
		{DeliveryID: deliveryID, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
		{DeliveryID: d2, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
		{DeliveryID: d3, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
	}); err != nil {
		t.Fatalf("MarkRetryBatch: %v", err)
	}

	// Deferred: the mark must not have touched the breaker row.
	assertWebhookBreaker(t, ctx, db, deliveryID, "closed", 0)

	// Applying the equivalent outcomes opens the breaker (3 failures == threshold).
	if err := store.ApplyBreakerOutcomes(ctx, []outbox.BreakerOutcome{
		{WebhookID: webhookID, DeliveryID: deliveryID, Reason: retry.ReasonRetryable5xx},
		{WebhookID: webhookID, DeliveryID: d2, Reason: retry.ReasonRetryable5xx},
		{WebhookID: webhookID, DeliveryID: d3, Reason: retry.ReasonRetryable5xx},
	}); err != nil {
		t.Fatalf("ApplyBreakerOutcomes: %v", err)
	}

	assertWebhookBreaker(t, ctx, db, deliveryID, "open", 3)
}

// TestBreakerCoalescerOpensBreakerAsync verifies the coalescer goroutine
// consumes enqueued outcomes and applies them, opening the breaker without any
// inline breaker write on the mark path.
func TestBreakerCoalescerOpensBreakerAsync(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	deliveryID := submitOneDelivery(t, ctx, db, "cus_coalescer")
	d2, err := appendDeliveryForExistingWebhook(t, ctx, db, deliveryID)
	if err != nil {
		t.Fatalf("append delivery 2: %v", err)
	}

	store := outbox.New(db,
		outbox.WithDeferredBreaker(),
		outbox.WithBreakerConfig(outbox.BreakerConfig{
			FailureThreshold: 2,
			InitialCooldown:  time.Hour,
			MaxCooldown:      time.Hour,
		}),
	)

	c1 := claimDelivery(t, ctx, store, "cus_coalescer", deliveryID)
	_ = claimDelivery(t, ctx, store, "cus_coalescer", d2)
	webhookID := c1.WebhookID

	coalescer := outbox.NewBreakerCoalescer(store, nil)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = coalescer.Run(runCtx)
		close(done)
	}()

	coalescer.Enqueue([]outbox.BreakerOutcome{
		{WebhookID: webhookID, DeliveryID: deliveryID, Reason: retry.ReasonRetryable5xx},
		{WebhookID: webhookID, DeliveryID: d2, Reason: retry.ReasonRetryable5xx},
	})

	if !waitForBreakerState(t, ctx, db, deliveryID, "open", 3*time.Second) {
		cancel()
		<-done
		t.Fatal("breaker did not open via coalescer within timeout")
	}

	cancel()
	<-done
}

// waitForBreakerState polls the webhook breaker state behind deliveryID until it
// equals want or the timeout elapses.
func waitForBreakerState(t *testing.T, ctx context.Context, db *sql.DB, deliveryID, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var state string
		err := db.QueryRowContext(ctx, `
			SELECT breaker_state FROM webhooks
			WHERE id = (SELECT webhook_id FROM deliveries WHERE id = $1::uuid)
		`, deliveryID).Scan(&state)
		if err == nil && state == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
