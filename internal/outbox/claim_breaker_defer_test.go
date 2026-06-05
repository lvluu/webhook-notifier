package outbox_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

// These tests exercise deferBlockedDeliveries (finding B-3) across every
// breaker-open code path and lifecycle transition. The behavior under test:
// when a webhook breaker opens, that webhook's ready deliveries are pushed out
// to the probe time (GREATEST with their current schedule) so they leave the
// claim ready-scan, and they return to the ready set when the breaker probes or
// closes.

// nextAttemptAt reads a delivery's scheduled next_attempt_at.
func nextAttemptAt(t *testing.T, ctx context.Context, db *sql.DB, deliveryID string) time.Time {
	t.Helper()
	var at time.Time
	if err := db.QueryRowContext(ctx, `SELECT next_attempt_at FROM deliveries WHERE id = $1`, deliveryID).Scan(&at); err != nil {
		t.Fatalf("next_attempt_at: %v", err)
	}
	return at
}

// submitEventTo submits one event of eventType for customerID and returns the
// single resulting delivery id.
func submitEventTo(t *testing.T, ctx context.Context, db *sql.DB, customerID, eventType string) string {
	t.Helper()
	submitted, err := newIngestStore(db).Submit(ctx, ingest.Event{
		CustomerID: customerID,
		Type:       eventType,
		Payload:    []byte(`{"k":"v"}`),
	})
	if err != nil {
		t.Fatalf("submit %s: %v", eventType, err)
	}
	if len(submitted.DeliveryIDs) != 1 {
		t.Fatalf("submit %s: got %d deliveries, want 1", eventType, len(submitted.DeliveryIDs))
	}
	return submitted.DeliveryIDs[0]
}

func breakerStore(db *sql.DB, threshold int, cooldown time.Duration) *outbox.Store {
	return outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: threshold,
		InitialCooldown:  cooldown,
		MaxCooldown:      cooldown,
	}))
}

// TestDeferBlocked_SingleMarkRetryPath covers the single-delivery openBreaker
// path (MarkRetry -> recordBreakerFailure -> openBreaker), distinct from the
// batch path the main test exercises.
func TestDeferBlocked_SingleMarkRetryPath(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_defer_single"
	const cooldown = time.Hour

	firstID := submitOneDelivery(t, ctx, db, customerID)
	backlogID, err := appendDeliveryForExistingWebhook(t, ctx, db, firstID)
	if err != nil {
		t.Fatalf("append backlog: %v", err)
	}

	store := breakerStore(db, 2, cooldown)

	// Two single-mark failures on the same webhook trip the breaker via the
	// single path.
	c1 := claimDelivery(t, ctx, store, customerID, firstID)
	if err := store.MarkRetry(ctx, c1.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 1: %v", err)
	}
	c2 := waitForClaim(t, ctx, store, customerID, time.Second)
	if err := store.MarkRetry(ctx, c2.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 2: %v", err)
	}

	assertWebhookBreaker(t, ctx, db, firstID, "open", 2)

	// The untouched backlog delivery must be deferred ~1h out, not ready.
	if got := nextAttemptAt(t, ctx, db, backlogID); !got.After(time.Now().Add(50 * time.Minute)) {
		t.Errorf("backlog next_attempt_at: got %s, want deferred ~1h out", got)
	}
	if ready := readyCount(t, ctx, db, customerID); ready != 0 {
		t.Errorf("ready after single-path open: got %d, want 0", ready)
	}
}

// TestDeferBlocked_OnlyAffectsOpenedWebhook proves deferral is scoped to the
// opened webhook: a sibling healthy webhook's deliveries stay ready.
func TestDeferBlocked_OnlyAffectsOpenedWebhook(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_defer_iso"

	insertWebhook(t, ctx, db, customerID, "https://blocked.example/hook", true, "blocked.event")
	insertWebhook(t, ctx, db, customerID, "https://healthy.example/hook", true, "healthy.event")

	// Two deliveries on the soon-to-open webhook, one on the healthy webhook.
	blockedA := submitEventTo(t, ctx, db, customerID, "blocked.event")
	blockedB := submitEventTo(t, ctx, db, customerID, "blocked.event")
	healthy := submitEventTo(t, ctx, db, customerID, "healthy.event")

	store := breakerStore(db, 1, time.Hour)

	// One failure on the blocked webhook (threshold 1) opens it.
	cA := claimDelivery(t, ctx, store, customerID, blockedA)
	if err := store.MarkRetry(ctx, cA.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry blockedA: %v", err)
	}
	assertWebhookBreaker(t, ctx, db, blockedA, "open", 1)

	// blockedB deferred; healthy still ready.
	if got := nextAttemptAt(t, ctx, db, blockedB); !got.After(time.Now().Add(50 * time.Minute)) {
		t.Errorf("blockedB next_attempt_at: got %s, want deferred", got)
	}
	if got := nextAttemptAt(t, ctx, db, healthy); got.After(time.Now().Add(time.Minute)) {
		t.Errorf("healthy next_attempt_at: got %s, want still ready (not deferred)", got)
	}

	claimed, ok, err := store.ClaimNext(ctx, customerID)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if !ok || claimed.ID != healthy {
		t.Fatalf("claim after open: got ok=%v id=%q, want the healthy delivery %q", ok, claimed.ID, healthy)
	}
}

// TestDeferBlocked_GreatestPreservesLaterSchedule proves a delivery already
// scheduled past the probe time keeps its later next_attempt_at (GREATEST), not
// pulled earlier.
func TestDeferBlocked_GreatestPreservesLaterSchedule(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_defer_greatest"

	firstID := submitOneDelivery(t, ctx, db, customerID)
	farID, err := appendDeliveryForExistingWebhook(t, ctx, db, firstID)
	if err != nil {
		t.Fatalf("append far delivery: %v", err)
	}
	// Schedule farID well past any probe time.
	if _, err := db.ExecContext(ctx, `UPDATE deliveries SET next_attempt_at = now() + interval '5 hours' WHERE id = $1`, farID); err != nil {
		t.Fatalf("set far schedule: %v", err)
	}

	store := breakerStore(db, 2, time.Hour) // probe ~1h out, far less than 5h

	c1 := claimDelivery(t, ctx, store, customerID, firstID)
	if err := store.MarkRetry(ctx, c1.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 1: %v", err)
	}
	c2 := waitForClaim(t, ctx, store, customerID, time.Second)
	if err := store.MarkRetry(ctx, c2.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 2: %v", err)
	}
	assertWebhookBreaker(t, ctx, db, firstID, "open", 2)

	// farID must keep its ~5h schedule (GREATEST), not be pulled to the ~1h probe.
	if got := nextAttemptAt(t, ctx, db, farID); !got.After(time.Now().Add(4 * time.Hour)) {
		t.Errorf("far delivery next_attempt_at: got %s, want preserved ~5h out", got)
	}
}

// TestDeferBlocked_HalfOpenReopenRedefers covers batchOpenHalfOpenBreakers: a
// failed probe reopens the breaker with a longer cooldown and must re-defer the
// backlog to the new, later probe time.
func TestDeferBlocked_HalfOpenReopenRedefers(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_defer_reopen"
	const cooldown = 400 * time.Millisecond

	firstID := submitOneDelivery(t, ctx, db, customerID)
	backlogID, err := appendDeliveryForExistingWebhook(t, ctx, db, firstID)
	if err != nil {
		t.Fatalf("append backlog: %v", err)
	}

	// Small initial cooldown so the probe is testable; large max so the doubled
	// cooldown on reopen lands further in the future.
	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  cooldown,
		MaxCooldown:      24 * time.Hour,
	}))

	c1 := claimDelivery(t, ctx, store, customerID, firstID)
	if err := store.MarkRetry(ctx, c1.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 1: %v", err)
	}
	c2 := waitForClaim(t, ctx, store, customerID, time.Second)
	if err := store.MarkRetry(ctx, c2.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 2: %v", err)
	}
	assertWebhookBreaker(t, ctx, db, firstID, "open", 2)

	// Wait out the initial cooldown, claim the probe (half_open), then fail it.
	time.Sleep(cooldown + 150*time.Millisecond)
	probe, ok, err := store.ClaimNext(ctx, customerID)
	if err != nil || !ok {
		t.Fatalf("probe claim: ok=%v err=%v", ok, err)
	}
	if err := store.MarkRetry(ctx, probe.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry probe: %v", err)
	}

	// Reopened with a doubled cooldown: probe time should be well in the future
	// and the backlog re-deferred past now.
	assertWebhookNextProbeAfter(t, ctx, db, firstID, time.Now().Add(200*time.Millisecond))
	if got := nextAttemptAt(t, ctx, db, backlogID); !got.After(time.Now()) {
		t.Errorf("backlog after reopen: got %s, want re-deferred to future", got)
	}
	if ready := readyCount(t, ctx, db, customerID); ready != 0 {
		t.Errorf("ready after reopen: got %d, want 0", ready)
	}
}

// TestDeferBlocked_ClosedBreakerRestoresReadiness proves that once the breaker
// closes (successful probe), the previously deferred deliveries — scheduled to
// the now-past probe time — return to the ready set.
func TestDeferBlocked_ClosedBreakerRestoresReadiness(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_defer_close"
	const cooldown = 400 * time.Millisecond

	firstID := submitOneDelivery(t, ctx, db, customerID)
	backlogID, err := appendDeliveryForExistingWebhook(t, ctx, db, firstID)
	if err != nil {
		t.Fatalf("append backlog: %v", err)
	}

	store := breakerStore(db, 2, cooldown)

	c1 := claimDelivery(t, ctx, store, customerID, firstID)
	if err := store.MarkRetry(ctx, c1.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 1: %v", err)
	}
	c2 := waitForClaim(t, ctx, store, customerID, time.Second)
	if err := store.MarkRetry(ctx, c2.ID, 500, retry.ReasonRetryable5xx, time.Millisecond, 0); err != nil {
		t.Fatalf("MarkRetry 2: %v", err)
	}
	assertWebhookBreaker(t, ctx, db, firstID, "open", 2)

	// Probe, then succeed it to close the breaker.
	time.Sleep(cooldown + 150*time.Millisecond)
	probe, ok, err := store.ClaimNext(ctx, customerID)
	if err != nil || !ok {
		t.Fatalf("probe claim: ok=%v err=%v", ok, err)
	}
	if err := store.MarkSucceeded(ctx, probe.ID, 200, 0); err != nil {
		t.Fatalf("MarkSucceeded probe: %v", err)
	}
	assertWebhookBreaker(t, ctx, db, backlogID, "closed", 0)

	// The deferred backlog was scheduled to the probe time, now in the past, so
	// it is ready again.
	if ready := readyCount(t, ctx, db, customerID); ready == 0 {
		t.Errorf("ready after breaker close: got 0, want the restored backlog")
	}
}
