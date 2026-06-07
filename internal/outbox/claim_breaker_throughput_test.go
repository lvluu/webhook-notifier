package outbox_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

// TestClaimThroughput_OpenBreakerBallast measures claim-path cost when a
// Customer has a large ready backlog sitting behind an OPEN breaker — the
// finding B-3 hot spot. It seeds N such deliveries, trips the breaker through
// the real store API (so deferBlockedDeliveries runs), then times M ClaimNextN
// calls. With the deferral the backlog has left idx_deliveries_ready, so each
// claim is an empty index probe; without it (revert breaker.go) every claim
// re-scans and discards the whole N-row backlog through the webhook join.
//
// Run with: go test ./internal/outbox -run TestClaimThroughput_OpenBreakerBallast -v
// Revert the deferral (git stash the breaker.go change) to capture the baseline.
func TestClaimThroughput_OpenBreakerBallast(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_b3_tput"
	const backlog = 20000
	const claimCalls = 500

	webhookID := bulkSeedReadyBacklog(t, ctx, db, customerID, backlog)

	store := outbox.New(db, outbox.WithBreakerFiltering(true))

	firstDeliveryID := firstDeliveryForWebhook(t, ctx, db, webhookID)
	persistBreakerOpenForDelivery(t, ctx, db, firstDeliveryID, 2, time.Hour)
	assertWebhookBreaker(t, ctx, db, firstDeliveryID, "open", 2)
	_ = webhookID

	if _, err := db.ExecContext(ctx, `ANALYZE deliveries, webhooks, delivery_claims`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	ready := readyCount(t, ctx, db, customerID)
	t.Logf("backlog=%d, ready-after-open=%d (0 ⇒ deferred by fix, ~%d ⇒ ballast/no-fix)", backlog, ready, backlog)
	if ready != 0 {
		t.Fatalf("ready after breaker open: got %d, want 0 — the %d-row backlog was not deferred off the claim ready-scan", ready, backlog)
	}

	// Time M claim attempts against the open-breaker state.
	start := time.Now()
	claimed := 0
	for i := 0; i < claimCalls; i++ {
		ds, err := store.ClaimNextN(ctx, customerID, 32)
		if err != nil {
			t.Fatalf("ClaimNextN[%d]: %v", i, err)
		}
		claimed += len(ds)
	}
	elapsed := time.Since(start)

	t.Logf("B-3 claim throughput: %d ClaimNextN calls over %d-row open-breaker backlog took %s (avg %s/call), claimed=%d",
		claimCalls, backlog, elapsed.Round(time.Microsecond), (elapsed / claimCalls).Round(time.Microsecond), claimed)
}

// bulkSeedReadyBacklog inserts n ready (pending) deliveries for one webhook of
// customerID in a couple of set-based statements and returns the webhook id.
func bulkSeedReadyBacklog(t *testing.T, ctx context.Context, db *sql.DB, customerID string, n int) string {
	t.Helper()

	insertWebhook(t, ctx, db, customerID, "https://b3-tput.example/hook", true, "subscriber.created")
	var webhookID string
	if err := db.QueryRowContext(ctx,
		`SELECT id::text FROM webhooks WHERE customer_id = $1 AND url = 'https://b3-tput.example/hook'`,
		customerID).Scan(&webhookID); err != nil {
		t.Fatalf("load webhook id: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		WITH e AS (
			INSERT INTO events (customer_id, type, payload)
			SELECT $1, 'subscriber.created', '{"k":"v"}'::jsonb
			FROM generate_series(1, $2)
			RETURNING id
		)
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status)
		SELECT e.id, $3::uuid, $1, 'pending' FROM e
	`, customerID, n, webhookID); err != nil {
		t.Fatalf("bulk seed deliveries: %v", err)
	}
	return webhookID
}

func firstDeliveryForWebhook(t *testing.T, ctx context.Context, db *sql.DB, webhookID string) string {
	t.Helper()
	var deliveryID string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM deliveries
		WHERE webhook_id = $1::uuid
		ORDER BY created_at, id
		LIMIT 1
	`, webhookID).Scan(&deliveryID); err != nil {
		t.Fatalf("first delivery for webhook: %v", err)
	}
	return deliveryID
}
