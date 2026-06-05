package outbox_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
)

// TestBreakerOpenDefersBlockedDeliveries is the behavior + evidence test for
// finding B-3. When a webhook breaker opens, its ready deliveries must be
// deferred to the probe schedule so they leave idx_deliveries_ready instead of
// piling up as unclaimable ballast that the claim ready-scan reads and discards
// on every cycle. It proves: (1) blocked deliveries are no longer ready after
// open, (2) the claim ready-scan no longer over-reads them, and (3) the probe
// delivery becomes claimable again at probe time.
func TestBreakerOpenDefersBlockedDeliveries(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)

	const customerID = "cus_b3_defer"
	const cooldown = 400 * time.Millisecond

	// One webhook with a backlog of pending deliveries.
	firstID := submitOneDelivery(t, ctx, db, customerID)
	const backlog = 200
	for i := 0; i < backlog; i++ {
		if _, err := appendDeliveryForExistingWebhook(t, ctx, db, firstID); err != nil {
			t.Fatalf("append delivery %d: %v", i, err)
		}
	}

	store := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 2,
		InitialCooldown:  cooldown,
		MaxCooldown:      cooldown,
	}))

	// Claim two deliveries and fail them to trip the breaker (threshold 2).
	c1 := claimDelivery(t, ctx, store, customerID, firstID)
	c2 := waitForClaim(t, ctx, store, customerID, time.Second)
	if err := store.MarkRetryBatch(ctx, []outbox.BatchMarkItem{
		{DeliveryID: c1.ID, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
		{DeliveryID: c2.ID, StatusCode: 500, ErrorClass: retry.ReasonRetryable5xx, Delay: time.Millisecond},
	}); err != nil {
		t.Fatalf("MarkRetryBatch: %v", err)
	}

	assertWebhookBreaker(t, ctx, db, firstID, "open", 2)

	// (1) The breaker open must have deferred the ready backlog out to the probe
	// time: no deliveries for this customer should be ready right now.
	if ready := readyCount(t, ctx, db, customerID); ready != 0 {
		t.Fatalf("ready deliveries immediately after breaker open: got %d, want 0 (all deferred to probe)", ready)
	}

	// (2) Evidence: the claim ready-scan no longer reads the backlog. With every
	// row deferred, the index cond (next_attempt_at <= now()) excludes them, so
	// the scan returns nothing without a 200-row join-filter discard.
	plan := explainReadyScan(t, ctx, db, customerID)
	t.Logf("=== claim ready-scan after breaker-open deferral ===\n%s", plan)
	if strings.Contains(plan, "Rows Removed by Join Filter: 2") {
		t.Errorf("ready-scan still over-reads the deferred backlog:\n%s", plan)
	}

	// (3) The probe must still work: after the cooldown elapses the deferred
	// deliveries become ready again and one is claimable as the half-open probe.
	time.Sleep(cooldown + 150*time.Millisecond)
	probe, ok, err := store.ClaimNext(ctx, customerID)
	if err != nil {
		t.Fatalf("probe ClaimNext: %v", err)
	}
	if !ok {
		t.Fatal("probe ClaimNext: got no delivery, want a half-open probe after cooldown")
	}
	assertWebhookBreaker(t, ctx, db, probe.ID, "half_open", 2)
}

func readyCount(t *testing.T, ctx context.Context, db *sql.DB, customerID string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM deliveries d
		LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
		WHERE d.customer_id = $1
		  AND d.status IN ('pending', 'failed_retry')
		  AND d.next_attempt_at <= now()
		  AND dc.delivery_id IS NULL
	`, customerID).Scan(&n); err != nil {
		t.Fatalf("ready count: %v", err)
	}
	return n
}

func explainReadyScan(t *testing.T, ctx context.Context, db *sql.DB, customerID string) string {
	t.Helper()
	if _, err := db.ExecContext(ctx, `ANALYZE deliveries, webhooks, delivery_claims`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	rows, err := db.QueryContext(ctx, `
		EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
		SELECT d.id
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
		WHERE d.customer_id = $1
		  AND d.status IN ('pending', 'failed_retry')
		  AND d.next_attempt_at <= now()
		  AND dc.delivery_id IS NULL
		  AND (
		      w.breaker_state = 'closed'
		      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
		      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
		  )
		ORDER BY d.next_attempt_at, d.created_at, d.id
		LIMIT 64
	`, customerID)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain: %v", err)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("explain rows: %v", err)
	}
	return b.String()
}
