package outbox_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

// TestDiscoverClaimableCustomersDoesNotScanFullBacklog guards finding B-5:
// DiscoverClaimableCustomers must enumerate distinct claimable customers with a
// loose index-scan whose cost tracks the number of customers returned, not the
// number of ready *deliveries* in the backlog. Before the fix a UNION/DISTINCT
// forced a full ready-set scan before the outer LIMIT, so latency grew with the
// backlog row count (~100x at 100k rows); every replica paid this on its
// discovery cadence (default 1s) under steady load.
//
// The test holds the customer set fixed (same distinct result) and grows the
// per-customer backlog ~1000x. With the loose index-scan the heavy-backlog
// latency must stay close to the light-backlog latency; a regression to the
// full scan would blow the ratio (and the absolute bound) wide open. It also
// logs the EXPLAIN (ANALYZE, BUFFERS) plan for both sizes as the evidence
// record agents can diff.
func TestDiscoverClaimableCustomersDoesNotScanFullBacklog(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db)

	const customers = 50
	const lightPerCustomer = 1
	const heavyPerCustomer = 1000

	// Light backlog: each customer has a single ready delivery.
	for i := 0; i < customers; i++ {
		cid := fmt.Sprintf("cus_disc_%03d", i)
		bulkSeedReadyBacklog(t, ctx, db, cid, lightPerCustomer)
	}

	lightFound, lightDur := timeDiscover(t, ctx, store, customers)
	lightPlan := explainDiscoveryScan(t, ctx, db)

	// Heavy backlog: same customers, far more ready deliveries each. The
	// distinct-customer result is unchanged; only the scanned row count grows.
	for i := 0; i < customers; i++ {
		cid := fmt.Sprintf("cus_disc_%03d", i)
		appendReadyBacklog(t, ctx, db, cid, heavyPerCustomer)
	}

	heavyFound, heavyDur := timeDiscover(t, ctx, store, customers)
	heavyPlan := explainDiscoveryScan(t, ctx, db)

	if lightFound != customers || heavyFound != customers {
		t.Fatalf("distinct customers discovered: light=%d heavy=%d, want %d for both",
			lightFound, heavyFound, customers)
	}

	totalLight := customers * lightPerCustomer
	totalHeavy := customers * (lightPerCustomer + heavyPerCustomer)
	t.Logf("B-5 discovery cost: %d customers, both backlog sizes return the same %d distinct customers",
		customers, customers)
	t.Logf("  light backlog: %d ready deliveries -> Discover took %s", totalLight, lightDur)
	t.Logf("  heavy backlog: %d ready deliveries -> Discover took %s", totalHeavy, heavyDur)
	ratio := float64(heavyDur) / float64(maxDuration(lightDur, time.Microsecond))
	t.Logf("  ratio heavy/light latency: %.1fx (backlog grew %.0fx)",
		ratio, float64(totalHeavy)/float64(totalLight))
	t.Logf("B-5 EXPLAIN (light backlog):\n%s", lightPlan)
	t.Logf("B-5 EXPLAIN (heavy backlog):\n%s", heavyPlan)

	// Regression guard: the loose index-scan must keep the heavy-backlog scan
	// cheap. A full-backlog scan (the pre-fix behaviour) ran ~100x slower and
	// hundreds of ms at this size; bound generously to stay non-flaky on noisy
	// CI containers while still catching a return to the full scan.
	const maxHeavyLatency = 60 * time.Millisecond
	if heavyDur > maxHeavyLatency {
		t.Errorf("heavy-backlog discovery took %s (> %s): loose index-scan likely regressed to a full backlog scan",
			heavyDur, maxHeavyLatency)
	}
}

func timeDiscover(t *testing.T, ctx context.Context, store *outbox.Store, limit int) (int, time.Duration) {
	t.Helper()
	// Warm once, then take the best of a few runs to reduce container noise.
	if _, err := store.DiscoverClaimableCustomers(ctx, "", limit); err != nil {
		t.Fatalf("discover warmup: %v", err)
	}
	best := time.Hour
	var found int
	for i := 0; i < 5; i++ {
		start := time.Now()
		customers, err := store.DiscoverClaimableCustomers(ctx, "", limit)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if elapsed < best {
			best = elapsed
		}
		found = len(customers)
	}
	return found, best
}

// appendReadyBacklog adds n more ready deliveries for an existing customer's
// webhook without creating a new webhook (so the distinct-customer set is
// unchanged).
func appendReadyBacklog(t *testing.T, ctx context.Context, db *sql.DB, customerID string, n int) {
	t.Helper()
	var webhookID string
	if err := db.QueryRowContext(ctx,
		`SELECT id::text FROM webhooks WHERE customer_id = $1 LIMIT 1`,
		customerID).Scan(&webhookID); err != nil {
		t.Fatalf("load webhook id for %s: %v", customerID, err)
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
		t.Fatalf("append ready backlog for %s: %v", customerID, err)
	}
}

// explainDiscoveryScan EXPLAINs the live DiscoverClaimableCustomers query (the
// recursive loose index-scan over the ready branch merged with the stale
// branch) so the plan's actual-rows and buffer counts show the loose scan
// touching ~one row per returned customer instead of the whole ready backlog.
func explainDiscoveryScan(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	if _, err := db.ExecContext(ctx, `ANALYZE deliveries, webhooks, delivery_claims`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	rows, err := db.QueryContext(ctx, `
		EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
		WITH RECURSIVE ready_customers AS (
			SELECT seed.customer_id, 1 AS depth
			FROM (
				SELECT d.customer_id
				FROM deliveries d
				JOIN webhooks w ON w.id = d.webhook_id
				WHERE d.customer_id > ''
				  AND d.status IN ('pending', 'failed_retry')
				  AND d.next_attempt_at <= now()
				  AND (
				      w.breaker_state = 'closed'
				      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
				      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
				  )
				ORDER BY d.customer_id
				LIMIT 1
			) seed
			UNION ALL
			SELECT
				(
					SELECT d.customer_id
					FROM deliveries d
					JOIN webhooks w ON w.id = d.webhook_id
					WHERE d.customer_id > rc.customer_id
					  AND d.status IN ('pending', 'failed_retry')
					  AND d.next_attempt_at <= now()
					  AND (
					      w.breaker_state = 'closed'
					      OR (w.breaker_state = 'half_open' AND w.breaker_probe_delivery_id = d.id)
					      OR (w.breaker_state = 'open' AND w.breaker_next_probe_at <= now())
					  )
					ORDER BY d.customer_id
					LIMIT 1
				),
				rc.depth + 1
			FROM ready_customers rc
			WHERE rc.customer_id IS NOT NULL
			  AND rc.depth < 64
		)
		SELECT customer_id
		FROM ready_customers
		WHERE customer_id IS NOT NULL
		ORDER BY customer_id
		LIMIT 64
	`)
	if err != nil {
		t.Fatalf("explain discovery: %v", err)
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

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
