package outbox_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

// TestDiscoverClaimableCustomersUnderInFlightBacklog reproduces issue #124:
// DiscoverClaimableCustomers degrades under a large *in-flight* backlog.
//
// Claiming a Delivery inserts a delivery_claims row but leaves
// deliveries.status = 'pending' (the in_flight status is logical only — see
// claim.go: the old UPDATE status='in_flight' was removed for dead-tuple
// bloat). So claimed rows stay in idx_deliveries_ready and the discovery
// loose index-scan can no longer stop at the first row per customer: its
// per-customer LIMIT 1 must walk every claimed index entry (the anti-join
// dc.delivery_id IS NULL filters them out) before it can advance past a
// heavily-claimed customer.
//
// B-5's guard (claim_discovery_explain_test.go) only exercises *unclaimed*
// backlog, where LIMIT 1 finds the first row immediately and stays flat. This
// test holds the same backlog and contrasts unclaimed vs fully-claimed to
// isolate the claimed-row walk as the cost driver.
func TestDiscoverClaimableCustomersUnderInFlightBacklog(t *testing.T) {
	ctx := context.Background()
	db := newPostgresDB(t, ctx)
	store := outbox.New(db, outbox.WithClaimLease(time.Hour))

	const customers = 10
	const perCustomer = 100

	for i := 0; i < customers; i++ {
		cid := fmt.Sprintf("cus_inflight_%03d", i)
		bulkSeedReadyBacklog(t, ctx, db, cid, perCustomer)
	}

	// Unclaimed baseline: every customer has ready, unclaimed work. The loose
	// scan stops at the first index row per customer.
	unclaimedFound, unclaimedDur := timeDiscover(t, ctx, store, customers)
	unclaimedPlan := explainDiscoveryScan(t, ctx, db)

	// Claim the entire backlog: insert a delivery_claims row for every ready
	// delivery. status stays 'pending', so every row remains in
	// idx_deliveries_ready but now fails the anti-join.
	claimEntireBacklog(t, ctx, db)

	claimedFound, claimedDur := timeDiscover(t, ctx, store, customers)
	claimedPlan := explainDiscoveryScan(t, ctx, db)

	total := customers * perCustomer
	t.Logf("#124 in-flight discovery: %d customers, %d deliveries", customers, total)
	t.Logf("  unclaimed backlog -> Discover took %s, found %d customers", unclaimedDur, unclaimedFound)
	t.Logf("  fully-claimed backlog -> Discover took %s, found %d customers", claimedDur, claimedFound)
	t.Logf("  slowdown: %.1fx", float64(claimedDur)/float64(maxDuration(unclaimedDur, time.Microsecond)))
	t.Logf("#124 EXPLAIN (unclaimed):\n%s", unclaimedPlan)
	t.Logf("#124 EXPLAIN (fully claimed):\n%s", claimedPlan)

	// Discovery is claim-agnostic (issue #124): a claimed Delivery stays
	// status='pending', so every seeded customer still has a ready row and is
	// surfaced as a rotation hint. ClaimNextN (FOR UPDATE SKIP LOCKED) is the
	// authority on actual claimability; discovery must not pay to prove a
	// customer is fully claimed.
	if claimedFound != customers {
		t.Fatalf("fully-claimed backlog: discovered %d customers, want %d (discovery is claim-agnostic)",
			claimedFound, customers)
	}

	// Regression guard: walking a fully-claimed backlog must stay as cheap as
	// the unclaimed baseline. Before the fix the dc.delivery_id IS NULL
	// anti-join collapsed the loose index-scan into a full hash anti-join over
	// the whole ready set once unclaimed rows went sparse (~74x slower,
	// hundreds of ms at this size). Bound generously to stay non-flaky on noisy
	// CI containers while still catching a return to the anti-join walk.
	const maxClaimedLatency = 100 * time.Millisecond
	if claimedDur > maxClaimedLatency {
		t.Errorf("fully-claimed discovery took %s (> %s): discovery likely regressed to walking claimed rows in idx_deliveries_ready",
			claimedDur, maxClaimedLatency)
	}
}

// claimEntireBacklog inserts a fresh delivery_claims row for every delivery
// that does not already have one, marking the whole backlog in-flight without
// touching deliveries.status (matching the production claim model).
func claimEntireBacklog(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO delivery_claims (delivery_id, attempt_number, claimed_at)
		SELECT d.id, 1, now()
		FROM deliveries d
		LEFT JOIN delivery_claims dc ON dc.delivery_id = d.id
		WHERE dc.delivery_id IS NULL
	`); err != nil {
		t.Fatalf("claim entire backlog: %v", err)
	}
}
