package outbox

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestLockWebhooksOrderedPreventsLockCycle is the regression guard for the
// 40P01 deadlock that appeared once the fleet survived long enough to expose it
// (deadlock-prone since the set-based breaker bookkeeping landed): concurrent
// mark transactions locked the same webhook breaker rows in opposite order.
//
// The test reproduces that exact cross deterministically with a barrier, then
// proves lockWebhooksOrdered eliminates it:
//
//   - Control branch (no ordered pre-lock): each transaction takes one row
//     lock, meets at a barrier so both are held, then requests the other row.
//     This MUST deadlock — asserting it does guarantees the fix assertion is not
//     vacuous.
//   - Fix branch: each transaction first calls lockWebhooksOrdered for both rows
//     in opposite slice order. Because the helper locks in a single id-ordered
//     statement, both transactions contend on the lower id first and serialize
//     instead of forming a cycle. No deadlock.
//
// If lockWebhooksOrdered is removed or stops ordering, the fix branch deadlocks
// and the test fails.
func TestLockWebhooksOrderedPreventsLockCycle(t *testing.T) {
	ctx := context.Background()
	db := newLockOrderTestDB(t, ctx)

	a := insertLockTestWebhook(t, ctx, db, "https://partner.example/a")
	b := insertLockTestWebhook(t, ctx, db, "https://partner.example/b")
	lo, hi := a, b
	if lo > hi {
		lo, hi = hi, lo
	}

	if !runOppositeOrderLockCycle(t, ctx, db, false, lo, hi) {
		t.Fatal("control scenario (no ordered pre-lock) did not deadlock; cannot validate the fix")
	}

	if runOppositeOrderLockCycle(t, ctx, db, true, lo, hi) {
		t.Fatal("lockWebhooksOrdered did not prevent the opposite-order deadlock cycle")
	}
}

// runOppositeOrderLockCycle runs two transactions that lock webhooks lo and hi
// in opposite order. With preLock, each first acquires both via
// lockWebhooksOrdered. Returns true if either transaction was aborted with a
// Postgres deadlock (SQLSTATE 40P01).
func runOppositeOrderLockCycle(t *testing.T, ctx context.Context, db *sql.DB, preLock bool, lo, hi string) bool {
	t.Helper()

	g1arrived := make(chan struct{})
	g2arrived := make(chan struct{})
	var deadlock atomic.Bool
	var wg sync.WaitGroup
	wg.Add(2)

	run := func(first, second string, arrived, other chan struct{}) {
		defer wg.Done()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Errorf("begin tx: %v", err)
			return
		}
		defer func() { _ = tx.Rollback() }()

		if preLock {
			if err := lockWebhooksOrdered(ctx, tx, []string{first, second}); err != nil {
				recordLockErr(t, &deadlock, "pre-lock", err)
				return
			}
		}
		if err := lockWebhookRow(ctx, tx, first); err != nil {
			recordLockErr(t, &deadlock, "lock first", err)
			return
		}

		// Barrier: signal arrival and wait for the peer so both hold their first
		// lock before either crosses. Bounded so the fix branch (where the peer
		// is intentionally blocked inside lockWebhooksOrdered) does not hang.
		close(arrived)
		select {
		case <-other:
		case <-time.After(300 * time.Millisecond):
		}

		if err := lockWebhookRow(ctx, tx, second); err != nil {
			recordLockErr(t, &deadlock, "lock second", err)
			return
		}
		_ = tx.Commit()
	}

	go run(lo, hi, g1arrived, g2arrived)
	go run(hi, lo, g2arrived, g1arrived)
	wg.Wait()

	return deadlock.Load()
}

func recordLockErr(t *testing.T, deadlock *atomic.Bool, phase string, err error) {
	t.Helper()
	if isDeadlockErr(err) {
		deadlock.Store(true)
		return
	}
	t.Errorf("%s: unexpected error: %v", phase, err)
}

func lockWebhookRow(ctx context.Context, tx *sql.Tx, id string) error {
	_, err := tx.ExecContext(ctx, `SELECT id FROM webhooks WHERE id = $1::uuid FOR UPDATE`, id)
	return err
}

func isDeadlockErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

func insertLockTestWebhook(t *testing.T, ctx context.Context, db *sql.DB, url string) string {
	t.Helper()
	var id string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
		VALUES ('cus_lock', $1, 'test-secret', ARRAY['evt'], true)
		RETURNING id::text
	`, url).Scan(&id); err != nil {
		t.Fatalf("insert webhook: %v", err)
	}
	return id
}

func newLockOrderTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	ctr, err := postgres.RunContainer(
		ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithDatabase("notifier"),
		postgres.WithUsername("notifier"),
		postgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(15*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Errorf("terminate postgres container: %v", err)
		}
	})

	connString, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(files)
	for _, file := range files {
		sqlBytes, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read migration %s: %v", file, err)
		}
		if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply migration %s: %v", file, err)
		}
	}

	return db
}
