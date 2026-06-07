package dbweights_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lvluu/webhook-notifier/internal/scheduler/dbweights"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newTestDB(t *testing.T, ctx context.Context) *sql.DB {
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
				WithStartupTimeout(10*time.Second),
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
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.ExecContext(ctx, `
		CREATE TABLE customers (
			id text PRIMARY KEY,
			class integer NOT NULL DEFAULT 1 CHECK (class >= 0),
			created_at timestamptz NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		t.Fatalf("create customers table: %v", err)
	}

	return db
}

func seedCustomer(t *testing.T, ctx context.Context, db *sql.DB, id string, class uint32) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO customers (id, class) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET class = $2`, id, class)
	if err != nil {
		t.Fatalf("seed customer %s: %v", id, err)
	}
}

func TestWeight_KnownCustomer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)
	seedCustomer(t, ctx, db, "whale", 3)
	seedCustomer(t, ctx, db, "small", 1)

	ws := dbweights.New(db)

	if w := ws.Weight("whale"); w != 3 {
		t.Errorf("whale weight = %d, want 3", w)
	}
	if w := ws.Weight("small"); w != 1 {
		t.Errorf("small weight = %d, want 1", w)
	}
}

func TestWeight_UnknownCustomerDefaultsToOne(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)

	ws := dbweights.New(db)

	if w := ws.Weight("nonexistent"); w != 1 {
		t.Errorf("unknown customer weight = %d, want 1", w)
	}
}

func TestWeight_ZeroWeight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)
	seedCustomer(t, ctx, db, "paused", 0)

	ws := dbweights.New(db)

	if w := ws.Weight("paused"); w != 0 {
		t.Errorf("paused customer weight = %d, want 0", w)
	}
}

func TestWeight_CachesResult(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)
	seedCustomer(t, ctx, db, "cached", 5)

	ws := dbweights.New(db)

	// First call hits DB and caches.
	if w := ws.Weight("cached"); w != 5 {
		t.Fatalf("first weight = %d, want 5", w)
	}

	// Change the DB row directly — cache should serve stale value.
	seedCustomer(t, ctx, db, "cached", 99)

	if w := ws.Weight("cached"); w != 5 {
		t.Errorf("cached weight = %d, want 5 (stale from cache)", w)
	}
}

func TestWeight_ReloadsAfterCacheTTL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)
	seedCustomer(t, ctx, db, "ttl", 5)

	ws := dbweights.New(db, dbweights.WithCacheTTL(time.Millisecond))

	if w := ws.Weight("ttl"); w != 5 {
		t.Fatalf("first weight = %d, want 5", w)
	}

	seedCustomer(t, ctx, db, "ttl", 9)
	time.Sleep(5 * time.Millisecond)

	if w := ws.Weight("ttl"); w != 9 {
		t.Errorf("expired weight = %d, want 9", w)
	}
}

func TestWeight_RefreshInvalidatesCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)
	seedCustomer(t, ctx, db, "refreshme", 7)

	ws := dbweights.New(db)

	if w := ws.Weight("refreshme"); w != 7 {
		t.Fatalf("first weight = %d, want 7", w)
	}

	// Change DB and refresh.
	seedCustomer(t, ctx, db, "refreshme", 42)
	ws.Refresh("refreshme")

	if w := ws.Weight("refreshme"); w != 42 {
		t.Errorf("refreshed weight = %d, want 42", w)
	}
}

func TestWeight_EmptyID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)

	ws := dbweights.New(db)

	if w := ws.Weight(""); w != 1 {
		t.Errorf("empty id weight = %d, want 1", w)
	}
}

func TestWeight_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t, ctx)
	seedCustomer(t, ctx, db, "concurrent", 8)

	ws := dbweights.New(db)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w := ws.Weight("concurrent"); w != 8 {
				t.Errorf("concurrent weight = %d, want 8", w)
			}
		}()
	}
	wg.Wait()
}
