package worker_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lvluu/webhook-notifier/internal/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newPostgresDBForDistributedLimiter(t *testing.T, ctx context.Context) *sql.DB {
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

	applyMigrationsForDistributedLimiter(t, ctx, db)

	return db
}

func applyMigrationsForDistributedLimiter(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

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
}

func TestDistributedHostLimiter_TryAcquireSucceedsAndFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	limiter := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:   2,
		LeaseTTL:     5 * time.Second,
		RequeueDelay: 100 * time.Millisecond,
	})

	release1, err := limiter.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire 1: %v", err)
	}

	release2, err := limiter.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire 2: %v", err)
	}

	_, err = limiter.TryAcquire("partner.example.com")
	if err == nil {
		t.Fatal("TryAcquire 3 should fail at cap")
	}

	release1()

	release3, err := limiter.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire after release: %v", err)
	}
	release2()
	release3()
}

func TestDistributedHostLimiter_AcquireBlocksAndTimesOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	limiter := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:     1,
		LeaseTTL:       5 * time.Second,
		AcquireTimeout: 200 * time.Millisecond,
		RequeueDelay:   100 * time.Millisecond,
	})

	release1, err := limiter.Acquire(ctx, "partner.example.com")
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}

	_, err = limiter.Acquire(ctx, "partner.example.com")
	if err == nil {
		t.Fatal("Acquire at cap should timeout")
	}

	release1()

	release2, err := limiter.Acquire(ctx, "partner.example.com")
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	release2()
}

func TestDistributedHostLimiter_RequeueDelay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	limiter := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:   1,
		LeaseTTL:     5 * time.Second,
		RequeueDelay: 250 * time.Millisecond,
	})

	if got, want := limiter.RequeueDelay(), 250*time.Millisecond; got != want {
		t.Errorf("RequeueDelay: got %v, want %v", got, want)
	}
}

func TestDistributedHostLimiter_DifferentHostsIndependent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	limiter := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:     1,
		LeaseTTL:       5 * time.Second,
		AcquireTimeout: 100 * time.Millisecond,
		RequeueDelay:   10 * time.Millisecond,
	})

	releaseA, err := limiter.TryAcquire("partner-a.example.com")
	if err != nil {
		t.Fatalf("TryAcquire partner-a: %v", err)
	}

	releaseB, err := limiter.TryAcquire("partner-b.example.com")
	if err != nil {
		t.Fatalf("TryAcquire partner-b: %v", err)
	}

	_, err = limiter.TryAcquire("partner-a.example.com")
	if err == nil {
		t.Fatal("partner-a should be at cap")
	}

	releaseA()
	releaseB()
}

func TestDistributedHostLimiter_TwoInstancesShareCap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	const host = "partner.example.com"
	limiter1 := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:     2,
		LeaseTTL:       30 * time.Second,
		AcquireTimeout: 100 * time.Millisecond,
		RequeueDelay:   10 * time.Millisecond,
	})
	limiter2 := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:     2,
		LeaseTTL:       30 * time.Second,
		AcquireTimeout: 100 * time.Millisecond,
		RequeueDelay:   10 * time.Millisecond,
	})

	release1, err := limiter1.TryAcquire(host)
	if err != nil {
		t.Fatalf("limiter1 TryAcquire 1: %v", err)
	}

	release2, err := limiter2.TryAcquire(host)
	if err != nil {
		t.Fatalf("limiter2 TryAcquire 1: %v", err)
	}

	_, err = limiter1.TryAcquire(host)
	if err == nil {
		t.Fatal("limiter1 TryAcquire 2 should fail (shared cap exhausted)")
	}

	_, err = limiter2.TryAcquire(host)
	if err == nil {
		t.Fatal("limiter2 TryAcquire 2 should fail (shared cap exhausted)")
	}

	release1()

	release3, err := limiter2.TryAcquire(host)
	if err != nil {
		t.Fatalf("limiter2 TryAcquire after release: %v", err)
	}
	release2()
	release3()
}

func TestDistributedHostLimiter_StaleLeasesDontConsumeCap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	const host = "partner.example.com"
	limiter := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
		MaxPerHost:     1,
		LeaseTTL:       100 * time.Millisecond,
		AcquireTimeout: 100 * time.Millisecond,
		RequeueDelay:   10 * time.Millisecond,
	})

	release, err := limiter.TryAcquire(host)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	release()

	// Wait for lease TTL to expire
	time.Sleep(150 * time.Millisecond)

	release2, err := limiter.TryAcquire(host)
	if err != nil {
		t.Fatalf("TryAcquire after stale lease expired: %v", err)
	}
	release2()
}

func TestDistributedHostLimiter_NeverExceedsCapConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := newPostgresDBForDistributedLimiter(t, ctx)

	const (
		deliveries = 50
		host       = "partner.example.com"
		cap        = 5
	)

	var (
		mu          sync.Mutex
		inFlight    int
		maxInFlight int
	)
	hold := make(chan struct{})

	doAcquire := func() {
		limiter := worker.NewDistributedHostLimiter(db, worker.DistributedHostLimiterConfig{
			MaxPerHost:     cap,
			LeaseTTL:       30 * time.Second,
			AcquireTimeout: 30 * time.Second,
			RequeueDelay:   10 * time.Millisecond,
		})
		release, err := limiter.Acquire(ctx, host)
		if err != nil {
			panic(fmt.Sprintf("acquire: %v", err))
		}
		defer release()

		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()

		<-hold

		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doAcquire()
		}()
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	if maxInFlight > cap {
		t.Errorf("max in-flight: got %d, want <= %d", maxInFlight, cap)
	}
	mu.Unlock()

	close(hold)
	wg.Wait()
}
