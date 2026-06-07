package worker_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/worker"
)

func TestHostLimiter_AcquireAndRelease(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     2,
		AcquireTimeout: 1 * time.Second,
		RequeueDelay:   100 * time.Millisecond,
	})

	// Acquire two slots on the same host — should succeed immediately.
	release1, err := hl.Acquire(context.Background(), "partner.example.com")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}

	release2, err := hl.Acquire(context.Background(), "partner.example.com")
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	// Release the first slot. The second is still held.
	release1()

	// A third acquire should now succeed because one slot was released.
	release3, err := hl.Acquire(context.Background(), "partner.example.com")
	if err != nil {
		t.Fatalf("acquire 3 after release: %v", err)
	}

	release2()
	release3()
}

func TestHostLimiter_ConcurrentAcquisitionsBlockedAtCap(t *testing.T) {
	t.Parallel()

	const cap = 3
	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     cap,
		AcquireTimeout: 5 * time.Second,
		RequeueDelay:   100 * time.Millisecond,
	})

	var (
		mu          sync.Mutex
		inFlight    int
		maxInFlight int
	)

	hold := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < cap; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := hl.Acquire(context.Background(), "partner.example.com")
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
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
		}()
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if maxInFlight != cap {
		t.Errorf("max in-flight: got %d, want %d", maxInFlight, cap)
	}
	mu.Unlock()

	var unblocked atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		release, err := hl.Acquire(context.Background(), "partner.example.com")
		if err != nil {
			t.Errorf("4th acquire: %v", err)
			return
		}
		defer release()
		unblocked.Add(1)
	}()

	time.Sleep(100 * time.Millisecond)
	if unblocked.Load() != 0 {
		t.Fatal("4th acquire should be blocked until a slot is released")
	}

	close(hold)
	wg.Wait()

	if unblocked.Load() != 1 {
		t.Errorf("4th acquire unblocked count: got %d, want 1", unblocked.Load())
	}
}

func TestHostLimiter_AcquireTimeoutDoesNotConsumeSlot(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: 50 * time.Millisecond,
		RequeueDelay:   10 * time.Millisecond,
	})

	release1, err := hl.Acquire(context.Background(), "partner.example.com")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}

	_, err = hl.Acquire(context.Background(), "partner.example.com")
	if err == nil {
		t.Fatal("expected timeout error when capacity is full")
	}

	release1()

	release2, err := hl.Acquire(context.Background(), "partner.example.com")
	if err != nil {
		t.Fatalf("acquire after release should succeed: %v", err)
	}
	release2()
}

func TestHostLimiter_ContextCancellation(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: 5 * time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	release1, err := hl.Acquire(context.Background(), "partner.example.com")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	defer release1()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = hl.Acquire(ctx, "partner.example.com")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestHostLimiter_DifferentHostsAreIndependent(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: 50 * time.Millisecond,
		RequeueDelay:   10 * time.Millisecond,
	})

	release1, err := hl.Acquire(context.Background(), "partner-a.example.com")
	if err != nil {
		t.Fatalf("acquire partner-a: %v", err)
	}

	release2, err := hl.Acquire(context.Background(), "partner-b.example.com")
	if err != nil {
		t.Fatalf("acquire partner-b should succeed independently: %v", err)
	}

	_, err = hl.Acquire(context.Background(), "partner-a.example.com")
	if err == nil {
		t.Fatal("partner-a should be at capacity")
	}

	release1()
	release2()
}

func TestHostLimiter_NeverExceedsCapWith50Deliveries(t *testing.T) {
	t.Parallel()

	const (
		deliveries = 50
		cap        = 5
	)

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     cap,
		AcquireTimeout: 5 * time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	var (
		mu          sync.Mutex
		inFlight    int
		maxInFlight int
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()

		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := hl.Acquire(context.Background(), "partner.example.com")
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer release()

			resp, err := http.DefaultClient.Post(srv.URL, "application/json", bytes.NewReader([]byte(`{}`)))
			if err != nil {
				t.Errorf("post: %v", err)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if maxInFlight > cap {
		t.Errorf("max concurrent in-flight: got %d, want <= %d", maxInFlight, cap)
	}
}

func TestHostLimiter_TryAcquireSucceedsWhenCapacityAvailable(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     2,
		AcquireTimeout: 1 * time.Second,
		RequeueDelay:   100 * time.Millisecond,
	})

	release1, err := hl.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire 1: %v", err)
	}

	release2, err := hl.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire 2: %v", err)
	}

	release1()
	release2()
}

func TestHostLimiter_TryAcquireFailsImmediatelyAtCapacity(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: 5 * time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	release1, err := hl.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire 1: %v", err)
	}
	defer release1()

	started := time.Now()
	_, err = hl.TryAcquire("partner.example.com")
	if err == nil {
		t.Fatal("expected error when capacity is full")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Errorf("TryAcquire should return immediately when full, took %v", elapsed)
	}
}

func TestHostLimiter_CleanupRemovesStaleEntries(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	// Exhaust capacity on a host so the semaphore state is verifiable.
	release1, err := hl.Acquire(context.Background(), "stale.example.com")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Confirm capacity is exhausted.
	_, err = hl.TryAcquire("stale.example.com")
	if err == nil {
		t.Fatal("expected capacity error before cleanup")
	}

	// Release and wait for the entry to become stale.
	release1()
	time.Sleep(60 * time.Millisecond)

	// Cleanup with 50ms maxAge should remove the stale entry.
	hl.Cleanup(50 * time.Millisecond)

	// After cleanup a new entry is created on demand with fresh capacity.
	release2, err := hl.TryAcquire("stale.example.com")
	if err != nil {
		t.Fatalf("TryAcquire after cleanup should succeed: %v", err)
	}
	release2()
}

func TestHostLimiter_CleanupPreservesRecentEntries(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	// Exhaust capacity — after this, the semaphore has 0 permits left.
	release1, err := hl.Acquire(context.Background(), "active.example.com")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release1()

	// Cleanup with a maxAge well beyond the entry's age — the entry survives.
	hl.Cleanup(time.Hour)

	// The original entry is still present; capacity remains exhausted.
	_, err = hl.TryAcquire("active.example.com")
	if err == nil {
		t.Fatal("expected capacity error; entry should still be present after cleanup with long maxAge")
	}
}

func TestHostLimiter_CleanupStaleEntriesDontAffectOtherHosts(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	// Exhaust capacity on stale host.
	releaseStale, err := hl.Acquire(context.Background(), "stale.example.com")
	if err != nil {
		t.Fatalf("Acquire stale: %v", err)
	}
	// Keep stale capacity held so lastUsed does not refresh.

	// Exhaust capacity on active host, then release for later refresh.
	releaseActive, err := hl.Acquire(context.Background(), "active.example.com")
	if err != nil {
		t.Fatalf("Acquire active: %v", err)
	}
	releaseActive()

	// Wait so stale becomes idle.
	time.Sleep(60 * time.Millisecond)

	// Refresh active entry's lastUsed AFTER the sleep.
	releaseActive2, err := hl.TryAcquire("active.example.com")
	if err != nil {
		t.Fatalf("TryAcquire active: %v", err)
	}

	// Cleanup removes stale (lastUsed ~60ms+ old), preserves active (just touched).
	hl.Cleanup(50 * time.Millisecond)

	// Stale host: entry was removed, new entry gets fresh capacity.
	_, err = hl.TryAcquire("stale.example.com")
	if err != nil {
		t.Fatal("TryAcquire stale after cleanup should succeed (new entry with fresh capacity)")
	}

	// Active host: entry survived cleanup, capacity still exhausted.
	_, err = hl.TryAcquire("active.example.com")
	if err == nil {
		t.Fatal("expected capacity error; active entry should survive cleanup")
	}

	releaseActive2()
	releaseStale()
}

func TestHostLimiter_RunCleanupExitsOnCancellation(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := hl.RunCleanup(ctx, time.Hour, time.Hour)
	if err != context.Canceled {
		t.Fatalf("RunCleanup: got %v, want context.Canceled", err)
	}
}

func TestHostLimiter_TryAcquireDoesNotConsumeSlotOnFailure(t *testing.T) {
	t.Parallel()

	hl := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: 1 * time.Second,
		RequeueDelay:   10 * time.Millisecond,
	})

	release1, err := hl.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire 1: %v", err)
	}

	_, err = hl.TryAcquire("partner.example.com")
	if err == nil {
		t.Fatal("expected error when capacity is full")
	}

	release1()

	release2, err := hl.TryAcquire("partner.example.com")
	if err != nil {
		t.Fatalf("TryAcquire after release should succeed: %v", err)
	}
	release2()
}
