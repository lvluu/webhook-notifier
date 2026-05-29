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
