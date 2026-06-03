package chaos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/chaos"
)

func TestEmitter_SmoothArrival(t *testing.T) {
	t.Parallel()

	received := atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	const targetRate = 100.0
	const window = 2 * time.Second
	const margin = 0.30

	customerIDs := make([]string, 5)
	for i := range customerIDs {
		customerIDs[i] = fmt.Sprintf("chaos-%04d", i+1)
	}

	emitter := chaos.New(chaos.Config{
		NotifierURL:  srv.URL,
		Rate:         targetRate,
		MaxInFlight:  50,
		CustomerIDs:  customerIDs,
		CustomerBase: "chaos",
		HTTPClient:   srv.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()

	go emitter.Run(ctx)

	<-ctx.Done()

	got := float64(received.Load())
	want := targetRate * window.Seconds()
	lower := want * (1.0 - margin)
	upper := want * (1.0 + margin)

	if got < lower || got > upper {
		t.Fatalf("received %v events in %v (target ~%v), outside ±%.0f%% of %v", received.Load(), window, targetRate, margin*100, want)
	}
}

func TestEmitter_MaxInFlight(t *testing.T) {
	t.Parallel()

	var concurrent atomic.Int64
	var maxSeen atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := concurrent.Add(1)
		for {
			high := maxSeen.Load()
			if n <= high || maxSeen.CompareAndSwap(high, n) {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	customerIDs := make([]string, 1)
	customerIDs[0] = "chaos-0001"

	const maxInFlight = 3

	emitter := chaos.New(chaos.Config{
		NotifierURL:  srv.URL,
		Rate:         1000.0,
		MaxInFlight:  maxInFlight,
		CustomerIDs:  customerIDs,
		CustomerBase: "chaos",
		HTTPClient:   srv.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go emitter.Run(ctx)

	<-ctx.Done()
	time.Sleep(400 * time.Millisecond)

	if got := maxSeen.Load(); got > int64(maxInFlight) {
		t.Fatalf("max concurrent POSTs %d exceeded MaxInFlight %d", got, maxInFlight)
	}
}

func TestEmitter_CounterAccuracy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		wantSent   int64
		wantShed   int64
		wantFailed int64
	}{
		{"200 → sent", http.StatusOK, 1, 0, 0},
		{"503 → shed", http.StatusServiceUnavailable, 0, 1, 0},
		{"429 → shed", http.StatusTooManyRequests, 0, 1, 0},
		{"connection refused → failed", 0, 0, 0, 1},
		{"500 → failed", http.StatusInternalServerError, 0, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var srvURL string
			var client *http.Client

			if tt.statusCode == 0 {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
				srvURL = srv.URL
				client = srv.Client()
				srv.Close()
			} else {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tt.statusCode)
				}))
				t.Cleanup(srv.Close)
				srvURL = srv.URL
				client = srv.Client()
			}

			customerIDs := make([]string, 1)
			customerIDs[0] = "chaos-0001"

			emitter := chaos.New(chaos.Config{
				NotifierURL:  srvURL,
				Rate:         5.0,
				MaxInFlight:  1,
				CustomerIDs:  customerIDs,
				CustomerBase: "chaos",
				HTTPClient:   client,
			})

			var wg sync.WaitGroup
			wg.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			go func() {
				emitter.Run(ctx)
				wg.Done()
			}()
			<-ctx.Done()
			wg.Wait()

			if got := emitter.Sent(); got != tt.wantSent {
				t.Errorf("sent: got %d, want %d", got, tt.wantSent)
			}
			if got := emitter.Shed(); got != tt.wantShed {
				t.Errorf("shed: got %d, want %d", got, tt.wantShed)
			}
			if got := emitter.Failed(); got != tt.wantFailed {
				t.Errorf("failed: got %d, want %d", got, tt.wantFailed)
			}
		})
	}
}

func TestEmitter_CustomerDistribution(t *testing.T) {
	t.Parallel()

	const numCustomers = 5
	customerIDs := make([]string, numCustomers)
	for i := range customerIDs {
		customerIDs[i] = fmt.Sprintf("chaos-%04d", i+1)
	}

	hits := make(map[string]int)
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		var req struct {
			CustomerID string `json:"customer_id"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		mu.Lock()
		hits[req.CustomerID]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	emitter := chaos.New(chaos.Config{
		NotifierURL:  srv.URL,
		Rate:         200.0,
		MaxInFlight:  50,
		CustomerIDs:  customerIDs,
		CustomerBase: "chaos",
		HTTPClient:   srv.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		emitter.Run(ctx)
		wg.Done()
	}()
	<-ctx.Done()
	wg.Wait()

	mu.Lock()
	for _, cid := range customerIDs {
		if hits[cid] == 0 {
			t.Errorf("customer %s was never hit", cid)
		}
	}
	if got := len(hits); got != numCustomers {
		t.Errorf("got %d distinct customers, want %d", got, numCustomers)
	}
	mu.Unlock()
}
