// Package roundrobin_test contains unit tests for the roundrobin scheduler.
package roundrobin_test

import (
	"context"
	"math"
	"testing"

	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
)

// TestRoundRobin_TableDriven covers the three acceptance scenarios from #7:
// empty schedule, single-Customer back-to-back, multi-Customer interleaving.
func TestRoundRobin_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		notify  []string
		picks   int
		want    []string
		wantOK  []bool
		wantErr bool
	}{
		{
			name:   "empty schedule returns ok=false",
			notify: nil,
			picks:  1,
			want:   []string{""},
			wantOK: []bool{false},
		},
		{
			name:   "single Customer stays in rotation",
			notify: []string{"cus_A"},
			picks:  3,
			want:   []string{"cus_A", "cus_A", "cus_A"},
			wantOK: []bool{true, true, true},
		},
		{
			name:   "multi Customer cycles in insertion order",
			notify: []string{"cus_A", "cus_B", "cus_C"},
			picks:  6,
			want:   []string{"cus_A", "cus_B", "cus_C", "cus_A", "cus_B", "cus_C"},
			wantOK: []bool{true, true, true, true, true, true},
		},
		{
			name:   "duplicate Notify is idempotent",
			notify: []string{"cus_A", "cus_B", "cus_A", "cus_B"},
			picks:  4,
			want:   []string{"cus_A", "cus_B", "cus_A", "cus_B"},
			wantOK: []bool{true, true, true, true},
		},
		{
			name:   "empty CustomerID Notify is ignored",
			notify: []string{"", "cus_A", ""},
			picks:  3,
			want:   []string{"cus_A", "cus_A", "cus_A"},
			wantOK: []bool{true, true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := roundrobin.New()
			for _, c := range tt.notify {
				s.Notify(c)
			}

			ctx := context.Background()
			for i := 0; i < tt.picks; i++ {
				got, batchHint, ok, err := s.Pick(ctx)
				if (err != nil) != tt.wantErr {
					t.Fatalf("Pick[%d] err: got %v, wantErr=%v", i, err, tt.wantErr)
				}
				if ok != tt.wantOK[i] {
					t.Errorf("Pick[%d] ok: got %v, want %v", i, ok, tt.wantOK[i])
				}
				if got != tt.want[i] {
					t.Errorf("Pick[%d]: got %q, want %q", i, got, tt.want[i])
				}
				if ok && batchHint != roundrobin.ClaimBatchSize {
					t.Errorf("Pick[%d] batchHint: got %d, want %d", i, batchHint, roundrobin.ClaimBatchSize)
				}
			}
		})
	}
}

// TestRoundRobin_PickRespectsContextCancellation guards against a scheduler
// that silently ignores ctx cancellation — matters once the worker loop blocks
// on Pick during shutdown.
func TestRoundRobin_PickRespectsContextCancellation(t *testing.T) {
	t.Parallel()

	s := roundrobin.New()
	s.Notify("cus_A")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := s.Pick(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

// TestRoundRobin_FairnessProperty is the property test from #7: across any
// arrival/draw sequence, no Customer's share of picks exceeds
// 1/active_count + ε over a sliding window.
//
// ε = 0.01 is generous enough to absorb the at-most-one rounding error at
// window boundaries while still catching any implementation that favours a
// Customer.
func TestRoundRobin_FairnessProperty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		customers []string
		window    int
		epsilon   float64
	}{
		{
			name:      "three Customers over 300 picks",
			customers: []string{"cus_A", "cus_B", "cus_C"},
			window:    300,
			epsilon:   0.01,
		},
		{
			name:      "ten Customers over 1000 picks",
			customers: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
			window:    1000,
			epsilon:   0.01,
		},
		{
			name:      "whale vs small — two Customers over 100 picks",
			customers: []string{"whale", "small"},
			window:    100,
			epsilon:   0.01,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := roundrobin.New()
			for _, c := range tt.customers {
				s.Notify(c)
			}

			counts := make(map[string]int, len(tt.customers))
			ctx := context.Background()
			for i := 0; i < tt.window; i++ {
				customer, _, ok, err := s.Pick(ctx)
				if err != nil {
					t.Fatalf("Pick[%d]: %v", i, err)
				}
				if !ok {
					t.Fatalf("Pick[%d]: got ok=false, want ok=true", i)
				}
				counts[customer]++
			}

			activeCount := len(tt.customers)
			expectedShare := 1.0 / float64(activeCount)

			for customer, count := range counts {
				share := float64(count) / float64(tt.window)
				delta := math.Abs(share - expectedShare)
				if delta > tt.epsilon {
					t.Errorf(
						"Customer %s share %.4f deviates from expected %.4f by %.4f (ε=%.4f)",
						customer, share, expectedShare, delta, tt.epsilon,
					)
				}
			}

			// Every active Customer must have been picked at least once.
			for _, c := range tt.customers {
				if counts[c] == 0 {
					t.Errorf("Customer %s was never picked in %d draws", c, tt.window)
				}
			}
		})
	}
}

// TestRoundRobin_Remove verifies that Remove correctly removes a Customer from
// rotation and that the cursor adjusts properly.
func TestRoundRobin_Remove(t *testing.T) {
	t.Parallel()

	t.Run("remove from single Customer leaves scheduler empty", func(t *testing.T) {
		t.Parallel()
		s := roundrobin.New()
		s.Notify("cus_A")
		s.Remove("cus_A")
		_, _, ok, _ := s.Pick(context.Background())
		if ok {
			t.Fatal("expected ok=false after removing only customer")
		}
	})

	t.Run("remove from middle preserves order", func(t *testing.T) {
		t.Parallel()
		s := roundrobin.New()
		s.Notify("cus_A")
		s.Notify("cus_B")
		s.Notify("cus_C")
		s.Remove("cus_B")

		ctx := context.Background()
		got1, _, ok1, _ := s.Pick(ctx)
		got2, _, ok2, _ := s.Pick(ctx)
		got3, _, ok3, _ := s.Pick(ctx)
		if !ok1 || !ok2 || !ok3 {
			t.Fatal("expected three successful picks from 3→2 customers after remove")
		}
		if got1 != "cus_A" || got2 != "cus_C" || got3 != "cus_A" {
			t.Fatalf("got %q %q %q, want cus_A cus_C cus_A", got1, got2, got3)
		}
	})

	t.Run("remove from cursor position adjusts cursor", func(t *testing.T) {
		t.Parallel()
		s := roundrobin.New()
		s.Notify("cus_A")
		s.Notify("cus_B")
		s.Notify("cus_C")

		ctx := context.Background()
		// Advance cursor past A.
		s.Pick(ctx) // returns A, cursor now at 1 (B)

		// Remove A (before cursor) — cursor should shift down.
		s.Remove("cus_A")

		got1, _, ok1, _ := s.Pick(ctx)
		got2, _, ok2, _ := s.Pick(ctx)
		if !ok1 || !ok2 {
			t.Fatal("expected two successful picks")
		}
		if got1 != "cus_B" || got2 != "cus_C" {
			t.Fatalf("got %q %q, want cus_B cus_C", got1, got2)
		}
	})

	t.Run("no-op for unknown customer", func(t *testing.T) {
		t.Parallel()
		s := roundrobin.New()
		s.Notify("cus_A")
		s.Remove("unknown")
		got, _, ok, _ := s.Pick(context.Background())
		if !ok || got != "cus_A" {
			t.Fatalf("got %q ok=%v, want cus_A ok=true", got, ok)
		}
	})

	t.Run("no-op for empty ID", func(t *testing.T) {
		t.Parallel()
		s := roundrobin.New()
		s.Notify("cus_A")
		s.Remove("")
		got, _, ok, _ := s.Pick(context.Background())
		if !ok || got != "cus_A" {
			t.Fatalf("got %q ok=%v, want cus_A ok=true", got, ok)
		}
	})
}

// TestRoundRobin_ConcurrentNotifyAndPick exercises thread safety — the
// Scheduler contract requires safe concurrent use.
func TestRoundRobin_ConcurrentNotifyAndPick(t *testing.T) {
	t.Parallel()

	s := roundrobin.New()
	ctx := context.Background()

	const goroutines = 8
	const opsPerGoroutine = 500

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < goroutines*opsPerGoroutine; i++ {
			_, _, _, _ = s.Pick(ctx)
		}
	}()

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			for i := 0; i < opsPerGoroutine; i++ {
				s.Notify("cus_" + string(rune('A'+g)))
			}
		}(g)
	}

	<-done
}
