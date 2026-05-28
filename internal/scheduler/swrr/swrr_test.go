// Package swrr_test contains unit tests for the SWRR scheduler.
package swrr_test

import (
	"context"
	"math"
	"testing"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/scheduler/swrr"
)

func TestSWRR_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		weights   map[string]uint32
		notify    []string
		picks     int
		want      []string
		wantOK    []bool
		wantHints []uint32
		wantErr   bool
	}{
		{
			name:      "empty schedule returns ok=false",
			weights:   map[string]uint32{"cus_A": 1},
			notify:    nil,
			picks:     1,
			want:      []string{""},
			wantOK:    []bool{false},
			wantHints: []uint32{0},
		},
		{
			name:      "single Customer exhausted after one Pick",
			weights:   map[string]uint32{"cus_A": 1},
			notify:    []string{"cus_A"},
			picks:     2,
			want:      []string{"cus_A", ""},
			wantOK:    []bool{true, false},
			wantHints: []uint32{1, 0},
		},
		{
			name:      "multi Customer interleaving uniform",
			weights:   map[string]uint32{"cus_A": 1, "cus_B": 1, "cus_C": 1},
			notify:    []string{"cus_A", "cus_B", "cus_C"},
			picks:     4,
			want:      []string{"cus_A", "cus_B", "cus_C", ""},
			wantOK:    []bool{true, true, true, false},
			wantHints: []uint32{1, 1, 1, 0},
		},
		{
			name:      "multi Customer non-uniform single cycle",
			weights:   map[string]uint32{"cus_A": 3, "cus_B": 1},
			notify:    []string{"cus_A", "cus_B"},
			picks:     4,
			want:      []string{"cus_A", "cus_B", "", ""},
			wantOK:    []bool{true, true, false, false},
			wantHints: []uint32{3, 1, 0, 0},
		},
		{
			name:      "weight=0 Customer is skipped",
			weights:   map[string]uint32{"cus_A": 1, "cus_B": 0},
			notify:    []string{"cus_A", "cus_B"},
			picks:     2,
			want:      []string{"cus_A", ""},
			wantOK:    []bool{true, false},
			wantHints: []uint32{1, 0},
		},
		{
			name:      "duplicate Notify is idempotent",
			weights:   map[string]uint32{"cus_A": 1, "cus_B": 1},
			notify:    []string{"cus_A", "cus_B", "cus_A", "cus_B"},
			picks:     3,
			want:      []string{"cus_A", "cus_B", ""},
			wantOK:    []bool{true, true, false},
			wantHints: []uint32{1, 1, 0},
		},
		{
			name:      "empty CustomerID Notify is ignored",
			weights:   map[string]uint32{"cus_A": 1},
			notify:    []string{"", "cus_A", ""},
			picks:     2,
			want:      []string{"cus_A", ""},
			wantOK:    []bool{true, false},
			wantHints: []uint32{1, 0},
		},
		{
			name:      "batchHint equals weight",
			weights:   map[string]uint32{"cus_A": 5},
			notify:    []string{"cus_A"},
			picks:     1,
			want:      []string{"cus_A"},
			wantOK:    []bool{true},
			wantHints: []uint32{5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ws := staticWeights{weights: tt.weights}
			s := swrr.New(ws)
			for _, c := range tt.notify {
				s.Notify(c)
			}

			ctx := context.Background()
			for i := 0; i < tt.picks; i++ {
				got, hint, ok, err := s.Pick(ctx)
				if (err != nil) != tt.wantErr {
					t.Fatalf("Pick[%d] err: got %v, wantErr=%v", i, err, tt.wantErr)
				}
				if ok != tt.wantOK[i] {
					t.Errorf("Pick[%d] ok: got %v, want %v", i, ok, tt.wantOK[i])
				}
				if got != tt.want[i] {
					t.Errorf("Pick[%d]: got %q, want %q", i, got, tt.want[i])
				}
				if hint != tt.wantHints[i] {
					t.Errorf("Pick[%d] batchHint: got %d, want %d", i, hint, tt.wantHints[i])
				}
			}
		})
	}
}

func TestSWRR_NonUniformInterleavingWithReNotification(t *testing.T) {
	t.Parallel()

	ws := staticWeights{weights: map[string]uint32{"cus_A": 3, "cus_B": 1}}
	s := swrr.New(ws)
	s.Notify("cus_A")
	s.Notify("cus_B")

	ctx := context.Background()
	var got []string
	for i := 0; i < 8; i++ {
		customer, _, ok, err := s.Pick(ctx)
		if err != nil {
			t.Fatalf("Pick[%d]: %v", i, err)
		}
		if !ok {
			t.Fatalf("Pick[%d]: unexpected ok=false", i)
		}
		got = append(got, customer)
		s.Notify(customer)
	}

	want := []string{"cus_A", "cus_B", "cus_A", "cus_A", "cus_A", "cus_B", "cus_A", "cus_A"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Pick[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSWRR_PickRespectsContextCancellation(t *testing.T) {
	t.Parallel()

	s := swrr.New(scheduler.ConstantWeights{Value: 1})
	s.Notify("cus_A")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := s.Pick(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

func TestSWRR_FairnessPropertyUniform(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := swrr.New(scheduler.ConstantWeights{Value: 1})
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
				s.Notify(customer)
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

			for _, c := range tt.customers {
				if counts[c] == 0 {
					t.Errorf("Customer %s was never picked in %d draws", c, tt.window)
				}
			}
		})
	}
}

func TestSWRR_FairnessPropertyNonUniform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		weights map[string]uint32
		window  int
		epsilon float64
	}{
		{
			name:    "weights [3,1] over 400 picks",
			weights: map[string]uint32{"cus_A": 3, "cus_B": 1},
			window:  400,
			epsilon: 0.02,
		},
		{
			name:    "weights [2,2,1] over 500 picks",
			weights: map[string]uint32{"cus_A": 2, "cus_B": 2, "cus_C": 1},
			window:  500,
			epsilon: 0.02,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ws := staticWeights{weights: tt.weights}
			s := swrr.New(ws)
			for id := range tt.weights {
				s.Notify(id)
			}

			var totalWeight int64
			for _, w := range tt.weights {
				totalWeight += int64(w)
			}

			counts := make(map[string]int, len(tt.weights))
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
				s.Notify(customer)
			}

			for customer, weight := range tt.weights {
				expectedShare := float64(weight) / float64(totalWeight)
				actualShare := float64(counts[customer]) / float64(tt.window)
				delta := math.Abs(actualShare - expectedShare)
				if delta > tt.epsilon {
					t.Errorf(
						"Customer %s share %.4f deviates from expected %.4f by %.4f (ε=%.4f)",
						customer, actualShare, expectedShare, delta, tt.epsilon,
					)
				}
			}
		})
	}
}

func TestSWRR_Smoothness(t *testing.T) {
	t.Parallel()

	// Weights [2,1] should produce max run ≤ 2 (e.g. ABA ABA).
	// Weights [3,1] naturally allows runs of 3 with standard Nginx SWRR.
	ws := staticWeights{weights: map[string]uint32{"cus_A": 2, "cus_B": 1}}
	s := swrr.New(ws)
	s.Notify("cus_A")
	s.Notify("cus_B")

	ctx := context.Background()
	var sequence []string
	for i := 0; i < 100; i++ {
		customer, _, ok, err := s.Pick(ctx)
		if err != nil {
			t.Fatalf("Pick[%d]: %v", i, err)
		}
		if !ok {
			t.Fatalf("Pick[%d]: unexpected ok=false", i)
		}
		sequence = append(sequence, customer)
		s.Notify(customer)
	}

	maxRun := 0
	currentRun := 0
	var prev string
	for _, c := range sequence {
		if c == prev {
			currentRun++
		} else {
			currentRun = 1
			prev = c
		}
		if currentRun > maxRun {
			maxRun = currentRun
		}
	}

	if maxRun > 2 {
		t.Errorf("max run %d > 2 for [2,1]; sequence: %v", maxRun, sequence[:16])
	}
}

func TestSWRR_ConcurrentNotifyAndPick(t *testing.T) {
	t.Parallel()

	ws := staticWeights{weights: map[string]uint32{
		"cus_A": 1, "cus_B": 1, "cus_C": 1, "cus_D": 1,
		"cus_E": 1, "cus_F": 1, "cus_G": 1, "cus_H": 1,
	}}
	s := swrr.New(ws)
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

func TestSWRR_WeightZeroNeverPickedWhilePending(t *testing.T) {
	t.Parallel()

	ws := staticWeights{weights: map[string]uint32{"cus_A": 1, "cus_B": 0}}
	s := swrr.New(ws)

	for i := 0; i < 10; i++ {
		s.Notify("cus_A")
		s.Notify("cus_B")
	}

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		customer, _, ok, err := s.Pick(ctx)
		if err != nil {
			t.Fatalf("Pick[%d]: %v", i, err)
		}
		if !ok {
			break
		}
		if customer == "cus_B" {
			t.Fatalf("Pick[%d]: got weight=0 Customer cus_B", i)
		}
		s.Notify(customer)
	}
}

type staticWeights struct {
	weights map[string]uint32
}

func (s staticWeights) Weight(id scheduler.CustomerID) uint32 {
	if w, ok := s.weights[id]; ok {
		return w
	}
	return 1
}
