package legacy

import (
	"strings"
	"testing"
	"time"

	scenarios "github.com/lvluu/webhook-notifier/bench/scenarios"
)

func TestCompareBreakerIsolationLatencies(t *testing.T) {
	tests := []struct {
		name      string
		control   []scenarios.BreakerIsolationCustomerLatency
		dead      []scenarios.BreakerIsolationCustomerLatency
		wantOK    bool
		wantErr   string
		wantRatio float64
	}{
		{
			name:    "control p95 zero errors",
			control: []scenarios.BreakerIsolationCustomerLatency{{CustomerID: "c1", ControlP95: 0}},
			dead:    []scenarios.BreakerIsolationCustomerLatency{{CustomerID: "c1", ControlP95: 10}},
			wantErr: "control P95 is zero",
		},
		{
			name:      "valid comparison within threshold",
			control:   []scenarios.BreakerIsolationCustomerLatency{{CustomerID: "c1", ControlP95: 100}},
			dead:      []scenarios.BreakerIsolationCustomerLatency{{CustomerID: "c1", ControlP95: 105}},
			wantOK:    true,
			wantRatio: 1.05,
		},
		{
			name:      "valid comparison above threshold",
			control:   []scenarios.BreakerIsolationCustomerLatency{{CustomerID: "c1", ControlP95: 100}},
			dead:      []scenarios.BreakerIsolationCustomerLatency{{CustomerID: "c1", ControlP95: 120}},
			wantOK:    false,
			wantRatio: 1.20,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := compareBreakerIsolationLatencies(tc.control, tc.dead)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("compareBreakerIsolationLatencies error: %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if len(got) != 1 {
				t.Fatalf("got len=%d want=1", len(got))
			}
			if got[0].Ratio != tc.wantRatio {
				t.Fatalf("ratio=%.2f want=%.2f", got[0].Ratio, tc.wantRatio)
			}
		})
	}
}

func TestBreakerIsolationBreakerPollingConfig(t *testing.T) {
	tests := []struct {
		name             string
		profile          string
		wantTotalTimeout time.Duration
	}{
		{name: "dev profile shorter timeout", profile: scenarios.BenchProfileDev, wantTotalTimeout: 90 * time.Second},
		{name: "stress profile full timeout", profile: scenarios.BenchProfileStress, wantTotalTimeout: 5 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BENCH_PROFILE", tc.profile)
			total, interval := scenarios.BreakerIsolationBreakerPollingConfig()
			if total != tc.wantTotalTimeout {
				t.Fatalf("total timeout = %s, want %s", total, tc.wantTotalTimeout)
			}
			if interval < 250*time.Millisecond || interval > 2*time.Second {
				t.Fatalf("poll interval out of bounds: %s", interval)
			}
		})
	}
}

func TestTerminalFastFailWebhookURLMatches(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected string
		want     bool
	}{
		{name: "exact match", url: "http://127.0.0.1:1234/hooks/good", expected: "http://127.0.0.1:1234/hooks/good", want: true},
		{name: "suffix-only should fail", url: "http://127.0.0.1:1234/hooks/good", expected: "https://another-host/hooks/good", want: false},
		{name: "trim spaces still exact", url: " http://127.0.0.1:1234/hooks/good ", expected: "http://127.0.0.1:1234/hooks/good", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := terminalFastFailWebhookURLMatches(tc.url, tc.expected)
			if got != tc.want {
				t.Fatalf("terminalFastFailWebhookURLMatches(%q,%q)=%v want %v", tc.url, tc.expected, got, tc.want)
			}
		})
	}
}
