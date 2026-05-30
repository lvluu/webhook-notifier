package scenarios

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/retry"
)

func TestScenario2ProfileFromEnvDefaults(t *testing.T) {
	t.Setenv("BENCH_PROFILE", "")
	t.Setenv("BENCH_REPORT_PATH", "")
	t.Setenv("BENCH_SCENARIO2_CUSTOMERS", "")
	t.Setenv("BENCH_EVENTS_PER_CUSTOMER", "")
	t.Setenv("BENCH_SUBMIT_INTERVAL", "")
	t.Setenv("BENCH_WORKERS", "")

	profile := scenario2ProfileFromEnv()
	if profile.Name != "baseline" {
		t.Fatalf("name: got %q, want baseline", profile.Name)
	}
	if profile.CustomerCount != defaultScenario2CustomerCount {
		t.Fatalf("customers: got %d, want %d", profile.CustomerCount, defaultScenario2CustomerCount)
	}
	if profile.EventsPerCustomer != defaultScenario2EventsPerCustomer {
		t.Fatalf("events/customer: got %d, want %d", profile.EventsPerCustomer, defaultScenario2EventsPerCustomer)
	}
	if profile.SubmitInterval != defaultScenario2SubmitInterval {
		t.Fatalf("submit interval: got %s, want %s", profile.SubmitInterval, defaultScenario2SubmitInterval)
	}
}

func TestScenario2ProfileFromEnvOverrides(t *testing.T) {
	t.Setenv("BENCH_PROFILE", "medium")
	t.Setenv("BENCH_REPORT_PATH", "bench/results/scenario-2-medium.md")
	t.Setenv("BENCH_SCENARIO2_CUSTOMERS", "25")
	t.Setenv("BENCH_EVENTS_PER_CUSTOMER", "200")
	t.Setenv("BENCH_SUBMIT_INTERVAL", "10ms")
	t.Setenv("BENCH_WORKERS", "64")

	profile := scenario2ProfileFromEnv()
	if profile.Name != "medium" {
		t.Fatalf("name: got %q, want medium", profile.Name)
	}
	if profile.ReportPath != "bench/results/scenario-2-medium.md" {
		t.Fatalf("report path: got %q", profile.ReportPath)
	}
	if profile.CustomerCount != 25 || profile.EventsPerCustomer != 200 || profile.SubmitInterval != 10*time.Millisecond || profile.WorkerCount != 64 {
		t.Fatalf("profile overrides not applied: %+v", profile)
	}
}

func TestScenario2RetryPolicySurvivesPartnerCOutage(t *testing.T) {
	t.Parallel()

	policy := scenario2RetryPolicy()
	for i := range 100 {
		key := fmt.Sprintf("delivery-%03d", i)
		elapsed, attempt, prevDelay := time.Duration(0), 1, time.Duration(0)
		for elapsed < scenario2OutageDuration {
			resp := response(http.StatusServiceUnavailable)
			delay, terminal, reason := policy.NextWithKey(attempt, prevDelay, resp, nil, key)
			_ = resp.Body.Close()
			if terminal {
				t.Fatalf("%s exhausted before recovery at attempt %d after %s: %s", key, attempt, elapsed, reason)
			}
			elapsed += delay
			prevDelay = delay
			attempt++
		}

		resp := response(http.StatusOK)
		_, terminal, reason := policy.NextWithKey(attempt, prevDelay, resp, nil, key)
		_ = resp.Body.Close()
		if !terminal || reason != retry.ReasonSucceeded {
			t.Fatalf("%s did not succeed after recovery: terminal=%v reason=%q", key, terminal, reason)
		}
	}
}

func TestScenario2RetryPolicyTerminalsWithinBenchTimeout(t *testing.T) {
	t.Parallel()

	policy := scenario2RetryPolicy()
	for i := range 100 {
		key := fmt.Sprintf("delivery-%03d", i)
		elapsed, prevDelay := time.Duration(0), time.Duration(0)
		for attempt := 1; ; attempt++ {
			resp := response(http.StatusInternalServerError)
			delay, terminal, _ := policy.NextWithKey(attempt, prevDelay, resp, nil, key)
			_ = resp.Body.Close()
			if terminal {
				if elapsed > 5*time.Minute {
					t.Fatalf("%s exhausted after %s, want within bench timeout", key, elapsed)
				}
				break
			}
			elapsed += delay
			prevDelay = delay
		}
	}
}

func response(statusCode int) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
	}
}
