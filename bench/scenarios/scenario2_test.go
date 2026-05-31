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

	profile, err := scenario2ProfileFromEnv()
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	if profile.Name != "dev" {
		t.Fatalf("name: got %q, want dev", profile.Name)
	}
	if profile.CustomerCount != 5 {
		t.Fatalf("customers: got %d, want 5", profile.CustomerCount)
	}
	if profile.EventsPerCustomer != 20 {
		t.Fatalf("events/customer: got %d, want 20", profile.EventsPerCustomer)
	}
	if profile.SubmitInterval != 5*time.Millisecond {
		t.Fatalf("submit interval: got %s, want 5ms", profile.SubmitInterval)
	}
	if profile.WorkerCount != 16 || profile.OutageDelay != time.Second || profile.OutageDuration != 5*time.Second || profile.BreakerFailureThreshold != 0 {
		t.Fatalf("dev profile overrides not applied: %+v", profile)
	}
}

func TestScenario2ProfileFromEnvOverrides(t *testing.T) {
	t.Setenv("BENCH_PROFILE", "stress")

	profile, err := scenario2ProfileFromEnv()
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	if profile.Name != "stress" {
		t.Fatalf("name: got %q, want stress", profile.Name)
	}
	if profile.CustomerCount != 10 || profile.EventsPerCustomer != 100 || profile.SubmitInterval != 20*time.Millisecond || profile.WorkerCount != 64 || profile.OutageDelay != 5*time.Second || profile.OutageDuration != 30*time.Second || profile.BreakerFailureThreshold != 0 {
		t.Fatalf("stress profile overrides not applied: %+v", profile)
	}
}

func TestScenario2RetryPolicySurvivesPartnerCOutage(t *testing.T) {
	t.Parallel()

	profile, err := scenario2ProfileFromEnv()
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	policy := scenario2RetryPolicy()
	for i := range 100 {
		key := fmt.Sprintf("delivery-%03d", i)
		elapsed, attempt, prevDelay := time.Duration(0), 1, time.Duration(0)
		for elapsed < profile.OutageDuration {
			resp := response(http.StatusServiceUnavailable)
			delay, terminal, reason := policy.NextWithKey(attempt, prevDelay, resp, nil, key, elapsed)
			_ = resp.Body.Close()
			if terminal {
				t.Fatalf("%s exhausted before recovery at attempt %d after %s: %s", key, attempt, elapsed, reason)
			}
			elapsed += delay
			prevDelay = delay
			attempt++
		}

		resp := response(http.StatusOK)
		_, terminal, reason := policy.NextWithKey(attempt, prevDelay, resp, nil, key, elapsed)
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
			delay, terminal, _ := policy.NextWithKey(attempt, prevDelay, resp, nil, key, elapsed)
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
