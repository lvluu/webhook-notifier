package legacy

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/retry"

	scenarios "github.com/lvluu/webhook-notifier/bench/scenarios"
)

func TestTransientRecoveryProfileFromEnvDefaults(t *testing.T) {
	t.Setenv("BENCH_PROFILE", "")

	profile, err := transientRecoveryProfileFromEnv()
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	if profile.Name != "dev" {
		t.Fatalf("name: got %q, want dev", profile.Name)
	}
	want := mustBenchProfileConfig(t, "transient_recovery", "dev")
	if profile.WorkerCount != want.WorkerCount || profile.CustomerCount != want.CustomerCount || profile.EventsPerCustomer != want.EventsPerCustomer || profile.SubmitInterval != want.SubmitInterval.Duration() || profile.OutageDelay != want.OutageDelay.Duration() || profile.OutageDuration != want.OutageDuration.Duration() || profile.BreakerFailureThreshold != want.BreakerFailureThreshold {
		t.Fatalf("dev profile overrides not applied: %+v want %+v", profile, want)
	}
}

func TestTransientRecoveryProfileFromEnvOverrides(t *testing.T) {
	t.Setenv("BENCH_PROFILE", "stress")

	profile, err := transientRecoveryProfileFromEnv()
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	if profile.Name != "stress" {
		t.Fatalf("name: got %q, want stress", profile.Name)
	}
	want := mustBenchProfileConfig(t, "transient_recovery", "stress")
	if profile.WorkerCount != want.WorkerCount || profile.CustomerCount != want.CustomerCount || profile.EventsPerCustomer != want.EventsPerCustomer || profile.SubmitInterval != want.SubmitInterval.Duration() || profile.OutageDelay != want.OutageDelay.Duration() || profile.OutageDuration != want.OutageDuration.Duration() || profile.BreakerFailureThreshold != want.BreakerFailureThreshold {
		t.Fatalf("stress profile overrides not applied: %+v want %+v", profile, want)
	}
}

func TestTransientRecoveryRetryPolicySurvivesPartnerCOutage(t *testing.T) {
	t.Parallel()

	profile, err := transientRecoveryProfileFromEnv()
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	policy := transientRecoveryRetryPolicy()
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

func TestTransientRecoveryRetryPolicyTerminalsWithinBenchTimeout(t *testing.T) {
	t.Parallel()

	policy := transientRecoveryRetryPolicy()
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

func mustBenchProfileConfig(t *testing.T, scenario, profile string) scenarios.BenchProfileConfig {
	t.Helper()

	profiles, err := scenarios.ReadBenchProfiles()
	if err != nil {
		t.Fatalf("read bench profiles: %v", err)
	}
	scenarioProfiles, ok := profiles.Scenarios[scenario]
	if !ok {
		t.Fatalf("missing scenario %q in bench profiles", scenario)
	}
	got, ok := scenarioProfiles[profile]
	if !ok {
		t.Fatalf("missing profile %q for scenario %q", profile, scenario)
	}
	return got
}
