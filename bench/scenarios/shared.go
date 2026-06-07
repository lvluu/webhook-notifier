package scenarios

import (
	"context"
	"database/sql"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
)

// passFail renders a benchmark gate result for markdown reports.
func passFail(v bool) string {
	if v {
		return "**PASS**"
	}
	return "**FAIL**"
}

// queryTotalDeliveries counts all live Deliveries in the benchmark database.
func queryTotalDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries`).Scan(&n)
	return n, err
}

// queryTerminalDeliveries counts succeeded and dead Deliveries.
func queryTerminalDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status IN ('succeeded', 'dead')`).Scan(&n)
	return n, err
}

// healthyCustomerLatency compares a healthy Customer against control.
type healthyCustomerLatency struct {
	CustomerID string
	ControlP95 float64
	DeadP95    float64
	Ratio      float64
}

// partnerProtectionPollingConfig returns timeout and poll interval for breaker checks.
func partnerProtectionPollingConfig() (time.Duration, time.Duration) {
	profileName, err := benchProfileNameFromEnv()
	if err != nil {
		profileName = benchProfileDev
	}

	totalTimeout := 5 * time.Minute
	if profileName != benchProfileStress {
		totalTimeout = 90 * time.Second
	}

	cooldown := partnerProtectionInitialCooldownFromProfile()
	pollInterval := 500 * time.Millisecond
	if cooldown > 0 {
		pollInterval = cooldown / 20
	}
	if pollInterval < 250*time.Millisecond {
		pollInterval = 250 * time.Millisecond
	}
	if pollInterval > 2*time.Second {
		pollInterval = 2 * time.Second
	}

	return totalTimeout, pollInterval
}

// partnerProtectionInitialCooldownFromProfile returns the configured initial cooldown.
func partnerProtectionInitialCooldownFromProfile() time.Duration {
	raw, _, err := loadBenchProfileConfig("partner_protection")
	if err != nil {
		return 0
	}
	return raw.BreakerInitialCooldown.Duration()
}

// partnerLatencyStats holds latency percentiles for a partner route.
type partnerLatencyStats struct {
	P50Ms float64
	P95Ms float64
	P99Ms float64
	MaxMs float64
}

// partnerRecoveryStats summarizes recovered retry Deliveries.
type partnerRecoveryStats struct {
	Total     int
	Succeeded int
}

// retryBucketStats summarizes retry attempts per time bucket.
type retryBucketStats struct {
	Total        int
	MaxPerSecond int
}

// attemptTuple identifies a delivery attempt as seen by the receiver fixture.
type attemptTuple struct {
	DeliveryID    string
	AttemptNumber string
}

// countDuplicateAttemptTuples counts duplicate receiver-observed attempt tuples.
func countDuplicateAttemptTuples(recordings []receiver.Recording) int {
	seen := make(map[attemptTuple]bool, len(recordings))
	duplicates := 0
	for _, r := range recordings {
		t := attemptTuple{
			DeliveryID:    r.Headers.Get("X-Delivery-Id"),
			AttemptNumber: r.Headers.Get("X-Attempt-Number"),
		}
		if seen[t] {
			duplicates++
		}
		seen[t] = true
	}
	return duplicates
}
