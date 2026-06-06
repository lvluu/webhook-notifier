package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
)

func PassFail(v bool) string {
	if v {
		return "**PASS**"
	}
	return "**FAIL**"
}

func QueryTotalDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries`).Scan(&n)
	return n, err
}

func QueryTerminalDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status IN ('succeeded', 'dead')`).Scan(&n)
	return n, err
}

type DeliveryOutcome struct {
	Status string
	Count  int
}

func QueryDeliveryOutcomes(ctx context.Context, db *sql.DB) ([]DeliveryOutcome, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM deliveries
		GROUP BY status
		ORDER BY status
	`)
	if err != nil {
		return nil, fmt.Errorf("query outcomes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var outcomes []DeliveryOutcome
	for rows.Next() {
		var o DeliveryOutcome
		if err := rows.Scan(&o.Status, &o.Count); err != nil {
			return nil, fmt.Errorf("scan outcome: %w", err)
		}
		outcomes = append(outcomes, o)
	}
	return outcomes, rows.Err()
}

type RetryBucket struct {
	Attempts int
	Count    int
}

func QueryRetryDistribution(ctx context.Context, db *sql.DB) ([]RetryBucket, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT attempt_count, COUNT(*)
		FROM deliveries
		WHERE status IN ('succeeded', 'dead')
		GROUP BY attempt_count
		ORDER BY attempt_count
	`)
	if err != nil {
		return nil, fmt.Errorf("query retry distribution: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var buckets []RetryBucket
	for rows.Next() {
		var b RetryBucket
		if err := rows.Scan(&b.Attempts, &b.Count); err != nil {
			return nil, fmt.Errorf("scan retry bucket: %w", err)
		}
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}

func QueryLatencyPercentiles(ctx context.Context, db *sql.DB, customerFilter string) (p50, p95, p99, maxMs float64, err error) {
	var whereClause string
	var args []any
	switch {
	case customerFilter == "":
		whereClause = "status = 'succeeded'"
	case strings.Contains(customerFilter, "%"):
		whereClause = "status = 'succeeded' AND customer_id LIKE $1"
		args = append(args, customerFilter)
	default:
		whereClause = "status = 'succeeded' AND customer_id = $1"
		args = append(args, customerFilter)
	}

	query := fmt.Sprintf(`
		SELECT
			COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000), 0),
			COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000), 0),
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000), 0),
			COALESCE(MAX(EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000), 0)
		FROM deliveries
		WHERE %s
	`, whereClause)

	var p50f, p95f, p99f, maxf float64
	err = db.QueryRowContext(ctx, query, args...).Scan(&p50f, &p95f, &p99f, &maxf)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("query latency: %w", err)
	}

	return p50f, p95f, p99f, maxf, nil
}

func QueryAttemptDurationPercentiles(ctx context.Context, db *sql.DB) (p50, p95, p99, maxMs float64, err error) {
	err = db.QueryRowContext(ctx, `
		SELECT
			COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms), 0),
			COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms), 0),
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms), 0),
			COALESCE(MAX(duration_ms), 0)
		FROM delivery_attempts
	`).Scan(&p50, &p95, &p99, &maxMs)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("query attempt durations: %w", err)
	}
	return p50, p95, p99, maxMs, nil
}

type BreakerIsolationCustomerLatency struct {
	CustomerID string
	ControlP95 float64
	DeadP95    float64
	Ratio      float64
}

func BreakerIsolationBreakerPollingConfig() (time.Duration, time.Duration) {
	profileName, err := BenchProfileNameFromEnv()
	if err != nil {
		profileName = BenchProfileDev
	}

	totalTimeout := 5 * time.Minute
	if profileName != BenchProfileStress {
		totalTimeout = 90 * time.Second
	}

	cooldown := BreakerIsolationBreakerInitialCooldownFromProfile()
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

func BreakerIsolationBreakerInitialCooldownFromProfile() time.Duration {
	raw, _, err := LoadBenchProfileConfig("breaker_isolation")
	if err != nil {
		return 0
	}
	return raw.BreakerInitialCooldown.Duration()
}

type PartnerLatencyStats struct {
	P50Ms float64
	P95Ms float64
	P99Ms float64
	MaxMs float64
}

type PartnerRecoveryStats struct {
	Total     int
	Succeeded int
}

type RetryBucketStats struct {
	Total        int
	MaxPerSecond int
}

type AttemptTuple struct {
	DeliveryID    string
	AttemptNumber string
}

func CountDuplicateAttemptTuples(recordings []receiver.Recording) int {
	seen := make(map[AttemptTuple]bool, len(recordings))
	duplicates := 0
	for _, r := range recordings {
		t := AttemptTuple{
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
