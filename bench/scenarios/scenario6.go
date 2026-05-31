package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
	"github.com/redis/go-redis/v9"
)

const (
	scenario6CustomerID = "scenario-6-customer"
	scenario6ReportPath = "bench/results/scenario-6.md"
	scenario6Route      = "/hooks/scenario-6"
	scenario6RedisAddr  = "localhost:6379"
)

type scenario6Profile struct {
	Name                    string
	WorkerCounts            []int
	ThroughputDeliveries    int
	RetryDeliveries         int
	BreakerFailureThreshold int
}

func scenario6ProfileFromEnv() (scenario6Profile, error) {
	raw, name, err := loadBenchProfileConfig("scenario6")
	if err != nil {
		return scenario6Profile{}, err
	}
	if len(raw.WorkerCounts) == 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q is missing worker counts", name)
	}
	if raw.ThroughputDeliveries < 1 || raw.RetryDeliveries < 1 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q has invalid positive counts", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q has invalid breaker threshold", name)
	}
	return scenario6Profile{
		Name:                    name,
		WorkerCounts:            raw.WorkerCounts,
		ThroughputDeliveries:    raw.ThroughputDeliveries,
		RetryDeliveries:         raw.RetryDeliveries,
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
	}, nil
}

type scenario6ThroughputResult struct {
	WorkerCount     int
	Duration        time.Duration
	Throughput      float64
	Received        int
	DuplicateTuples int
}

type scenario6RetryProbeResult struct {
	Deliveries                int
	Received                  int
	RetriedDeliveries         int
	DuplicateTuples           int
	DeliveryIDStable          bool
	DeliveryIDMatchesTuple    bool
	EventIDStable             bool
	EventIDPresent            bool
	AttemptNumbersUnique      bool
	AttemptNumbersConsecutive bool
}

// RunScenario6 runs the Redis Streams benchmark: throughput scaling at
// [1,4,8,32,64] workers plus a retry probe validating idempotency headers.
func RunScenario6(ctx context.Context) error {
	rdb := redis.NewClient(&redis.Options{
		Addr: scenario6RedisAddr,
		DB:   0,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	slog.Info("redis connected", "addr", scenario6RedisAddr)

	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := scenario6ProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	throughput := make([]scenario6ThroughputResult, 0, len(profile.WorkerCounts))
	for _, count := range profile.WorkerCounts {
		slog.Info("running scenario 6 throughput workload", "worker_count", count)
		res, err := runScenario6Throughput(ctx, rdb, db, count, profile)
		if err != nil {
			return fmt.Errorf("run throughput with %d workers: %w", count, err)
		}
		throughput = append(throughput, res)
	}

	slog.Info("running scenario 6 retry probe")
	retryProbe, err := runScenario6RetryProbe(ctx, rdb, db, profile)
	if err != nil {
		return fmt.Errorf("run retry probe: %w", err)
	}

	if err := writeScenario6Report(scenario6ReportPath, profile.Name, profile.ThroughputDeliveries, profile.RetryDeliveries, throughput, retryProbe); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("scenario 6 complete", "profile", profile.Name, "report", scenario6ReportPath)
	return nil
}

func runScenario6Throughput(ctx context.Context, rdb *redis.Client, db *sql.DB, numWorkers int, profile scenario6Profile) (scenario6ThroughputResult, error) {
	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("scenario-6-%d", numWorkers)),
		receiver.WithRouteLatency(scenario6Route, 1.0, 10*time.Millisecond),
	)
	defer rs.Close()

	if err := prepareScenario6Workload(ctx, rdb, db, rs.URL(), profile.ThroughputDeliveries); err != nil {
		return scenario6ThroughputResult{}, err
	}

	duration, err := drainScenario6(ctx, rdb, db, numWorkers, profile, retry.New(retry.Config{}))
	if err != nil {
		return scenario6ThroughputResult{}, err
	}

	recordings := rs.Recorded()
	duplicates := countDuplicateAttemptTuples(recordings)

	return scenario6ThroughputResult{
		WorkerCount:     numWorkers,
		Duration:        duration,
		Throughput:      float64(profile.ThroughputDeliveries) / duration.Seconds(),
		Received:        len(recordings),
		DuplicateTuples: duplicates,
	}, nil
}

func runScenario6RetryProbe(ctx context.Context, rdb *redis.Client, db *sql.DB, profile scenario6Profile) (scenario6RetryProbeResult, error) {
	rs := receiver.NewServer(
		receiver.WithStableDecisions("scenario-6-retry-probe"),
		receiver.WithRouteStatusFunc(scenario6Route, func(r *http.Request, _ time.Time) int {
			if r.Header.Get("X-Attempt-Number") == "1" {
				return http.StatusServiceUnavailable
			}
			return 0
		}),
	)
	defer rs.Close()

	if err := prepareScenario6Workload(ctx, rdb, db, rs.URL(), profile.RetryDeliveries); err != nil {
		return scenario6RetryProbeResult{}, err
	}

	policy := retry.New(retry.Config{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		CapDelay:    10 * time.Millisecond,
	})
	if _, err := drainScenario6(ctx, rdb, db, profile.WorkerCounts[len(profile.WorkerCounts)-1], profile, policy); err != nil {
		return scenario6RetryProbeResult{}, err
	}

	return analyzeScenario6RetryProbe(rs.Recorded()), nil
}

func prepareScenario6Workload(ctx context.Context, rdb *redis.Client, db *sql.DB, receiverBaseURL string, deliveries int) error {
	if err := resetBenchData(ctx, db); err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	if err := seedScenario6Customer(ctx, db); err != nil {
		return fmt.Errorf("seed customer: %w", err)
	}
	webhookID, err := seedScenario6Webhook(ctx, db, receiverBaseURL)
	if err != nil {
		return fmt.Errorf("seed webhook: %w", err)
	}
	if err := seedScenario6Deliveries(ctx, rdb, db, webhookID, deliveries); err != nil {
		return fmt.Errorf("seed deliveries: %w", err)
	}
	return nil
}

func drainScenario6(ctx context.Context, rdb *redis.Client, db *sql.DB, numWorkers int, profile scenario6Profile, policy retry.RetryPolicy) (time.Duration, error) {
	plainSched := roundrobin.New()

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			ws := outbox.NewRedisStore(rdb, db,
				outbox.WithRedisConsumerID(fmt.Sprintf("bench-worker-%d", id)),
			)
			w := worker.New(
				plainSched,
				worker.WithRetryPolicy(policy),
				worker.WithLogger(workerLog),
			)
			if err := w.Run(workerCtx, ws); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}

	discoveryStore := outbox.NewRedisStore(rdb, db, outbox.WithRedisConsumerID("bench-discovery"))
	discoveryStore.RunRetryScheduler(workerCtx)

	started := time.Now()
	plainSched.Notify(scenario6CustomerID)

	pollCtx, cancelPoll := context.WithTimeout(ctx, 10*time.Minute)
	defer cancelPoll()
	for {
		pending, err := countScenario6Pending(ctx, rdb, db)
		if err != nil {
			return 0, fmt.Errorf("poll pending: %w", err)
		}
		if pending == 0 {
			break
		}
		hasPending, err := redisHasClaimableWork(ctx, rdb, scenario6CustomerID)
		if err != nil {
			return 0, fmt.Errorf("check pending: %w", err)
		}
		if hasPending {
			plainSched.Notify(scenario6CustomerID)
		}
		select {
		case <-pollCtx.Done():
			return 0, fmt.Errorf("deliveries not drained within timeout: %d pending", pending)
		case <-time.After(100 * time.Millisecond):
		}
	}
	duration := time.Since(started)

	cancelWorkers()
	workerWg.Wait()

	return duration, nil
}

func countScenario6Pending(ctx context.Context, rdb *redis.Client, db *sql.DB) (int, error) {
	streamKey := "deliveries:" + scenario6CustomerID
	info, err := rdb.XPending(ctx, streamKey, "notifier").Result()
	if err != nil {
		return 0, nil
	}
	len, err := rdb.XLen(ctx, streamKey).Result()
	if err != nil {
		return 0, nil
	}
	unread := int(len) - int(info.Count)
	if unread < 0 {
		unread = 0
	}
	return unread + int(info.Count), nil
}

func redisHasClaimableWork(ctx context.Context, rdb *redis.Client, customerID string) (bool, error) {
	streamKey := "deliveries:" + customerID
	info, err := rdb.XPending(ctx, streamKey, "notifier").Result()
	if err != nil {
		return false, nil
	}
	len, err := rdb.XLen(ctx, streamKey).Result()
	if err != nil {
		return false, nil
	}
	return len > info.Count, nil
}

func seedScenario6Customer(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO customers (id, class)
		VALUES ($1, 1)
		ON CONFLICT (id) DO NOTHING
	`, scenario6CustomerID)
	if err != nil {
		return fmt.Errorf("insert customer: %w", err)
	}
	return nil
}

func seedScenario6Webhook(ctx context.Context, db *sql.DB, receiverBaseURL string) (string, error) {
	var webhookID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
		VALUES ($1, $2, '', $3, true)
		RETURNING id
	`, scenario6CustomerID, receiverBaseURL+scenario6Route, "{"+eventType+"}").Scan(&webhookID)
	if err != nil {
		return "", fmt.Errorf("insert webhook: %w", err)
	}
	return webhookID, nil
}

func seedScenario6Deliveries(ctx context.Context, rdb *redis.Client, db *sql.DB, webhookID string, deliveries int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var eventID string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO events (customer_id, type, payload)
		VALUES ($1, $2, $3)
		RETURNING id
	`, scenario6CustomerID, eventType, []byte(`{"event_name":"bench.scenario6"}`)).Scan(&eventID)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	streamKey := "deliveries:" + scenario6CustomerID
	if err := rdb.XGroupCreateMkStream(ctx, streamKey, "notifier", "0").Err(); err != nil && !isBusyGroup(err) {
		return fmt.Errorf("create group: %w", err)
	}

	now := time.Now()
	nowMs := now.UnixMilli()

	pipe := rdb.Pipeline()
	for i := 0; i < deliveries; i++ {
		deliveryID := outbox.NewRedisDeliveryID()
		createdAt := nowMs

		pipe.HSet(ctx, "delivery:"+deliveryID, map[string]interface{}{
			"id":                deliveryID,
			"event_id":          eventID,
			"webhook_id":        webhookID,
			"customer_id":       scenario6CustomerID,
			"customer_class":    "1",
			"event_type":        "webhook",
			"payload":           `{"event_name":"bench.scenario6"}`,
			"url":               "",
			"secret":            "",
			"created_at":        strconv.FormatInt(createdAt, 10),
			"attempt_count":     "0",
			"previous_delay_ms": "0",
			"last_error":        "",
			"delay_ms":          "0",
		})

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			ID:     deliveryID,
			Values: map[string]interface{}{
				"id":                deliveryID,
				"event_id":          eventID,
				"webhook_id":        webhookID,
				"customer_id":       scenario6CustomerID,
				"customer_class":    "1",
				"event_type":        "webhook",
				"payload":           `{"event_name":"bench.scenario6"}`,
				"url":               "",
				"secret":            "",
				"created_at":        strconv.FormatInt(createdAt, 10),
				"attempt_count":     "0",
				"previous_delay_ms": "0",
				"last_error":        "",
			},
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pipeline seed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit seed: %w", err)
	}
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}

func analyzeScenario6RetryProbe(recordings []receiver.Recording) scenario6RetryProbeResult {
	byDelivery := make(map[string][]receiver.Recording)
	for _, r := range recordings {
		deliveryID := r.Headers.Get("X-Delivery-Id")
		byDelivery[deliveryID] = append(byDelivery[deliveryID], r)
	}

	result := scenario6RetryProbeResult{
		Deliveries:                len(byDelivery),
		Received:                  len(recordings),
		DuplicateTuples:           countDuplicateAttemptTuples(recordings),
		DeliveryIDStable:          true,
		DeliveryIDMatchesTuple:    true,
		EventIDStable:             true,
		EventIDPresent:            true,
		AttemptNumbersUnique:      true,
		AttemptNumbersConsecutive: true,
	}

	for deliveryID, attempts := range byDelivery {
		if len(attempts) > 1 {
			result.RetriedDeliveries++
		}

		eventID := attempts[0].Headers.Get("X-Event-Id")
		if eventID == "" {
			result.EventIDPresent = false
		}

		numbers := make([]int, 0, len(attempts))
		seenNumbers := make(map[int]bool, len(attempts))
		for _, attempt := range attempts {
			if got := attempt.Headers.Get("X-Delivery-Id"); got != deliveryID {
				result.DeliveryIDStable = false
			}
			if got := attempt.Headers.Get("X-Delivery-Id"); got == "" {
				result.DeliveryIDMatchesTuple = false
			}
			if got := attempt.Headers.Get("X-Event-Id"); got != eventID {
				result.EventIDStable = false
			}

			attemptNumber, err := strconv.Atoi(attempt.Headers.Get("X-Attempt-Number"))
			if err != nil || attemptNumber < 1 {
				result.AttemptNumbersConsecutive = false
				result.AttemptNumbersUnique = false
				continue
			}
			if seenNumbers[attemptNumber] {
				result.AttemptNumbersUnique = false
			}
			seenNumbers[attemptNumber] = true
			numbers = append(numbers, attemptNumber)
		}

		sort.Ints(numbers)
		for i, n := range numbers {
			if n != i+1 {
				result.AttemptNumbersConsecutive = false
				break
			}
		}
	}

	return result
}

func writeScenario6Report(path, profileName string, throughputDeliveries, retryDeliveries int, throughput []scenario6ThroughputResult, retryProbe scenario6RetryProbeResult) error {
	// #nosec G304 -- benchmark report writes to a fixed results path
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	t1 := throughput[0].Throughput
	t8 := throughput[len(throughput)-1].Throughput
	w1 := throughput[0].WorkerCount
	wN := throughput[len(throughput)-1].WorkerCount
	scalingOK := t8 >= 4*t1

	allZeroDuplicates := true
	for _, r := range throughput {
		if r.DuplicateTuples > 0 {
			allZeroDuplicates = false
			break
		}
	}

	retryProbeOK := retryProbe.RetriedDeliveries > 0 &&
		retryProbe.DuplicateTuples == 0 &&
		retryProbe.DeliveryIDStable &&
		retryProbe.DeliveryIDMatchesTuple &&
		retryProbe.EventIDStable &&
		retryProbe.EventIDPresent &&
		retryProbe.AttemptNumbersUnique &&
		retryProbe.AttemptNumbersConsecutive

	_, _ = fmt.Fprintf(f, `# Scenario 6: Redis Streams Horizontal Scale + Atomicity + Idempotency

## Description

Profile **%s**: pure-success throughput runs use the same %d Delivery workload at worker counts %v against a healthy receiver with 10ms per-Attempt latency. A separate retry probe forces %d Deliveries to fail once with 503 before succeeding, so idempotency headers are validated across multiple Attempts without polluting the throughput measurement with retry backoff.

Uses Redis Streams as the delivery queue instead of Postgres SKIP LOCKED.

## Performance Summary

| Profile | %s |
| Workers | Deliveries | Received Attempts | Duration | Throughput (Deliveries/sec) | Duplicate (delivery_id, attempt_number) tuples |
|---:|---:|---:|---:|---:|---:|
`, profileName, throughputDeliveries, scenario6WorkerCounts(throughput), retryDeliveries, profileName)
	for _, r := range throughput {
		_, _ = fmt.Fprintf(f, "| %d | %d | %d | %s | %.2f | %d |\n",
			r.WorkerCount,
			throughputDeliveries,
			r.Received,
			r.Duration.Round(time.Millisecond),
			r.Throughput,
			r.DuplicateTuples,
		)
	}

	_, _ = fmt.Fprintf(f, `
## Retry Probe

| Metric | Value |
|---|---:|
| Deliveries | %d |
| Received Attempts | %d |
| Deliveries with multiple Attempts | %d |
| Duplicate (delivery_id, attempt_number) tuples | %d |
| X-Delivery-Id stable per Delivery | %s |
| X-Delivery-Id present in tuple | %s |
| X-Event-Id stable per Delivery | %s |
| X-Event-Id present | %s |
| X-Attempt-Number unique per Delivery | %s |
| X-Attempt-Number consecutive from 1 | %s |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Scaling: %d workers vs %d workers | %.2fx | >= 4.00x | %s |
| Throughput duplicate tuples | %d | 0 | %s |
| Retry probe duplicate tuples | %d | 0 | %s |
| Retry probe idempotency header stability | %s | true | %s |
`,
		retryProbe.Deliveries,
		retryProbe.Received,
		retryProbe.RetriedDeliveries,
		retryProbe.DuplicateTuples,
		passFail(retryProbe.DeliveryIDStable),
		passFail(retryProbe.DeliveryIDMatchesTuple),
		passFail(retryProbe.EventIDStable),
		passFail(retryProbe.EventIDPresent),
		passFail(retryProbe.AttemptNumbersUnique),
		passFail(retryProbe.AttemptNumbersConsecutive),
		wN, w1,
		t8/t1,
		passFail(scalingOK),
		totalScenario6DuplicateTuples(throughput),
		passFail(allZeroDuplicates),
		retryProbe.DuplicateTuples,
		passFail(retryProbe.DuplicateTuples == 0),
		fmt.Sprint(retryProbeOK),
		passFail(retryProbeOK),
	)

	return f.Close()
}

func totalScenario6DuplicateTuples(results []scenario6ThroughputResult) int {
	total := 0
	for _, r := range results {
		total += r.DuplicateTuples
	}
	return total
}

func scenario6WorkerCounts(results []scenario6ThroughputResult) []int {
	counts := make([]int, 0, len(results))
	for _, r := range results {
		counts = append(counts, r.WorkerCount)
	}
	return counts
}
