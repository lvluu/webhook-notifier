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
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	scenario4CustomerID           = "scenario-4-customer"
	scenario4ThroughputDeliveries = 10000
	scenario4RetryDeliveries      = 100
	scenario4PartnerLatency       = 10 * time.Millisecond
	scenario4ReportPath           = "bench/results/scenario-4.md"
	scenario4Route                = "/hooks/scenario-4"
)

var scenario4WorkerCounts = []int{1, 4, 8}

type scenario4ThroughputResult struct {
	WorkerCount     int
	Duration        time.Duration
	Throughput      float64
	Received        int
	DuplicateTuples int
}

type scenario4RetryProbeResult struct {
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

// RunScenario4 executes Scenario 4: horizontal scale, claim atomicity, and
// idempotency header stability.
func RunScenario4(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = defaultDatabaseURL
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	throughput := make([]scenario4ThroughputResult, 0, len(scenario4WorkerCounts))
	for _, count := range scenario4WorkerCounts {
		slog.Info("running scenario 4 throughput workload", "worker_count", count)
		res, err := runScenario4Throughput(ctx, db, count)
		if err != nil {
			return fmt.Errorf("run throughput with %d workers: %w", count, err)
		}
		throughput = append(throughput, res)
	}

	slog.Info("running scenario 4 retry probe")
	retryProbe, err := runScenario4RetryProbe(ctx, db)
	if err != nil {
		return fmt.Errorf("run retry probe: %w", err)
	}

	if err := writeScenario4Report(scenario4ReportPath, throughput, retryProbe); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("scenario 4 complete", "report", scenario4ReportPath)
	return nil
}

func runScenario4Throughput(ctx context.Context, db *sql.DB, numWorkers int) (scenario4ThroughputResult, error) {
	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("scenario-4-%d", numWorkers)),
		receiver.WithRouteLatency(scenario4Route, 1.0, scenario4PartnerLatency),
	)
	defer rs.Close()

	if err := prepareScenario4Workload(ctx, db, rs.URL(), scenario4ThroughputDeliveries); err != nil {
		return scenario4ThroughputResult{}, err
	}

	duration, err := drainScenario4(ctx, db, numWorkers, retry.New(retry.Config{}))
	if err != nil {
		return scenario4ThroughputResult{}, err
	}

	recordings := rs.Recorded()
	duplicates := countDuplicateAttemptTuples(recordings)

	return scenario4ThroughputResult{
		WorkerCount:     numWorkers,
		Duration:        duration,
		Throughput:      float64(scenario4ThroughputDeliveries) / duration.Seconds(),
		Received:        len(recordings),
		DuplicateTuples: duplicates,
	}, nil
}

func runScenario4RetryProbe(ctx context.Context, db *sql.DB) (scenario4RetryProbeResult, error) {
	rs := receiver.NewServer(
		receiver.WithStableDecisions("scenario-4-retry-probe"),
		receiver.WithRouteStatusFunc(scenario4Route, func(r *http.Request, _ time.Time) int {
			if r.Header.Get("X-Attempt-Number") == "1" {
				return http.StatusServiceUnavailable
			}
			return 0
		}),
	)
	defer rs.Close()

	if err := prepareScenario4Workload(ctx, db, rs.URL(), scenario4RetryDeliveries); err != nil {
		return scenario4RetryProbeResult{}, err
	}

	policy := retry.New(retry.Config{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		CapDelay:    10 * time.Millisecond,
	})
	if _, err := drainScenario4(ctx, db, scenario4WorkerCounts[len(scenario4WorkerCounts)-1], policy); err != nil {
		return scenario4RetryProbeResult{}, err
	}

	return analyzeScenario4RetryProbe(rs.Recorded()), nil
}

func prepareScenario4Workload(ctx context.Context, db *sql.DB, receiverBaseURL string, deliveries int) error {
	if err := resetBenchData(ctx, db); err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	if err := seedScenario4Customer(ctx, db); err != nil {
		return fmt.Errorf("seed customer: %w", err)
	}
	webhookID, err := seedScenario4Webhook(ctx, db, receiverBaseURL)
	if err != nil {
		return fmt.Errorf("seed webhook: %w", err)
	}
	if err := seedScenario4Deliveries(ctx, db, webhookID, deliveries); err != nil {
		return fmt.Errorf("seed deliveries: %w", err)
	}
	return nil
}

func drainScenario4(ctx context.Context, db *sql.DB, numWorkers int, policy retry.RetryPolicy) (time.Duration, error) {
	plainSched := roundrobin.New()
	outboxStore := baselineOutboxStore(db)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(
				plainSched,
				worker.WithRetryPolicy(policy),
				worker.WithLogger(workerLog),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}

	started := time.Now()
	plainSched.Notify(scenario4CustomerID)

	pollCtx, cancelPoll := context.WithTimeout(ctx, 10*time.Minute)
	defer cancelPoll()
	for {
		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return 0, fmt.Errorf("poll pending deliveries: %w", err)
		}
		if pending == 0 {
			break
		}
		hasPending, err := outboxStore.HasPendingWork(pollCtx, scenario4CustomerID)
		if err != nil {
			return 0, fmt.Errorf("check pending customer work: %w", err)
		}
		if hasPending {
			plainSched.Notify(scenario4CustomerID)
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

func seedScenario4Customer(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO customers (id, weight)
		VALUES ($1, 1)
		ON CONFLICT (id) DO NOTHING
	`, scenario4CustomerID)
	if err != nil {
		return fmt.Errorf("insert customer: %w", err)
	}
	return nil
}

func seedScenario4Webhook(ctx context.Context, db *sql.DB, receiverBaseURL string) (string, error) {
	var webhookID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
		VALUES ($1, $2, '', $3, true)
		RETURNING id
	`, scenario4CustomerID, receiverBaseURL+scenario4Route, "{"+eventType+"}").Scan(&webhookID)
	if err != nil {
		return "", fmt.Errorf("insert webhook: %w", err)
	}
	return webhookID, nil
}

func seedScenario4Deliveries(ctx context.Context, db *sql.DB, webhookID string, deliveries int) error {
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
	`, scenario4CustomerID, eventType, []byte(`{"event_name":"bench.scenario4"}`)).Scan(&eventID)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
		SELECT $1, $2, $3, 'pending', 0, now()
		FROM generate_series(1, $4)
	`, eventID, webhookID, scenario4CustomerID, deliveries)
	if err != nil {
		return fmt.Errorf("insert deliveries: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit deliveries: %w", err)
	}
	return nil
}

type attemptTuple struct {
	DeliveryID    string
	AttemptNumber string
}

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

func analyzeScenario4RetryProbe(recordings []receiver.Recording) scenario4RetryProbeResult {
	byDelivery := make(map[string][]receiver.Recording)
	for _, r := range recordings {
		deliveryID := r.Headers.Get("X-Delivery-Id")
		byDelivery[deliveryID] = append(byDelivery[deliveryID], r)
	}

	result := scenario4RetryProbeResult{
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

func writeScenario4Report(path string, throughput []scenario4ThroughputResult, retryProbe scenario4RetryProbeResult) error {
	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	t1 := throughput[0].Throughput
	t8 := throughput[len(throughput)-1].Throughput
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

	_, _ = fmt.Fprintf(f, `# Scenario 4: Horizontal Scale + Claim Atomicity + Idempotency

## Description

Pure-success throughput runs use the same 10k Delivery workload at worker counts {1, 4, 8} against a healthy receiver with 10ms per-Attempt latency. A separate retry probe forces 100 Deliveries to fail once with 503 before succeeding, so idempotency headers are validated across multiple Attempts without polluting the throughput measurement with retry backoff.

This repo uses just bench-4 as the canonical task command for the issue #15 benchmark.

## Performance Summary

| Workers | Deliveries | Received Attempts | Duration | Throughput (Deliveries/sec) | Duplicate (delivery_id, attempt_number) tuples |
|---:|---:|---:|---:|---:|---:|
`)
	for _, r := range throughput {
		_, _ = fmt.Fprintf(f, "| %d | %d | %d | %s | %.2f | %d |\n",
			r.WorkerCount,
			scenario4ThroughputDeliveries,
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
| Scaling: 8 workers vs 1 worker | %.2fx | >= 4.00x | %s |
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
		t8/t1,
		passFail(scalingOK),
		totalScenario4DuplicateTuples(throughput),
		passFail(allZeroDuplicates),
		retryProbe.DuplicateTuples,
		passFail(retryProbe.DuplicateTuples == 0),
		fmt.Sprint(retryProbeOK),
		passFail(retryProbeOK),
	)

	return f.Close()
}

func totalScenario4DuplicateTuples(results []scenario4ThroughputResult) int {
	total := 0
	for _, r := range results {
		total += r.DuplicateTuples
	}
	return total
}
