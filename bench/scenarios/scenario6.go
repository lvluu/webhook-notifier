package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	scenario6WhaleCustomerID = "scenario6-whale"
	scenario6SmallPrefix     = "scenario6-small-"
	scenario6ReportPath      = "bench/results/scenario-6.md"
	scenario6SeedBatchRows   = 1000
)

type scenario6Profile struct {
	Name                    string
	WorkerCounts            []int
	SmallCount              int
	WhaleDeliveries         int
	SmallDeliveries         int
	SubmitInterval          time.Duration
	RequestTimeout          time.Duration
	MinBatchHint            int
	RetryMaxAttempts        int
	RetryBaseDelay          time.Duration
	RetryCapDelay           time.Duration
	FairnessRatioLimit      float64
	BreakerFailureThreshold int
}

type scenario6RunResult struct {
	Workers       int
	Duration      time.Duration
	Throughput    float64
	SmallP95Ms    float64
	WhaleP95Ms    float64
	DeadRatePct   float64
	RetryRatePct  float64
	AvgClaimBatch float64
	ClaimCalls    int64
	EmptyClaims   int64
	Total         int
	Duplicates    int
	ReliabilityOK bool
	FairnessOK    bool
}

func scenario6ProfileFromEnv() (scenario6Profile, error) {
	raw, name, err := loadBenchProfileConfig("scenario6")
	if err != nil {
		return scenario6Profile{}, err
	}
	if len(raw.WorkerCounts) == 0 || raw.SmallCount < 1 || raw.WhaleDeliveries < 1 || raw.SmallDeliveries < 1 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q has invalid counts", name)
	}
	if raw.SubmitInterval.Duration() < 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q has invalid submit interval", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q requires positive request_timeout", name)
	}
	if raw.MinBatchHint < 1 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q requires min_batch_hint >= 1", name)
	}
	if raw.RetryMaxAttempts < 1 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q requires retry_max_attempts >= 1", name)
	}
	if raw.RetryBaseDelay.Duration() < 0 || raw.RetryCapDelay.Duration() < 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q has invalid retry delays", name)
	}
	if raw.FairnessRatioLimit <= 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q requires fairness_ratio_limit > 0", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return scenario6Profile{}, fmt.Errorf("scenario6 profile %q has invalid breaker threshold", name)
	}
	return scenario6Profile{
		Name:                    name,
		WorkerCounts:            raw.WorkerCounts,
		SmallCount:              raw.SmallCount,
		WhaleDeliveries:         raw.WhaleDeliveries,
		SmallDeliveries:         raw.SmallDeliveries,
		SubmitInterval:          raw.SubmitInterval.Duration(),
		RequestTimeout:          raw.RequestTimeout.Duration(),
		MinBatchHint:            raw.MinBatchHint,
		RetryMaxAttempts:        raw.RetryMaxAttempts,
		RetryBaseDelay:          raw.RetryBaseDelay.Duration(),
		RetryCapDelay:           raw.RetryCapDelay.Duration(),
		FairnessRatioLimit:      raw.FairnessRatioLimit,
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
	}, nil
}

// RunScenario6 executes an assignment-aligned production-like benchmark:
// real ingest submissions, mixed partner latency/failures, whale+small load.
func RunScenario6(ctx context.Context) error {
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

	results := make([]scenario6RunResult, 0, len(profile.WorkerCounts))
	for _, workers := range profile.WorkerCounts {
		slog.Info("running scenario 6 production-like run", "workers", workers)
		run, err := runScenario6Once(ctx, db, profile, workers)
		if err != nil {
			return err
		}
		results = append(results, run)
	}

	if err := writeScenario6Report(scenario6ReportPath, profile, results); err != nil {
		return fmt.Errorf("write scenario6 report: %w", err)
	}
	slog.Info("scenario 6 complete", "profile", profile.Name, "report", scenario6ReportPath)
	return nil
}

func runScenario6Once(ctx context.Context, db *sql.DB, profile scenario6Profile, workers int) (scenario6RunResult, error) {
	// Keep DB pool proportional to worker concurrency during this benchmark so
	// higher worker counts are not artificially bottlenecked by a tiny client
	// pool, while staying below the default test Postgres max_connections.
	maxConns := min(max(workers*4, 16), 80)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	if err := resetBenchData(ctx, db); err != nil {
		return scenario6RunResult{}, fmt.Errorf("reset bench data: %w", err)
	}
	if err := seedScenario6Customers(ctx, db, profile.SmallCount); err != nil {
		return scenario6RunResult{}, err
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("scenario-6-%d", workers)),
		receiver.WithRouteLatency("/hooks/fast", 1.0, 15*time.Millisecond),
		receiver.WithRouteLatency("/hooks/slow", 0.25, 250*time.Millisecond),
		receiver.WithRouteStatus("/hooks/flaky", 0.03, http.StatusServiceUnavailable),
	)
	defer rs.Close()
	if err := seedScenario6Webhooks(ctx, db, rs.URL(), profile.SmallCount); err != nil {
		return scenario6RunResult{}, err
	}

	sched := roundrobin.New()
	ingestStore := ingest.New(db, registry.New(db), sched)
	outboxStore := newScenario6InstrumentedStore(outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: profile.BreakerFailureThreshold,
	})))

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var wg sync.WaitGroup
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := worker.New(
				sched,
				worker.WithLogger(workerLog),
				worker.WithRequestTimeout(profile.RequestTimeout),
				worker.WithMinBatchHint(profile.MinBatchHint),
				worker.WithMultiPickCount(32),
				worker.WithRetryPolicy(retry.New(retry.Config{
					MaxAttempts: profile.RetryMaxAttempts,
					BaseDelay:   profile.RetryBaseDelay,
					CapDelay:    profile.RetryCapDelay,
				})),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("scenario6 worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}

	start := time.Now()
	if err := submitScenario6Events(workerCtx, ingestStore, profile); err != nil {
		return scenario6RunResult{}, err
	}
	if err := waitScenario6Drain(ctx, db, outboxStore, sched); err != nil {
		return scenario6RunResult{}, err
	}
	duration := time.Since(start)
	cancelWorkers()
	wg.Wait()

	total, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return scenario6RunResult{}, fmt.Errorf("query total deliveries: %w", err)
	}
	dead, err := queryScenario6DeadDeliveries(ctx, db)
	if err != nil {
		return scenario6RunResult{}, err
	}
	attempts, err := queryScenario6Attempts(ctx, db)
	if err != nil {
		return scenario6RunResult{}, err
	}
	smallP95, err := queryScenario6FirstAttemptP95(ctx, db, scenario6SmallPrefix+"%")
	if err != nil {
		return scenario6RunResult{}, err
	}
	whaleP95, err := queryScenario6FirstAttemptP95(ctx, db, scenario6WhaleCustomerID)
	if err != nil {
		return scenario6RunResult{}, err
	}

	retryAttempts := attempts - total
	if retryAttempts < 0 {
		retryAttempts = 0
	}
	deadRate := 0.0
	retryRate := 0.0
	if total > 0 {
		deadRate = float64(dead) * 100 / float64(total)
	}
	if attempts > 0 {
		retryRate = float64(retryAttempts) * 100 / float64(attempts)
	}
	duplicates := countDuplicateAttemptTuples(rs.Recorded())
	throughput := float64(total) / duration.Seconds()
	claimStats := outboxStore.ClaimStats()

	return scenario6RunResult{
		Workers:       workers,
		Duration:      duration,
		Throughput:    throughput,
		SmallP95Ms:    smallP95,
		WhaleP95Ms:    whaleP95,
		DeadRatePct:   deadRate,
		RetryRatePct:  retryRate,
		AvgClaimBatch: claimStats.AvgBatch,
		ClaimCalls:    claimStats.Calls,
		EmptyClaims:   claimStats.Empty,
		Total:         total,
		Duplicates:    duplicates,
		ReliabilityOK: duplicates == 0 && deadRate <= 1.0,
		FairnessOK:    smallP95 <= whaleP95*profile.FairnessRatioLimit,
	}, nil
}

type scenario6InstrumentedStore struct {
	*outbox.Store
	claimCalls  atomic.Int64
	claimRows   atomic.Int64
	emptyClaims atomic.Int64
}

type scenario6ClaimStats struct {
	Calls    int64
	Empty    int64
	AvgBatch float64
}

func newScenario6InstrumentedStore(store *outbox.Store) *scenario6InstrumentedStore {
	return &scenario6InstrumentedStore{Store: store}
}

func (s *scenario6InstrumentedStore) ClaimNextN(ctx context.Context, customerID string, limit int) ([]outbox.Delivery, error) {
	deliveries, err := s.Store.ClaimNextN(ctx, customerID, limit)
	s.claimCalls.Add(1)
	if err != nil {
		return nil, err
	}
	s.claimRows.Add(int64(len(deliveries)))
	if len(deliveries) == 0 {
		s.emptyClaims.Add(1)
	}
	return deliveries, nil
}

func (s *scenario6InstrumentedStore) ClaimNextBatch(ctx context.Context, customerIDs []string, limit int) ([]outbox.Delivery, error) {
	deliveries, err := s.Store.ClaimNextBatch(ctx, customerIDs, limit)
	s.claimCalls.Add(1)
	if err != nil {
		return nil, err
	}
	s.claimRows.Add(int64(len(deliveries)))
	if len(deliveries) == 0 {
		s.emptyClaims.Add(1)
	}
	return deliveries, nil
}

func (s *scenario6InstrumentedStore) HasClaimableWorkBatch(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	return s.Store.HasClaimableWorkBatch(ctx, customerIDs)
}

func (s *scenario6InstrumentedStore) ClaimStats() scenario6ClaimStats {
	calls := s.claimCalls.Load()
	avgBatch := 0.0
	if calls > 0 {
		avgBatch = float64(s.claimRows.Load()) / float64(calls)
	}
	return scenario6ClaimStats{
		Calls:    calls,
		Empty:    s.emptyClaims.Load(),
		AvgBatch: avgBatch,
	}
}

func submitScenario6Events(ctx context.Context, store *ingest.Store, profile scenario6Profile) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}
	submit := func(customerID string, count int) {
		defer wg.Done()
		for i := 0; i < count; i++ {
			if _, err := store.Submit(ctx, ingest.Event{
				CustomerID: customerID,
				Type:       eventType,
				Payload:    []byte(`{"event_name":"subscriber.created"}`),
			}); err != nil {
				reportErr(fmt.Errorf("submit event for %s: %w", customerID, err))
				return
			}
			if profile.SubmitInterval > 0 {
				select {
				case <-ctx.Done():
					reportErr(ctx.Err())
					return
				case <-time.After(profile.SubmitInterval):
				}
			}
		}
	}

	wg.Add(1)
	go submit(scenario6WhaleCustomerID, profile.WhaleDeliveries)
	for i := 0; i < profile.SmallCount; i++ {
		wg.Add(1)
		go submit(scenario6SmallCustomerID(i), profile.SmallDeliveries)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

type scenario6Notifier interface {
	Notify(customerID string)
}

func waitScenario6Drain(ctx context.Context, db *sql.DB, store *scenario6InstrumentedStore, sched scenario6Notifier) error {
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	for {
		customers, err := store.DiscoverClaimableCustomers(pollCtx, "", 1000)
		if err != nil {
			return fmt.Errorf("discover claimable customers: %w", err)
		}
		for _, customerID := range customers {
			sched.Notify(customerID)
		}

		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return fmt.Errorf("poll pending deliveries: %w", err)
		}
		if pending == 0 {
			return nil
		}
		select {
		case <-pollCtx.Done():
			return fmt.Errorf("deliveries not drained within timeout: %d pending", pending)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func seedScenario6Customers(ctx context.Context, db *sql.DB, smallCount int) error {
	customerIDs := make([]string, 0, smallCount+1)
	customerIDs = append(customerIDs, scenario6WhaleCustomerID)
	for i := 0; i < smallCount; i++ {
		customerIDs = append(customerIDs, scenario6SmallCustomerID(i))
	}

	for start := 0; start < len(customerIDs); start += scenario6SeedBatchRows {
		end := min(start+scenario6SeedBatchRows, len(customerIDs))
		if err := seedScenario6CustomerBatch(ctx, db, customerIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func seedScenario6Webhooks(ctx context.Context, db *sql.DB, baseURL string, smallCount int) error {
	customerIDs := make([]string, 0, smallCount+1)
	customerIDs = append(customerIDs, scenario6WhaleCustomerID)
	for i := 0; i < smallCount; i++ {
		customerIDs = append(customerIDs, scenario6SmallCustomerID(i))
	}

	for start := 0; start < len(customerIDs); start += scenario6SeedBatchRows {
		end := min(start+scenario6SeedBatchRows, len(customerIDs))
		if err := seedScenario6WebhookBatch(ctx, db, baseURL, customerIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func scenario6SmallCustomerID(index int) string {
	return fmt.Sprintf("%s%05d", scenario6SmallPrefix, index)
}

func seedScenario6CustomerBatch(ctx context.Context, db *sql.DB, customerIDs []string) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, len(customerIDs))
	for i, customerID := range customerIDs {
		if i > 0 {
			sb.WriteString(", ")
		}
		_, _ = fmt.Fprintf(&sb, "($%d, 1)", i+1)
		args = append(args, customerID)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("seed scenario6 customers: %w", err)
	}
	return nil
}

func seedScenario6WebhookBatch(ctx context.Context, db *sql.DB, baseURL string, customerIDs []string) error {
	paths := []string{"/hooks/fast", "/hooks/slow", "/hooks/flaky"}
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, len(customerIDs)*len(paths)*4)
	row := 0
	for _, cid := range customerIDs {
		for _, path := range paths {
			if row > 0 {
				sb.WriteString(", ")
			}
			base := row*4 + 1
			_, _ = fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", base, base+1, base+2, base+3)
			args = append(args, cid, baseURL+path, "", "{"+eventType+"}")
			row++
		}
	}
	sb.WriteString(" ON CONFLICT DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("seed scenario6 webhooks: %w", err)
	}
	return nil
}

func queryScenario6DeadDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status = 'dead'`).Scan(&n)
	return n, err
}

func queryScenario6Attempts(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts`).Scan(&n)
	return n, err
}

func queryScenario6FirstAttemptP95(ctx context.Context, db *sql.DB, customerPattern string) (float64, error) {
	var p95 float64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(percentile_cont(0.95) WITHIN GROUP (
			ORDER BY EXTRACT(EPOCH FROM (da.sent_at - d.created_at)) * 1000
		), 0)
		FROM deliveries d
		JOIN delivery_attempts da ON da.delivery_id = d.id AND da.attempt_number = 1
		WHERE d.customer_id LIKE $1
	`, customerPattern).Scan(&p95)
	return p95, err
}

func writeScenario6Report(path string, profile scenario6Profile, results []scenario6RunResult) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path.
	if err != nil {
		return fmt.Errorf("create scenario6 report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Scenario 6: Assignment-Aligned Production-Like Workload

## Description

Profile **%s** executes real Event ingestion (`+"`POST /events`"+`) for one Whale Customer and %d Small Customers.
Each Customer has 3 Webhooks (`+"`/hooks/fast`"+`, `+"`/hooks/slow`"+`, `+"`/hooks/flaky`"+`) with mixed latency/failure behavior to approximate production HTTP variability.

Assignment mapping:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts %v.
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`+"`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`"+`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.
- Throughput in this scenario is end-to-end: total deliveries divided by wall time from ingest submission start to full drain.

## Results

| Workers | Total Deliveries | Duration | Throughput (Deliveries/sec) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | Duplicate (delivery_id, attempt_number) tuples | Small P95 First-Attempt Latency (ms) | Whale P95 First-Attempt Latency (ms) | Reliability | Fairness |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
`, profile.Name, profile.SmallCount, profile.WorkerCounts)
	for _, r := range results {
		_, _ = fmt.Fprintf(f, "| %d | %d | %s | %.2f | %.2f | %d | %d | %.2f%% | %.2f%% | %d | %.2f | %.2f | %s | %s |\n",
			r.Workers,
			r.Total,
			r.Duration.Round(time.Millisecond),
			r.Throughput,
			r.AvgClaimBatch,
			r.ClaimCalls,
			r.EmptyClaims,
			r.RetryRatePct,
			r.DeadRatePct,
			r.Duplicates,
			r.SmallP95Ms,
			r.WhaleP95Ms,
			passFail(r.ReliabilityOK),
			passFail(r.FairnessOK),
		)
	}
	return f.Close()
}
