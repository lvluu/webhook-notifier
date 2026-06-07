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
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	fairnesswhaleCustomerID = "pw-whale"
	fairnessSmallPrefix     = "pw-small-"
	fairnessReportPath      = "bench/results/fairness.md"
	fairnessSeedBatchRows   = 1000
)

type fairnessProfile struct {
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

type fairnessRunResult struct {
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

func fairnessProfileFromEnv() (fairnessProfile, error) {
	raw, name, err := loadBenchProfileConfig("fairness")
	if err != nil {
		return fairnessProfile{}, err
	}
	if len(raw.WorkerCounts) == 0 || raw.SmallCount < 1 || raw.WhaleDeliveries < 1 || raw.SmallDeliveries < 1 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q has invalid counts", name)
	}
	if raw.SubmitInterval.Duration() < 0 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q has invalid submit interval", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q requires positive request_timeout", name)
	}
	if raw.MinBatchHint < 1 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q requires min_batch_hint >= 1", name)
	}
	if raw.RetryMaxAttempts < 1 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q requires retry_max_attempts >= 1", name)
	}
	if raw.RetryBaseDelay.Duration() < 0 || raw.RetryCapDelay.Duration() < 0 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q has invalid retry delays", name)
	}
	if raw.FairnessRatioLimit <= 0 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q requires fairness_ratio_limit > 0", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return fairnessProfile{}, fmt.Errorf("fairness profile %q has invalid breaker threshold", name)
	}
	return fairnessProfile{
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

// RunFairness executes an assignment-aligned production-like benchmark:
// real ingest submissions, mixed partner latency/failures, whale+small load.
func RunFairness(ctx context.Context) error {
	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := fairnessProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	results := make([]fairnessRunResult, 0, len(profile.WorkerCounts))
	for _, workers := range profile.WorkerCounts {
		slog.Info("running fairness production-like run", "workers", workers)
		run, err := runFairnessOnce(ctx, db, profile, workers)
		if err != nil {
			return err
		}
		results = append(results, run)
	}

	if err := writeFairnessReport(fairnessReportPath, profile, results); err != nil {
		return fmt.Errorf("write fairness report: %w", err)
	}
	slog.Info("fairness complete", "profile", profile.Name, "report", fairnessReportPath)
	return nil
}

func runFairnessOnce(ctx context.Context, db *sql.DB, profile fairnessProfile, workers int) (fairnessRunResult, error) {
	// Keep DB pool proportional to worker concurrency during this benchmark so
	// higher worker counts are not artificially bottlenecked by a tiny client
	// pool, while staying below the default test Postgres max_connections.
	maxConns := min(max(workers*4, 16), 80)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	if err := resetBenchData(ctx, db); err != nil {
		return fairnessRunResult{}, fmt.Errorf("reset bench data: %w", err)
	}
	if err := seedFairnessCustomers(ctx, db, profile.SmallCount); err != nil {
		return fairnessRunResult{}, err
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("fairness-%d", workers)),
		receiver.WithRouteLatency("/hooks/fast", 1.0, 15*time.Millisecond),
		receiver.WithRouteLatency("/hooks/slow", 0.25, 250*time.Millisecond),
		receiver.WithRouteStatus("/hooks/flaky", 0.03, http.StatusServiceUnavailable),
	)
	defer rs.Close()
	if err := seedFairnessWebhooks(ctx, db, rs.URL(), profile.SmallCount); err != nil {
		return fairnessRunResult{}, err
	}

	sched := roundrobin.New()
	outboxStore := newFairnessInstrumentedStore(outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0)))

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
				slog.Error("fairness worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}

	start := time.Now()
	if err := bulkSeedFairnessDeliveries(ctx, db, profile); err != nil {
		return fairnessRunResult{}, err
	}
	// TRUNCATE resets pg_class.reltuples to zero. Without ANALYZE the planner
	// may skip index scans for the recursive CTE in DiscoverClaimableCustomers,
	// causing context deadline exceeded on stress profiles.
	if _, err := db.ExecContext(ctx, "ANALYZE deliveries, webhooks, delivery_claims"); err != nil {
		return fairnessRunResult{}, fmt.Errorf("analyze after seed: %w", err)
	}
	if err := waitFairnessDrain(ctx, db, outboxStore, sched); err != nil {
		return fairnessRunResult{}, err
	}
	duration := time.Since(start)
	cancelWorkers()
	wg.Wait()

	total, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return fairnessRunResult{}, fmt.Errorf("query total deliveries: %w", err)
	}
	dead, err := queryFairnessDeadDeliveries(ctx, db)
	if err != nil {
		return fairnessRunResult{}, err
	}
	attempts, err := queryFairnessAttempts(ctx, db)
	if err != nil {
		return fairnessRunResult{}, err
	}
	smallP95, err := queryFairnessFirstAttemptP95(ctx, db, fairnessSmallPrefix+"%")
	if err != nil {
		return fairnessRunResult{}, err
	}
	whaleP95, err := queryFairnessFirstAttemptP95(ctx, db, fairnesswhaleCustomerID)
	if err != nil {
		return fairnessRunResult{}, err
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

	return fairnessRunResult{
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

type fairnessInstrumentedStore struct {
	*outbox.Store
	claimCalls  atomic.Int64
	claimRows   atomic.Int64
	emptyClaims atomic.Int64
}

type fairnessClaimStats struct {
	Calls    int64
	Empty    int64
	AvgBatch float64
}

func newFairnessInstrumentedStore(store *outbox.Store) *fairnessInstrumentedStore {
	return &fairnessInstrumentedStore{Store: store}
}

func (s *fairnessInstrumentedStore) ClaimNextN(ctx context.Context, customerID string, limit int) ([]outbox.Delivery, error) {
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

func (s *fairnessInstrumentedStore) ClaimNextBatch(ctx context.Context, customerIDs []string, limit int) ([]outbox.Delivery, error) {
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

func (s *fairnessInstrumentedStore) HasClaimableWorkBatch(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	return s.Store.HasClaimableWorkBatch(ctx, customerIDs)
}

func (s *fairnessInstrumentedStore) ClaimStats() fairnessClaimStats {
	calls := s.claimCalls.Load()
	avgBatch := 0.0
	if calls > 0 {
		avgBatch = float64(s.claimRows.Load()) / float64(calls)
	}
	return fairnessClaimStats{
		Calls:    calls,
		Empty:    s.emptyClaims.Load(),
		AvgBatch: avgBatch,
	}
}

// bulkSeedFairnessDeliveries creates the whale and small-customer delivery
// backlog directly with set-based SQL instead of driving one ingest Submit
// goroutine per customer. The per-customer Submit fan-out (10k+ concurrent
// goroutines, each a unique registry key so the cache never warms) stampedes
// the shared DB pool on a single box and makes the run measure connection
// contention rather than delivery throughput — confirmed by db.Stats() showing
// ~17k connection waits with the failure point insensitive to pool size.
//
// Each seeded Event fans out to one Delivery per active matching Webhook (3 per
// Customer), exactly as ingest would, so total delivery counts and the
// created_at-based fairness metric are unchanged. The scheduler is fed by the
// discovery poller in waitFairnessDrain rather than ingest-side Notify.
func bulkSeedFairnessDeliveries(ctx context.Context, db *sql.DB, profile fairnessProfile) error {
	if err := seedFairnessDeliveryBatch(ctx, db, []string{fairnesswhaleCustomerID}, profile.WhaleDeliveries); err != nil {
		return fmt.Errorf("seed whale backlog: %w", err)
	}

	smallIDs := make([]string, 0, profile.SmallCount)
	for i := 0; i < profile.SmallCount; i++ {
		smallIDs = append(smallIDs, fairnesssmallCustomerID(i))
	}
	for start := 0; start < len(smallIDs); start += fairnessSeedBatchRows {
		end := min(start+fairnessSeedBatchRows, len(smallIDs))
		if err := seedFairnessDeliveryBatch(ctx, db, smallIDs[start:end], profile.SmallDeliveries); err != nil {
			return fmt.Errorf("seed small backlog: %w", err)
		}
	}
	return nil
}

// seedFairnessDeliveryBatch inserts eventsPerCustomer Events for each given
// Customer and one pending Delivery per active matching Webhook, mirroring the
// fan-out ingest.Submit would produce.
func seedFairnessDeliveryBatch(ctx context.Context, db *sql.DB, customerIDs []string, eventsPerCustomer int) error {
	if len(customerIDs) == 0 || eventsPerCustomer < 1 {
		return nil
	}
	_, err := db.ExecContext(ctx, `
		WITH ev AS (
			INSERT INTO events (customer_id, type, payload)
			SELECT c.cid, $2, '{"event_name":"subscriber.created"}'::jsonb
			FROM unnest($1::text[]) AS c(cid)
			CROSS JOIN generate_series(1, $3::int)
			RETURNING id, customer_id
		)
		INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
		SELECT ev.id, w.id, ev.customer_id, 'pending', 0, now()
		FROM ev
		JOIN webhooks w
		  ON w.customer_id = ev.customer_id
		 AND w.active = true
		 AND $2 = ANY(w.event_types)
	`, customerIDs, eventType, eventsPerCustomer)
	if err != nil {
		return fmt.Errorf("seed fairness deliveries: %w", err)
	}
	return nil
}

type fairnessNotifier interface {
	Notify(customerID string)
}

func waitFairnessDrain(ctx context.Context, db *sql.DB, store *fairnessInstrumentedStore, sched fairnessNotifier) error {
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

func seedFairnessCustomers(ctx context.Context, db *sql.DB, smallCount int) error {
	customerIDs := make([]string, 0, smallCount+1)
	customerIDs = append(customerIDs, fairnesswhaleCustomerID)
	for i := 0; i < smallCount; i++ {
		customerIDs = append(customerIDs, fairnesssmallCustomerID(i))
	}

	for start := 0; start < len(customerIDs); start += fairnessSeedBatchRows {
		end := min(start+fairnessSeedBatchRows, len(customerIDs))
		if err := seedFairnessCustomerBatch(ctx, db, customerIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func seedFairnessWebhooks(ctx context.Context, db *sql.DB, baseURL string, smallCount int) error {
	customerIDs := make([]string, 0, smallCount+1)
	customerIDs = append(customerIDs, fairnesswhaleCustomerID)
	for i := 0; i < smallCount; i++ {
		customerIDs = append(customerIDs, fairnesssmallCustomerID(i))
	}

	for start := 0; start < len(customerIDs); start += fairnessSeedBatchRows {
		end := min(start+fairnessSeedBatchRows, len(customerIDs))
		if err := seedFairnessWebhookBatch(ctx, db, baseURL, customerIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func fairnesssmallCustomerID(index int) string {
	return fmt.Sprintf("%s%05d", fairnessSmallPrefix, index)
}

func seedFairnessCustomerBatch(ctx context.Context, db *sql.DB, customerIDs []string) error {
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
		return fmt.Errorf("seed fairness customers: %w", err)
	}
	return nil
}

func seedFairnessWebhookBatch(ctx context.Context, db *sql.DB, baseURL string, customerIDs []string) error {
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
		return fmt.Errorf("seed fairness webhooks: %w", err)
	}
	return nil
}

func queryFairnessDeadDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status = 'dead'`).Scan(&n)
	return n, err
}

func queryFairnessAttempts(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts`).Scan(&n)
	return n, err
}

func queryFairnessFirstAttemptP95(ctx context.Context, db *sql.DB, customerPattern string) (float64, error) {
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

func writeFairnessReport(path string, profile fairnessProfile, results []fairnessRunResult) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path.
	if err != nil {
		return fmt.Errorf("create fairness report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Fairness

## Description

Profile **%s** seeds a Delivery backlog with set-based SQL (each Event fans out to one Delivery per matching Webhook, exactly as ingest would) for one Whale Customer and %d Small Customers, then measures the delivery pipeline draining it.
Seeding directly avoids a per-Customer ingest `+"`Submit`"+` stampede that would otherwise measure DB connection contention instead of delivery throughput; ingest-path performance is covered separately.
Each Customer has 3 Webhooks (`+"`/hooks/fast`"+`, `+"`/hooks/slow`"+`, `+"`/hooks/flaky`"+`) with mixed latency/failure behavior to approximate production HTTP variability.

Validation coverage:
- Reliability: retries under transient 503s, duplicate tuple protection.
- Scalability: throughput across worker counts %v.
- Fairness: Small-vs-Whale P95 first-attempt latency comparison under concurrent load.

Fairness metric note:
- Uses first-attempt queue latency (`+"`delivery_attempts.sent_at(attempt=1) - deliveries.created_at`"+`) at P95.
- Excludes retry/backoff tail so fairness reflects scheduler/queue behavior rather than partner instability.
- Throughput in this scenario is total deliveries divided by wall time from backlog seed start to full drain.

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
