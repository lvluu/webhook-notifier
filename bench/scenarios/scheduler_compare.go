package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
	"github.com/lvluu/webhook-notifier/internal/scheduler/dbweights"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/scheduler/swrr"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

func cpuProfileDir() string {
	return os.Getenv("BENCH_CPUPROFILE_DIR")
}

const (
	schedCmpPrefix   = "scmp-customer-"
	schedCmpBatchRows = 1000
)

type schedCmpProfile struct {
	Name                  string
	WorkerCounts          []int
	Customers             int
	DeliveriesPerCustomer int
	RequestTimeout        time.Duration
	MinBatchHint          int
	MultiPickCount        int
	SWRRMultiPickValues   []int
	RetryMaxAttempts      int
	RetryBaseDelay        time.Duration
	RetryCapDelay         time.Duration
	BreakerFailureThreshold int
}

type schedCmpRunResult struct {
	Workers          int
	Scheduler        string // "round-robin" or "SWRR"
	Duration         time.Duration
	Throughput       float64
	Total            int
	DeadRatePct      float64
	RetryRatePct     float64
	AvgClaimBatch    float64
	ClaimCalls       int64
	EmptyClaims      int64
	P95LowWeightMs   float64
	P95HighWeightMs  float64
	Duplicates       int
	ReliabilityOK    bool
}

func schedCmpProfileFromEnv() (schedCmpProfile, error) {
	raw, name, err := loadBenchProfileConfig("scheduler_compare")
	if err != nil {
		return schedCmpProfile{}, err
	}
	if len(raw.WorkerCounts) == 0 {
		return schedCmpProfile{}, fmt.Errorf("scheduler_compare profile %q requires worker_counts", name)
	}
	if raw.Customers < 1 || raw.DeliveriesPerCustomer < 1 {
		return schedCmpProfile{}, fmt.Errorf("scheduler_compare profile %q has invalid customer/delivery counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return schedCmpProfile{}, fmt.Errorf("scheduler_compare profile %q requires positive request_timeout", name)
	}
	if raw.MinBatchHint < 1 {
		return schedCmpProfile{}, fmt.Errorf("scheduler_compare profile %q requires min_batch_hint >= 1", name)
	}
	if len(raw.SWRRMultiPickValues) == 0 {
		raw.SWRRMultiPickValues = []int{raw.MultiPickCount}
	}
	if raw.RetryMaxAttempts < 1 {
		return schedCmpProfile{}, fmt.Errorf("scheduler_compare profile %q requires retry_max_attempts >= 1", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return schedCmpProfile{}, fmt.Errorf("scheduler_compare profile %q has invalid breaker threshold", name)
	}
	return schedCmpProfile{
		Name:                  name,
		WorkerCounts:          raw.WorkerCounts,
		Customers:             raw.Customers,
		DeliveriesPerCustomer: raw.DeliveriesPerCustomer,
		RequestTimeout:        raw.RequestTimeout.Duration(),
		MinBatchHint:          raw.MinBatchHint,
		MultiPickCount:        raw.MultiPickCount,
		SWRRMultiPickValues:   raw.SWRRMultiPickValues,
		RetryMaxAttempts:      raw.RetryMaxAttempts,
		RetryBaseDelay:        raw.RetryBaseDelay.Duration(),
		RetryCapDelay:         raw.RetryCapDelay.Duration(),
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
	}, nil
}

func RunSchedulerCompare(ctx context.Context) error {
	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := schedCmpProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	estimateCap := len(profile.WorkerCounts) * (1 + len(profile.SWRRMultiPickValues))
	results := make([]schedCmpRunResult, 0, estimateCap)
	for _, workers := range profile.WorkerCounts {
		rrRun, err := runSchedCmpOnce(ctx, db, profile, workers, "round-robin", profile.MultiPickCount)
		if err != nil {
			return fmt.Errorf("workers=%d scheduler=%s: %w", workers, "round-robin", err)
		}
		results = append(results, rrRun)
		for _, mpc := range profile.SWRRMultiPickValues {
			schedLabel := fmt.Sprintf("SWRR (mpc=%d)", mpc)
			slog.Info("running scheduler comparison", "workers", workers, "scheduler", schedLabel)
			run, err := runSchedCmpOnce(ctx, db, profile, workers, schedLabel, mpc)
			if err != nil {
				return fmt.Errorf("workers=%d scheduler=%s: %w", workers, schedLabel, err)
			}
			results = append(results, run)
		}
	}

	reportPath := benchmarkReportPath("scheduler_compare", profile.Name)
	if err := writeSchedCmpReport(reportPath, profile, results); err != nil {
		return fmt.Errorf("write scheduler_compare report: %w", err)
	}
	slog.Info("scheduler_compare complete", "profile", profile.Name, "report", reportPath)
	return nil
}

func runSchedCmpOnce(ctx context.Context, db *sql.DB, profile schedCmpProfile, workers int, schedType string, multiPickCount int) (schedCmpRunResult, error) {
	maxConns := min(max(workers*4, 16), 80)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	profileDir := cpuProfileDir()
	var cpuFile *os.File
	if profileDir != "" {
		safeName := strings.ReplaceAll(schedType, " ", "_")
		safeName = strings.ReplaceAll(safeName, "=", "-")
		pprofPath := filepath.Join(profileDir, fmt.Sprintf("cpu_w%d_%s.pprof", workers, safeName))
		var err error
		cpuFile, err = os.Create(pprofPath)
		if err != nil {
			return schedCmpRunResult{}, fmt.Errorf("create cpu profile %s: %w", pprofPath, err)
		}
		defer cpuFile.Close()
	}

	if err := resetBenchData(ctx, db); err != nil {
		return schedCmpRunResult{}, fmt.Errorf("reset bench data: %w", err)
	}

	// Seed customers with mixed weight classes: 60% weight 1, 20% weight 3,
	// 20% weight 5. SWRR will favour higher-weight customers; round-robin
	// ignores weights entirely.
	if err := seedSchedCmpCustomers(ctx, db, profile.Customers); err != nil {
		return schedCmpRunResult{}, err
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("schedcmp-%d-%s", workers, schedType)),
		receiver.WithRouteLatency("/hooks/fast", 1.0, 15*time.Millisecond),
		receiver.WithRouteLatency("/hooks/slow", 0.25, 250*time.Millisecond),
		receiver.WithRouteStatus("/hooks/flaky", 0.03, http.StatusServiceUnavailable),
	)
	defer rs.Close()
	if err := seedSchedCmpWebhooks(ctx, db, rs.URL(), profile.Customers); err != nil {
		return schedCmpRunResult{}, err
	}

	var sched scheduler.Scheduler
	if schedType == "SWRR" {
		ws := dbweights.New(db)
		defer ws.Stop()
		sched = swrr.New(ws)
	} else {
		sched = roundrobin.New()
	}

	outboxStore := newSchedCmpInstrumentedStore(outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0)))

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
				worker.WithMultiPickCount(multiPickCount),
				worker.WithRetryPolicy(retry.New(retry.Config{
					MaxAttempts: profile.RetryMaxAttempts,
					BaseDelay:   profile.RetryBaseDelay,
					CapDelay:    profile.RetryCapDelay,
				})),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("scheduler_compare worker failed", "worker_id", id, "scheduler", schedType, "error", err)
			}
		}(i)
	}

	if cpuFile != nil {
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			return schedCmpRunResult{}, fmt.Errorf("start cpu profile: %w", err)
		}
	}

	start := time.Now()
	if err := bulkSeedSchedCmpDeliveries(ctx, db, profile); err != nil {
		return schedCmpRunResult{}, err
	}
	if _, err := db.ExecContext(ctx, "ANALYZE deliveries, webhooks, delivery_claims"); err != nil {
		return schedCmpRunResult{}, fmt.Errorf("analyze after seed: %w", err)
	}
	if err := waitSchedCmpDrain(ctx, db, outboxStore, sched); err != nil {
		return schedCmpRunResult{}, err
	}
	duration := time.Since(start)

	if cpuFile != nil {
		pprof.StopCPUProfile()
	}

	cancelWorkers()
	wg.Wait()

	total, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return schedCmpRunResult{}, fmt.Errorf("query total deliveries: %w", err)
	}
	dead, err := querySchedCmpDeadDeliveries(ctx, db)
	if err != nil {
		return schedCmpRunResult{}, err
	}
	attempts, err := querySchedCmpAttempts(ctx, db)
	if err != nil {
		return schedCmpRunResult{}, err
	}
	p95Low, err := querySchedCmpFirstAttemptP95ByClass(ctx, db, 1)
	if err != nil {
		return schedCmpRunResult{}, err
	}
	p95High, err := querySchedCmpFirstAttemptP95ByClass(ctx, db, 3)
	if err != nil {
		return schedCmpRunResult{}, err
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

	return schedCmpRunResult{
		Workers:       workers,
		Scheduler:        schedType,
		Duration:         duration,
		Throughput:       throughput,
		Total:            total,
		DeadRatePct:      deadRate,
		RetryRatePct:     retryRate,
		AvgClaimBatch:    claimStats.AvgBatch,
		ClaimCalls:       claimStats.Calls,
		EmptyClaims:      claimStats.Empty,
		P95LowWeightMs:   p95Low,
		P95HighWeightMs:  p95High,
		Duplicates:       duplicates,
		ReliabilityOK:    duplicates == 0 && deadRate <= 1.0,
	}, nil
}

type schedCmpInstrumentedStore struct {
	*outbox.Store
	claimCalls  atomic.Int64
	claimRows   atomic.Int64
	emptyClaims atomic.Int64
}

func newSchedCmpInstrumentedStore(store *outbox.Store) *schedCmpInstrumentedStore {
	return &schedCmpInstrumentedStore{Store: store}
}

func (s *schedCmpInstrumentedStore) ClaimNextN(ctx context.Context, customerID string, limit int) ([]outbox.Delivery, error) {
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

func (s *schedCmpInstrumentedStore) ClaimNextBatch(ctx context.Context, customerIDs []string, limit int) ([]outbox.Delivery, error) {
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

func (s *schedCmpInstrumentedStore) HasClaimableWorkBatch(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	return s.Store.HasClaimableWorkBatch(ctx, customerIDs)
}

func (s *schedCmpInstrumentedStore) ClaimStats() fairnessClaimStats {
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

func bulkSeedSchedCmpDeliveries(ctx context.Context, db *sql.DB, profile schedCmpProfile) error {
	customerIDs := make([]string, profile.Customers)
	for i := range customerIDs {
		customerIDs[i] = schedCmpCustomerID(i)
	}
	for start := 0; start < len(customerIDs); start += schedCmpBatchRows {
		end := min(start+schedCmpBatchRows, len(customerIDs))
		if err := seedSchedCmpDeliveryBatch(ctx, db, customerIDs[start:end], profile.DeliveriesPerCustomer); err != nil {
			return fmt.Errorf("seed backlog: %w", err)
		}
	}
	return nil
}

func seedSchedCmpDeliveryBatch(ctx context.Context, db *sql.DB, customerIDs []string, deliveriesPerCustomer int) error {
	if len(customerIDs) == 0 || deliveriesPerCustomer < 1 {
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
	`, customerIDs, eventType, deliveriesPerCustomer)
	if err != nil {
		return fmt.Errorf("seed scheduler_compare deliveries: %w", err)
	}
	return nil
}

type schedCmpNotifier interface {
	Notify(customerID string)
}

func waitSchedCmpDrain(ctx context.Context, db *sql.DB, store *schedCmpInstrumentedStore, sched schedCmpNotifier) error {
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

// seedSchedCmpCustomers creates customers with mixed weights:
// 60% class=1, 20% class=3, 20% class=5 so SWRR has meaningful weights to schedule with.
func seedSchedCmpCustomers(ctx context.Context, db *sql.DB, count int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, count*2)
	for i := 0; i < count; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		class := 1
		switch {
		case i < count*60/100:
			class = 1
		case i < count*80/100:
			class = 3
		default:
			class = 5
		}
		base := i*2 + 1
		_, _ = fmt.Fprintf(&sb, "($%d, $%d)", base, base+1)
		args = append(args, schedCmpCustomerID(i), class)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	_, err := db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("seed scheduler_compare customers: %w", err)
	}
	return nil
}

func seedSchedCmpWebhooks(ctx context.Context, db *sql.DB, baseURL string, count int) error {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = schedCmpCustomerID(i)
	}
	for start := 0; start < len(ids); start += schedCmpBatchRows {
		end := min(start+schedCmpBatchRows, len(ids))
		if err := seedSchedCmpWebhookBatch(ctx, db, baseURL, ids[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func schedCmpCustomerID(index int) string {
	return fmt.Sprintf("%s%05d", schedCmpPrefix, index)
}

func seedSchedCmpWebhookBatch(ctx context.Context, db *sql.DB, baseURL string, customerIDs []string) error {
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
		return fmt.Errorf("seed scheduler_compare webhooks: %w", err)
	}
	return nil
}

func querySchedCmpDeadDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status = 'dead'`).Scan(&n)
	return n, err
}

func querySchedCmpAttempts(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts`).Scan(&n)
	return n, err
}

func querySchedCmpFirstAttemptP95ByClass(ctx context.Context, db *sql.DB, class int) (float64, error) {
	var p95 float64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(percentile_cont(0.95) WITHIN GROUP (
			ORDER BY EXTRACT(EPOCH FROM (da.sent_at - d.created_at)) * 1000
		), 0)
		FROM deliveries d
		JOIN delivery_attempts da ON da.delivery_id = d.id AND da.attempt_number = 1
		JOIN customers c ON c.id = d.customer_id
		WHERE c.class = $1
	`, class).Scan(&p95)
	return p95, err
}

func writeSchedCmpReport(path string, profile schedCmpProfile, results []schedCmpRunResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create scheduler_compare report: %w", err)
	}
	defer func() { _ = f.Close() }()

	mpcList := ""
	for i, v := range profile.SWRRMultiPickValues {
		if i > 0 {
			mpcList += ", "
		}
		mpcList += fmt.Sprint(v)
	}
	_, _ = fmt.Fprintf(f, `# Scheduler Comparison: Round-Robin vs SWRR

## Description

Profile **%s** compares the plain round-robin scheduler (O(1) Pick) against the
Smooth Weighted Round Robin (SWRR) scheduler (O(n) Pick, n = active Customers)
under identical workload and configuration.

Round-robin uses `+"`MultiPickCount=%d`"+`. SWRR is tested at multiple
`+"`MultiPickCount`"+` values (%s) to find whether smaller batches reduce the
O(n) per-Pick overhead at high concurrency.

Customers are seeded with mixed weight classes: 60%% weight 1 (default), 20%% weight 3,
20%% weight 5. Round-robin ignores weights entirely so all customers get equal scheduling
attention. SWRR favours higher-weight customers proportionally.

The cost comparison matters because SWRR's Pick() traverses every active Customer
on each call, while round-robin advances a cursor in O(1). Under high concurrency
with many active Customers, the O(n) scan may offset SWRR's scheduling benefits.

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | In-process worker goroutines sharing one Postgres database and one process-local Scheduler |
| Scheduler | Round-robin (O(1)) vs SWRR with DB weights (O(n)) — one run per scheduler per worker count |
| SWRR MultiPickCounts tested | %s |
| Ingest | Bypassed for load generation; Events and Deliveries are set-based SQL |
| Discovery | Manual claimable-Customer DB poll every 200ms; no PgNotify path |
| Claim batching | `+"`MinBatchHint=%d`"+` |
| Circuit breaker | Disabled (`+"`breaker_failure_threshold=%d`"+`) |
| HostLimiter | Disabled |
| Retry | Max attempts %d, base delay %s, cap delay %s |
| HTTP | Default shared Worker client, request timeout %s |

## Results

| Workers | Scheduler | Total Deliveries | Duration | Throughput (del/s) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | P95 Latency class=1 (ms) | P95 Latency class>=3 (ms) | Class Ratio (low/high) | Duplicates | Reliability |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
`, profile.Name,
		profile.MultiPickCount, mpcList,
		mpcList,
		profile.MinBatchHint,
		profile.BreakerFailureThreshold,
		profile.RetryMaxAttempts, profile.RetryBaseDelay, profile.RetryCapDelay,
		profile.RequestTimeout)

	for _, r := range results {
		ratio := 1.0
		if r.P95HighWeightMs > 0 {
			ratio = r.P95LowWeightMs / r.P95HighWeightMs
		}
		_, _ = fmt.Fprintf(f, "| %d | %s | %d | %s | %.2f | %.2f | %d | %d | %.2f%% | %.2f%% | %.2f | %.2f | %.2f | %d | %s |\n",
			r.Workers,
			r.Scheduler,
			r.Total,
			r.Duration.Round(time.Millisecond),
			r.Throughput,
			r.AvgClaimBatch,
			r.ClaimCalls,
			r.EmptyClaims,
			r.RetryRatePct,
			r.DeadRatePct,
			r.P95LowWeightMs,
			r.P95HighWeightMs,
			ratio,
			r.Duplicates,
			passFail(r.ReliabilityOK),
		)
	}
	mpcValuesDesc := ""
	for i, v := range profile.SWRRMultiPickValues {
		if i > 0 {
			mpcValuesDesc += ", "
		}
		mpcValuesDesc += fmt.Sprint(v)
	}
	_, _ = fmt.Fprintf(f, `
## Key Takeaways

- **Throughput** is total deliveries / wall time from seed to full drain.
- **Class Ratio** = P95 latency for class=1 (low weight) / P95 for class>=3 (high weight).
  A ratio > 1 means low-weight customers wait longer — expected under SWRR, which
  intentionally favours high-weight customers. Under round-robin with equal shares,
  the ratio should be ~1 (no class bias, but all customers pay the empty-claim tax).
- **Round-robin Pick is O(1)** — cursor advance, no traversal.
- **SWRR Pick is O(n)** — scans every active Customer to find the winner with highest currentWeight.
- With uniform weights SWRR degenerates to plain round-robin but still pays the O(n) scan cost.
- At %d workers with %d Customers, SWRR's O(n) scan is bounded by the number of Customers
  with in-flight work, which is typically workers × multi_pick_count.
- **MultiPickCount tuning** tests %s for SWRR to see if smaller batches reduce
  the O(n) scan overhead. A smaller batch means fewer picks per scheduler traversal
  but more round trips — the trade-off is between scan cost and scheduling granularity.
- The key question: does SWRR's weight-aware scheduling (lower latency for high-class customers,
  fewer empty claims) offset its O(n) scan overhead vs O(1) round-robin?
`, profile.WorkerCounts[len(profile.WorkerCounts)-1], profile.Customers, mpcValuesDesc)
	return f.Close()
}
