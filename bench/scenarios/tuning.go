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
	tuningCustomerPrefix = "tuning-customer-"
	tuningSeedBatchRows  = 1000
)

type tuningConfig struct {
	Workers             int
	DeliveryConcurrency int
	MinBatchHint        int
	MaxBatchHint        int
}

func (c tuningConfig) Label() string {
	return fmt.Sprintf("w%d-dc%d-mn%d-mx%d", c.Workers, c.DeliveryConcurrency, c.MinBatchHint, c.MaxBatchHint)
}

type tuningProfile struct {
	Name                      string
	WorkerCounts              []int
	DeliveryConcurrencyValues []int
	MinBatchHintValues        []int
	MaxBatchHintValues        []int
	Customers                 int
	DeliveriesPerCustomer     int
	SubmitInterval            time.Duration
	RequestTimeout            time.Duration
	RetryMaxAttempts          int
	RetryBaseDelay            time.Duration
	RetryCapDelay             time.Duration
	BreakerFailureThreshold   int
}

type tuningRunResult struct {
	Config        tuningConfig
	Duration      time.Duration
	Throughput    float64
	Total         int
	DeadRatePct   float64
	RetryRatePct  float64
	AvgClaimBatch float64
	ClaimCalls    int64
	EmptyClaims   int64
	P95LatencyMs  float64
	Duplicates    int
	ReliabilityOK bool
	BucketBatches []float64
}

func tuningProfileFromEnv() (tuningProfile, error) {
	raw, name, err := loadBenchProfileConfig("tuning")
	if err != nil {
		return tuningProfile{}, err
	}
	if len(raw.WorkerCounts) == 0 || len(raw.DeliveryConcurrencyValues) == 0 ||
		len(raw.MinBatchHintValues) == 0 || len(raw.MaxBatchHintValues) == 0 {
		return tuningProfile{}, fmt.Errorf("tuning profile %q requires non-empty sweep arrays", name)
	}
	if raw.Customers < 1 || raw.DeliveriesPerCustomer < 1 {
		return tuningProfile{}, fmt.Errorf("tuning profile %q has invalid customer/delivery counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return tuningProfile{}, fmt.Errorf("tuning profile %q requires positive request_timeout", name)
	}
	if raw.RetryMaxAttempts < 1 {
		return tuningProfile{}, fmt.Errorf("tuning profile %q requires retry_max_attempts >= 1", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return tuningProfile{}, fmt.Errorf("tuning profile %q has invalid breaker threshold", name)
	}
	return tuningProfile{
		Name:                      name,
		WorkerCounts:              raw.WorkerCounts,
		DeliveryConcurrencyValues: raw.DeliveryConcurrencyValues,
		MinBatchHintValues:        raw.MinBatchHintValues,
		MaxBatchHintValues:        raw.MaxBatchHintValues,
		Customers:                 raw.Customers,
		DeliveriesPerCustomer:     raw.DeliveriesPerCustomer,
		SubmitInterval:            raw.SubmitInterval.Duration(),
		RequestTimeout:            raw.RequestTimeout.Duration(),
		RetryMaxAttempts:          raw.RetryMaxAttempts,
		RetryBaseDelay:            raw.RetryBaseDelay.Duration(),
		RetryCapDelay:             raw.RetryCapDelay.Duration(),
		BreakerFailureThreshold:   raw.BreakerFailureThreshold,
	}, nil
}

func tuningConfigs(profile tuningProfile) []tuningConfig {
	configs := make([]tuningConfig, 0, len(profile.WorkerCounts)*
		len(profile.DeliveryConcurrencyValues)*
		len(profile.MinBatchHintValues)*
		len(profile.MaxBatchHintValues))
	for _, w := range profile.WorkerCounts {
		for _, dc := range profile.DeliveryConcurrencyValues {
			for _, mn := range profile.MinBatchHintValues {
				for _, mx := range profile.MaxBatchHintValues {
					if mx > 0 && mx < mn {
						continue
					}
					configs = append(configs, tuningConfig{
						Workers:             w,
						DeliveryConcurrency: dc,
						MinBatchHint:        mn,
						MaxBatchHint:        mx,
					})
				}
			}
		}
	}
	return configs
}

// RunTuning executes a configuration tuning grid search: for each
// combination of Worker count, DeliveryConcurrency, MinBatchHint, and
// MaxBatchHint it seeds a Delivery backlog, drains it under load, and
// records throughput + latency to find the optimal configuration.
func RunTuning(ctx context.Context) error {
	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := tuningProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	configs := tuningConfigs(profile)
	slog.Info("tuning sweep", "config_combinations", len(configs), "profile", profile.Name)

	results := make([]tuningRunResult, 0, len(configs))
	for _, cfg := range configs {
		slog.Info("running tuning config", "config", cfg.Label())
		run, err := runTuningOnce(ctx, db, profile, cfg)
		if err != nil {
			return fmt.Errorf("config %s: %w", cfg.Label(), err)
		}
		results = append(results, run)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Throughput > results[j].Throughput
	})

	reportPath := benchmarkReportPath("tuning", profile.Name)
	if err := writeTuningReport(reportPath, profile, results); err != nil {
		return fmt.Errorf("write tuning report: %w", err)
	}
	slog.Info("tuning complete", "profile", profile.Name, "report", reportPath,
		"best_config", results[0].Config.Label(),
		"best_throughput", fmt.Sprintf("%.2f/s", results[0].Throughput))
	return nil
}

func runTuningOnce(ctx context.Context, db *sql.DB, profile tuningProfile, cfg tuningConfig) (tuningRunResult, error) {
	maxConns := min(max(cfg.Workers*4, 16), 80)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	if err := resetBenchData(ctx, db); err != nil {
		return tuningRunResult{}, fmt.Errorf("reset bench data: %w", err)
	}
	if err := seedTuningCustomers(ctx, db, profile.Customers); err != nil {
		return tuningRunResult{}, err
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(cfg.Label()),
		receiver.WithRouteLatency("/hooks/fast", 1.0, 15*time.Millisecond),
		receiver.WithRouteLatency("/hooks/slow", 0.25, 250*time.Millisecond),
		receiver.WithRouteStatus("/hooks/flaky", 0.03, http.StatusServiceUnavailable),
	)
	defer rs.Close()
	if err := seedTuningWebhooks(ctx, db, rs.URL(), profile.Customers); err != nil {
		return tuningRunResult{}, err
	}

	sched := roundrobin.New()
	totalDeliveries := int64(profile.Customers) * int64(profile.DeliveriesPerCustomer) * 3
	outboxStore := newTuningInstrumentedStore(outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0)), totalDeliveries)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var wg sync.WaitGroup
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := worker.New(
				sched,
				worker.WithLogger(workerLog),
				worker.WithRequestTimeout(profile.RequestTimeout),
				worker.WithMinBatchHint(cfg.MinBatchHint),
				worker.WithMaxBatchHint(cfg.MaxBatchHint),
				worker.WithDeliveryConcurrency(cfg.DeliveryConcurrency),
				worker.WithMultiPickCount(32),
				worker.WithRetryPolicy(retry.New(retry.Config{
					MaxAttempts: profile.RetryMaxAttempts,
					BaseDelay:   profile.RetryBaseDelay,
					CapDelay:    profile.RetryCapDelay,
				})),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("tuning worker failed", "worker_id", id, "config", cfg.Label(), "error", err)
			}
		}(i)
	}

	start := time.Now()
	if err := bulkSeedTuningDeliveries(ctx, db, profile); err != nil {
		return tuningRunResult{}, err
	}
	if _, err := db.ExecContext(ctx, "ANALYZE deliveries, webhooks, delivery_claims"); err != nil {
		return tuningRunResult{}, fmt.Errorf("analyze after seed: %w", err)
	}
	if err := waitTuningDrain(ctx, db, outboxStore, sched); err != nil {
		return tuningRunResult{}, err
	}
	duration := time.Since(start)
	cancelWorkers()
	wg.Wait()

	total, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return tuningRunResult{}, fmt.Errorf("query total deliveries: %w", err)
	}
	dead, err := queryTuningDeadDeliveries(ctx, db)
	if err != nil {
		return tuningRunResult{}, err
	}
	attempts, err := queryTuningAttempts(ctx, db)
	if err != nil {
		return tuningRunResult{}, err
	}
	p95, err := queryTuningFirstAttemptP95(ctx, db)
	if err != nil {
		return tuningRunResult{}, err
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
	bucketBatches := outboxStore.BucketBatches()

	return tuningRunResult{
		Config:        cfg,
		Duration:      duration,
		Throughput:    throughput,
		Total:         total,
		DeadRatePct:   deadRate,
		RetryRatePct:  retryRate,
		AvgClaimBatch: claimStats.AvgBatch,
		ClaimCalls:    claimStats.Calls,
		EmptyClaims:   claimStats.Empty,
		P95LatencyMs:  p95,
		Duplicates:    duplicates,
		ReliabilityOK: duplicates == 0 && deadRate <= 1.0,
		BucketBatches: bucketBatches,
	}, nil
}

const tuningNumBuckets = 10

type tuningBucket struct {
	Calls int64
	Rows  int64
}

type tuningInstrumentedStore struct {
	*outbox.Store
	claimCalls      atomic.Int64
	claimRows       atomic.Int64
	emptyClaims     atomic.Int64
	totalDeliveries int64
	mu              sync.Mutex
	buckets         [tuningNumBuckets]tuningBucket
}

func newTuningInstrumentedStore(store *outbox.Store, totalDeliveries int64) *tuningInstrumentedStore {
	return &tuningInstrumentedStore{
		Store:           store,
		totalDeliveries: totalDeliveries,
	}
}

func (s *tuningInstrumentedStore) recordClaim(rows int64) {
	if s.totalDeliveries <= 0 {
		return
	}
	claimed := s.claimRows.Load()
	idx := int(float64(claimed) / float64(s.totalDeliveries) * tuningNumBuckets)
	if idx >= tuningNumBuckets {
		idx = tuningNumBuckets - 1
	}
	if idx < 0 {
		idx = 0
	}
	s.mu.Lock()
	s.buckets[idx].Calls++
	s.buckets[idx].Rows += rows
	s.mu.Unlock()
}

func (s *tuningInstrumentedStore) BucketBatches() []float64 {
	batches := make([]float64, tuningNumBuckets)
	s.mu.Lock()
	for i := range s.buckets {
		if s.buckets[i].Calls > 0 {
			batches[i] = float64(s.buckets[i].Rows) / float64(s.buckets[i].Calls)
		}
	}
	s.mu.Unlock()
	return batches
}

func (s *tuningInstrumentedStore) ClaimNextN(ctx context.Context, customerID string, limit int) ([]outbox.Delivery, error) {
	deliveries, err := s.Store.ClaimNextN(ctx, customerID, limit)
	s.claimCalls.Add(1)
	if err != nil {
		return nil, err
	}
	s.recordClaim(int64(len(deliveries)))
	s.claimRows.Add(int64(len(deliveries)))
	if len(deliveries) == 0 {
		s.emptyClaims.Add(1)
	}
	return deliveries, nil
}

func (s *tuningInstrumentedStore) ClaimNextBatch(ctx context.Context, customerIDs []string, limit int) ([]outbox.Delivery, error) {
	deliveries, err := s.Store.ClaimNextBatch(ctx, customerIDs, limit)
	s.claimCalls.Add(1)
	if err != nil {
		return nil, err
	}
	s.recordClaim(int64(len(deliveries)))
	s.claimRows.Add(int64(len(deliveries)))
	if len(deliveries) == 0 {
		s.emptyClaims.Add(1)
	}
	return deliveries, nil
}

func (s *tuningInstrumentedStore) HasClaimableWorkBatch(ctx context.Context, customerIDs []string) (map[string]bool, error) {
	return s.Store.HasClaimableWorkBatch(ctx, customerIDs)
}

func (s *tuningInstrumentedStore) ClaimStats() fairnessClaimStats {
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

func bulkSeedTuningDeliveries(ctx context.Context, db *sql.DB, profile tuningProfile) error {
	customerIDs := make([]string, profile.Customers)
	for i := range customerIDs {
		customerIDs[i] = tuningCustomerID(i)
	}
	for start := 0; start < len(customerIDs); start += tuningSeedBatchRows {
		end := min(start+tuningSeedBatchRows, len(customerIDs))
		if err := seedTuningDeliveryBatch(ctx, db, customerIDs[start:end], profile.DeliveriesPerCustomer); err != nil {
			return fmt.Errorf("seed tuning backlog: %w", err)
		}
	}
	return nil
}

func seedTuningDeliveryBatch(ctx context.Context, db *sql.DB, customerIDs []string, deliveriesPerCustomer int) error {
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
		return fmt.Errorf("seed tuning deliveries: %w", err)
	}
	return nil
}

func waitTuningDrain(ctx context.Context, db *sql.DB, store *tuningInstrumentedStore, sched fairnessNotifier) error {
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

func seedTuningCustomers(ctx context.Context, db *sql.DB, count int) error {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = tuningCustomerID(i)
	}
	for start := 0; start < len(ids); start += tuningSeedBatchRows {
		end := min(start+tuningSeedBatchRows, len(ids))
		if err := seedTuningCustomerBatch(ctx, db, ids[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func seedTuningWebhooks(ctx context.Context, db *sql.DB, baseURL string, count int) error {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = tuningCustomerID(i)
	}
	for start := 0; start < len(ids); start += tuningSeedBatchRows {
		end := min(start+tuningSeedBatchRows, len(ids))
		if err := seedTuningWebhookBatch(ctx, db, baseURL, ids[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func tuningCustomerID(index int) string {
	return fmt.Sprintf("%s%05d", tuningCustomerPrefix, index)
}

func seedTuningCustomerBatch(ctx context.Context, db *sql.DB, customerIDs []string) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, len(customerIDs))
	for i, id := range customerIDs {
		if i > 0 {
			sb.WriteString(", ")
		}
		_, _ = fmt.Fprintf(&sb, "($%d, 1)", i+1)
		args = append(args, id)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("seed tuning customers: %w", err)
	}
	return nil
}

func seedTuningWebhookBatch(ctx context.Context, db *sql.DB, baseURL string, customerIDs []string) error {
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
		return fmt.Errorf("seed tuning webhooks: %w", err)
	}
	return nil
}

func queryTuningDeadDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status = 'dead'`).Scan(&n)
	return n, err
}

func queryTuningAttempts(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts`).Scan(&n)
	return n, err
}

func queryTuningFirstAttemptP95(ctx context.Context, db *sql.DB) (float64, error) {
	var p95 float64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(percentile_cont(0.95) WITHIN GROUP (
			ORDER BY EXTRACT(EPOCH FROM (da.sent_at - d.created_at)) * 1000
		), 0)
		FROM deliveries d
		JOIN delivery_attempts da ON da.delivery_id = d.id AND da.attempt_number = 1
	`).Scan(&p95)
	return p95, err
}

func writeTuningReport(path string, profile tuningProfile, results []tuningRunResult) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path
	if err != nil {
		return fmt.Errorf("create tuning report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Configuration Tuning

## Description

Profile **%s** sweeps a grid of worker concurrency parameters against a fixed
Delivery backlog to identify the highest-throughput and lowest-latency configuration.

Each row seeds %d Customers × 3 Webhooks each (fast/slow/flaky) with
%d Deliveries per Customer via bulk SQL (same fan-out shape as real ingest),
then measures the drain.

## Sweep Dimensions

| Parameter | Values tested |
|---|---|
| Workers | %v |
| DeliveryConcurrency | %v |
| MinBatchHint | %v |
| MaxBatchHint | %v |

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | In-process worker goroutines sharing one Postgres database and one process-local Scheduler |
| Scheduler | Equal-share round-robin; weighted scheduling disabled |
| Ingest | Bypassed for load generation; Events and Deliveries are set-based SQL seeded with the same Event-to-Webhook fanout shape |
| Discovery | Manual claimable-Customer DB poll every 200ms; no PgNotify path |
| Claim batching | `+"`MultiPickCount=32`"+` |
| Circuit breaker | Disabled (`+"`breaker_failure_threshold=%d`"+`) to isolate throughput tuning from partner isolation |
| HostLimiter | Disabled; receiver latency and failures are part of Delivery cost, not capped by hostname |
| Retry | Max attempts %d, base delay %s, cap delay %s; MaxAge disabled |
| HTTP | Default shared Worker client, request timeout %s |

## Results (ranked by throughput)

| Rank | Config | Workers | DeliveryConcurrency | MinBatchHint | MaxBatchHint | Total Deliveries | Duration | Throughput (del/s) | Avg Claim Batch | Claim Calls | Empty Claims | Retry Rate | Dead Rate | P95 Latency (ms) | Duplicate tuples | Reliability |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
`, profile.Name,
		profile.Customers, profile.DeliveriesPerCustomer,
		profile.WorkerCounts, profile.DeliveryConcurrencyValues,
		profile.MinBatchHintValues, profile.MaxBatchHintValues,
		profile.BreakerFailureThreshold,
		profile.RetryMaxAttempts, profile.RetryBaseDelay, profile.RetryCapDelay,
		profile.RequestTimeout)

	for rank, r := range results {
		_, _ = fmt.Fprintf(f, "| %d | %s | %d | %d | %d | %d | %d | %s | %.2f | %.2f | %d | %d | %.2f%% | %.2f%% | %.2f | %d | %s |\n",
			rank+1,
			r.Config.Label(),
			r.Config.Workers,
			r.Config.DeliveryConcurrency,
			r.Config.MinBatchHint,
			r.Config.MaxBatchHint,
			r.Total,
			r.Duration.Round(time.Millisecond),
			r.Throughput,
			r.AvgClaimBatch,
			r.ClaimCalls,
			r.EmptyClaims,
			r.RetryRatePct,
			r.DeadRatePct,
			r.P95LatencyMs,
			r.Duplicates,
			passFail(r.ReliabilityOK),
		)
	}
	_, _ = fmt.Fprint(f, `
## Batch Size Progression

Shows average claim batch size per decile of drain progress (0–10%, 10–20%, …, 90–100%).
A steep decline indicates the batch window is emptying faster than workers replenish it;
a flat line at the hint ceiling means workers consistently fill to the limit.

| Config | 0-10% | 10-20% | 20-30% | 30-40% | 40-50% | 50-60% | 60-70% | 70-80% | 80-90% | 90-100% |
|---|---|---|---|---|---|---|---|---|---|---|
`)
	for _, r := range results {
		_, _ = fmt.Fprintf(f, "| %s ", r.Config.Label())
		for _, b := range r.BucketBatches {
			if b > 0 {
				_, _ = fmt.Fprintf(f, "| %.2f ", b)
			} else {
				_, _ = fmt.Fprint(f, "| - ")
			}
		}
		_, _ = fmt.Fprint(f, "|\n")
	}
	_, _ = fmt.Fprint(f, `
## Key Takeaways

- **Throughput** is total deliveries / wall time from seed to full drain.
- **Workers** of 16 vs 32 shows whether additional goroutines improve throughput or add claim contention.
- **DeliveryConcurrency** controls how many concurrent HTTP calls a single worker goroutine sends.
  Higher values overlap partner latency better; too high saturates the host limiter or DB.
- **MinBatchHint / MaxBatchHint** control how many deliveries a worker claims per scheduler pick.
  Larger batches amortise DB round-trips but delay fairness rotation to other Customers.
- **P95 Latency** is first-attempt queue latency — time from delivery creation to first send.
- **Reliability** fails if any duplicate (delivery_id, attempt_number) tuple or >1% dead-letter rate is observed.
`)
	return f.Close()
}
