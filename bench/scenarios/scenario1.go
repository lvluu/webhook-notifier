package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

// RunScenario1 executes Scenario 1: fairness under load with a late-arriving
// Small Customer.
func RunScenario1(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	if err := resetBenchData(ctx, db); err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	slog.Info("reset bench data from previous runs")

	rs := receiver.NewServer(
		// Baseline network latency: 15% of requests experience 200–500ms delay,
		// simulating real-world partner endpoint response times.
		receiver.WithLatency(0.15, 300*time.Millisecond),

		// Transient server errors: 5% of requests get 500, exercising the retry
		// mechanism under load.
		receiver.WithStatus(0.05, http.StatusInternalServerError),

		// Whale endpoint degradation: 10% of POSTs to /hooks/whale get 502,
		// simulating a partner under heavy load or with intermittent outages.
		receiver.WithRouteStatus("/hooks/whale", 0.10, http.StatusBadGateway),
	)
	defer rs.Close()

	reg := registry.New(db)
	plainSched := roundrobin.New()
	instrSched := newInstrumentedScheduler(plainSched)
	ingestStore := ingest.New(db, reg, instrSched)
	outboxStore := outbox.New(db)

	if err := seedCustomers(ctx, db); err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}
	slog.Info("seeded customers")

	if err := seedWebhooks(ctx, db, rs.URL()); err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}
	slog.Info("seeded webhooks", "receiver_url", rs.URL())

	if _, err := seedWhaleDeliveries(ctx, db); err != nil {
		return fmt.Errorf("seed whale deliveries: %w", err)
	}
	slog.Info("seeded whale deliveries")

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup
	for i := range workerCount {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(instrSched, worker.WithLogger(slog.Default()))
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	slog.Info("started workers", "count", workerCount)

	instrSched.Notify(whaleCustomerID)
	slog.Info("notified scheduler for whale")

	dripDone := make(chan struct{})
	go func() {
		defer close(dripDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for i := range smallCount {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
			}
			cid := smallCustomerID(i)
			if err := ingestSmallDeliveries(workerCtx, ingestStore, cid, smallDeliveries); err != nil {
				slog.Error("drip failed", "customer", cid, "error", err)
				return
			}
		}
	}()
	slog.Info("started small drip", "count", smallCount, "interval", "1s")

	picksBeforeLate := instrSched.TotalPicks()
	activeAtNotifyCh := make(chan int, 1)

	go func() {
		select {
		case <-workerCtx.Done():
			return
		case <-time.After(30 * time.Second):
		}
		if err := ingestSmallDeliveries(workerCtx, ingestStore, lateSmallID, lateDeliveries); err != nil {
			slog.Error("late inject failed", "error", err)
		}
		instrSched.Notify(lateSmallID)
		slog.Info("notified scheduler for late small")

		n, err := countActiveCustomers(workerCtx, db)
		if err != nil {
			slog.Error("count active customers at late notify", "error", err)
			n = 0
		}
		activeAtNotifyCh <- n
	}()

	slog.Info("waiting for all deliveries to complete")

	startTime := time.Now()
	pollCtx, cancelPoll := context.WithTimeout(ctx, 15*time.Minute)
	defer cancelPoll()

	for {
		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return fmt.Errorf("poll pending deliveries: %w", err)
		}
		if pending == 0 {
			break
		}
		elapsed := time.Since(startTime)
		if elapsed%30 == 0 || elapsed < 5*time.Second {
			slog.Info("waiting for completion", "pending", pending, "elapsed_seconds", int(elapsed.Seconds()))
		}
		select {
		case <-pollCtx.Done():
			return fmt.Errorf("deliveries not drained within timeout: %d pending", pending)
		case <-time.After(time.Second):
		}
	}

	cancelWorkers()
	workerWg.Wait()
	<-dripDone

	totalDuration := time.Since(startTime)
	slog.Info("all deliveries completed", "duration_seconds", int(totalDuration.Seconds()))

	picksAfterLate := instrSched.TotalPicks()
	lateCyclePicks := picksAfterLate - picksBeforeLate
	activeAtLateNotify := <-activeAtNotifyCh

	reportPath := "bench/results/scenario-1.md"
	if err := writeScenario1Report(ctx, db, reportPath, scenario1ReportParams{
		totalDuration:   totalDuration,
		lateCyclePicks:  lateCyclePicks,
		activeAtNotify:  activeAtLateNotify,
		picksBeforeLate: picksBeforeLate,
		picksAfterLate:  picksAfterLate,
	}); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("scenario 1 complete", "report", reportPath)
	return nil
}

func countActiveCustomers(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT customer_id)
		FROM deliveries
		WHERE status NOT IN ('succeeded', 'dead')
	`).Scan(&n)
	return n, err
}

type scenario1ReportParams struct {
	totalDuration   time.Duration
	lateCyclePicks  int64
	activeAtNotify  int
	picksBeforeLate int64
	picksAfterLate  int64
}

func writeScenario1Report(
	ctx context.Context,
	db *sql.DB,
	path string,
	p scenario1ReportParams,
) error {
	type latencyRow struct {
		Label    string
		Customer string // empty = all
	}
	rows := []latencyRow{
		{Label: "Whale (P50/P95/P99/Max)", Customer: whaleCustomerID},
		{Label: "All Small (P50/P95/P99/Max)", Customer: "small-%"},
		{Label: "Late Small (P50/P95/P99/Max)", Customer: lateSmallID},
		{Label: "Overall (P50/P95/P99/Max)", Customer: ""},
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

_, _ = fmt.Fprintf(f, `# Scenario 1: Fairness Under Load — Late-Arriving Small Customer

## Description

One **Whale** (5k deliveries, burst at T=0) + **100 Small Customers** (5 deliveries each, dripped one per second starting at T=1) + **1 Late Small Customer** (3 deliveries, injected at T=30).

**Chaos conditions**: 15%% of requests experience 300ms latency (simulating network round-trips), 5%% transient 500 errors (exercising retry), and the whale endpoint returns 502 10%% of the time (simulating a partner under load).

The acceptance criterion is that the late-arriving Small Customer must be picked by a worker within at most one round-robin cycle of its Notify, and that small-customer P99 latency stays under 10s despite failures.

## Results

| Metric | Value |
|---|---|
| Total duration | %s |
| Workers | %d |
| RR cycle picks after late Notify | %d |
| Active Customers at late Notify | %d |
| RR cycle bound satisfied | %s |

`, p.totalDuration.Round(time.Second), workerCount, p.lateCyclePicks, p.activeAtNotify,
		boolStr(p.lateCyclePicks <= int64(p.activeAtNotify)))

	for _, r := range rows {
		p50, p95, p99, maxLat, err := queryLatencyPercentiles(ctx, db, r.Customer)
		if err != nil {
			slog.Error("latency query failed", "label", r.Label, "error", err)
			continue
		}
		_, _ = fmt.Fprintf(f, "| **%s** | %.0f ms | %.0f ms | %.0f ms | %.0f ms |\n",
			r.Label, p50, p95, p99, maxLat)
	}

	_, _ = fmt.Fprintf(f, "\n## Delivery Outcomes\n\n")
	outcomes, err := queryDeliveryOutcomes(ctx, db)
	if err != nil {
		slog.Error("outcomes query failed", "error", err)
	} else {
		_, _ = fmt.Fprintf(f, "| Status | Count |\n|---|---|\n")
		for _, o := range outcomes {
			_, _ = fmt.Fprintf(f, "| %s | %d |\n", o.Status, o.Count)
		}
	}

	_, _ = fmt.Fprintf(f, "\n## Retry Distribution\n\n")
	retries, err := queryRetryDistribution(ctx, db)
	if err != nil {
		slog.Error("retry distribution query failed", "error", err)
	} else {
		_, _ = fmt.Fprintf(f, "| Attempts | Deliveries |\n|---|---|\n")
		for _, r := range retries {
			_, _ = fmt.Fprintf(f, "| %d | %d |\n", r.Attempts, r.Count)
		}
	}

	_, _ = fmt.Fprintf(f, `
## Round-Robin Cycle Bound

Picks before late Notify: **%d**
Picks after late Notify: **%d**
Delta (picks during cycle): **%d**
Active Customers at late Notify: **%d**

%s
`,
		p.picksBeforeLate, p.picksAfterLate,
		p.lateCyclePicks, p.activeAtNotify,
		cycleVerdict(p.lateCyclePicks, int64(p.activeAtNotify)))

	return f.Close()
}

func queryLatencyPercentiles(ctx context.Context, db *sql.DB, customerFilter string) (p50, p95, p99, maxMs float64, err error) {
	var whereClause string
	var args []any
	if customerFilter == "" {
		whereClause = "status = 'succeeded'"
	} else if customerFilter == "small-%" {
		whereClause = "status = 'succeeded' AND customer_id LIKE 'small-%'"
	} else {
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

func boolStr(v bool) string {
	if v {
		return "✅ Yes"
	}
	return "❌ No"
}

func cycleVerdict(picks, active int64) string {
	if picks <= active {
		return fmt.Sprintf("**PASS**: %d Picks ≤ %d active Customers — the late-arriving Small was served within one round-robin cycle.", picks, active)
	}
	return fmt.Sprintf("**FAIL**: %d Picks > %d active Customers — the late-arriving Small was not served within one round-robin cycle.", picks, active)
}

type deliveryOutcome struct {
	Status string
	Count  int
}

func queryDeliveryOutcomes(ctx context.Context, db *sql.DB) ([]deliveryOutcome, error) {
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

	var outcomes []deliveryOutcome
	for rows.Next() {
		var o deliveryOutcome
		if err := rows.Scan(&o.Status, &o.Count); err != nil {
			return nil, fmt.Errorf("scan outcome: %w", err)
		}
		outcomes = append(outcomes, o)
	}
	return outcomes, rows.Err()
}

type retryBucket struct {
	Attempts int
	Count    int
}

func queryRetryDistribution(ctx context.Context, db *sql.DB) ([]retryBucket, error) {
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

	var buckets []retryBucket
	for rows.Next() {
		var b retryBucket
		if err := rows.Scan(&b.Attempts, &b.Count); err != nil {
			return nil, fmt.Errorf("scan retry bucket: %w", err)
		}
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}
