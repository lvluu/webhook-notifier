package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
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
		receiver.WithStableDecisions("scenario-1"),
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

	dripDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for i := range smallCount {
			select {
			case <-workerCtx.Done():
				dripDone <- workerCtx.Err()
				return
			case <-ticker.C:
			}
			cid := smallCustomerID(i)
			if err := ingestSmallDeliveries(workerCtx, ingestStore, cid, smallDeliveries); err != nil {
				dripDone <- fmt.Errorf("drip small customer %s: %w", cid, err)
				return
			}
		}
		dripDone <- nil
	}()
	slog.Info("started small drip", "count", smallCount, "interval", "1s")

	picksBeforeLate := instrSched.TotalPicks()
	activeAtNotifyCh := make(chan int, 1)
	lateDone := make(chan error, 1)

	go func() {
		var active int
		defer func() { activeAtNotifyCh <- active }()

		select {
		case <-workerCtx.Done():
			lateDone <- workerCtx.Err()
			return
		case <-time.After(30 * time.Second):
		}
		if err := ingestSmallDeliveries(workerCtx, ingestStore, lateSmallID, lateDeliveries); err != nil {
			lateDone <- fmt.Errorf("late inject: %w", err)
			return
		}
		// Begin tracking BEFORE Notify so the cycle-start baseline captures
		// all picks before the late customer enters rotation.
		instrSched.BeginTracking(lateSmallID)
		instrSched.Notify(lateSmallID)
		slog.Info("notified scheduler for late small")

		n, err := countActiveCustomers(workerCtx, db)
		if err != nil {
			slog.Error("count active customers at late notify", "error", err)
			n = 0
		}
		active = n
		lateDone <- nil
	}()

	slog.Info("waiting for all deliveries to complete")

	startTime := time.Now()
	pollCtx, cancelPoll := context.WithTimeout(ctx, 15*time.Minute)
	defer cancelPoll()

	dripComplete := false
	lateComplete := false
	expectedDeliveries := whaleDeliveries + smallCount*smallDeliveries + lateDeliveries

	for {
		if !dripComplete {
			select {
			case err := <-dripDone:
				if err != nil {
					return err
				}
				dripComplete = true
			default:
			}
		}
		if !lateComplete {
			select {
			case err := <-lateDone:
				if err != nil {
					return err
				}
				lateComplete = true
			default:
			}
		}

		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return fmt.Errorf("poll pending deliveries: %w", err)
		}
		if dripComplete && lateComplete && pending == 0 {
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

	totalDuration := time.Since(startTime)
	slog.Info("all deliveries completed", "duration_seconds", int(totalDuration.Seconds()))

	lateCyclePicks := instrSched.CyclePicks()
	picksAfterLate := instrSched.TotalPicks()
	activeAtLateNotify := <-activeAtNotifyCh
	totalDeliveries, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query total deliveries: %w", err)
	}
	lateFirstLatencyMs, err := queryFirstTerminalLatencyMs(ctx, db, lateSmallID)
	if err != nil {
		return fmt.Errorf("query late first latency: %w", err)
	}

	reportPath := "bench/results/scenario-1.md"
	if err := writeScenario1Report(ctx, db, reportPath, scenario1ReportParams{
		totalDuration:   totalDuration,
		totalDeliveries: totalDeliveries,
		expectedTotal:   expectedDeliveries,
		lateFirstMs:     lateFirstLatencyMs,
		lateCyclePicks:  lateCyclePicks,
		picksUntilFirst: lateCyclePicks,
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
	totalDeliveries int
	expectedTotal   int
	lateFirstMs     float64
	lateCyclePicks  int64 // picks from BeginTracking to first service of late customer
	picksUntilFirst int64 // same value, clearer name for the report
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
		{Label: "Whale", Customer: whaleCustomerID},
		{Label: "All Small", Customer: "small-%"},
		{Label: "Late Small", Customer: lateSmallID},
		{Label: "Overall", Customer: ""},
	}
	_, _, allSmallP99, _, err := queryLatencyPercentiles(ctx, db, "small-%")
	if err != nil {
		return fmt.Errorf("query small lifecycle latencies: %w", err)
	}
	httpP50, httpP95, httpP99, httpMax, err := queryAttemptDurationPercentiles(ctx, db)
	if err != nil {
		return fmt.Errorf("query HTTP attempt durations: %w", err)
	}
	totalOK := p.totalDeliveries == p.expectedTotal
	smallP99OK := allSmallP99 < 2000
	cycleOK := p.picksUntilFirst <= int64(p.activeAtNotify)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Scenario 1: Fairness Under Load — Late-Arriving Small Customer

## Description

One **Whale** (50k deliveries, burst at T=0) + **100 Small Customers** (5 deliveries each, dripped one per second starting at T=1) + **1 Late Small Customer** (3 deliveries, injected at T=30).

**Chaos conditions**: 15%% of requests experience 300ms latency (simulating network round-trips), 5%% transient 500 errors (exercising retry), and the whale endpoint returns 502 10%% of the time (simulating a partner under load).

The acceptance criteria are that Small Customers have lifecycle p99 under 2s, the late-arriving Small Customer is picked within at most one round-robin cycle of its Notify, and the generated workload matches the deterministic setup.

## Results

| Metric | Value |
|---|---|
| Total duration | %s |
| Total deliveries | %d / %d |
| Workers | %d |
| Picks until first late service | %d |
| Active Customers at late Notify | %d |
| Late Small first terminal lifecycle latency | %.0f ms |
| RR cycle bound satisfied | %s |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| Full workload created | %d deliveries | %d deliveries | %s |
| All Small lifecycle p99 | %.0f ms | < 2000 ms | %s |
| Late Small picked within one RR cycle | %d picks | <= %d active Customers | %s |

## Lifecycle Latency (enqueue to terminal)

| Class | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|

`, p.totalDuration.Round(time.Second), p.totalDeliveries, p.expectedTotal,
		workerCount, p.picksUntilFirst, p.activeAtNotify, p.lateFirstMs, boolStr(cycleOK),
		p.totalDeliveries, p.expectedTotal, passFail(totalOK),
		allSmallP99, passFail(smallP99OK),
		p.picksUntilFirst, p.activeAtNotify, passFail(cycleOK))

	for _, r := range rows {
		p50, p95, p99, maxLat, err := queryLatencyPercentiles(ctx, db, r.Customer)
		if err != nil {
			slog.Error("latency query failed", "label", r.Label, "error", err)
			continue
		}
		_, _ = fmt.Fprintf(f, "| **%s** | %.0f ms | %.0f ms | %.0f ms | %.0f ms |\n",
			r.Label, p50, p95, p99, maxLat)
	}
	_, _ = fmt.Fprintf(f, `
## HTTP Attempt Duration

| Scope | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **All attempts** | %.0f ms | %.0f ms | %.0f ms | %.0f ms |

`, httpP50, httpP95, httpP99, httpMax)

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
Picks after late Notify: **%d** (total for remaining benchmark)
Picks until first late service: **%d**
Active Customers at late Notify: **%d**

%s
`,
		p.picksBeforeLate, p.picksAfterLate,
		p.picksUntilFirst, p.activeAtNotify,
		cycleVerdict(p.picksUntilFirst, int64(p.activeAtNotify)))

	return f.Close()
}

func queryLatencyPercentiles(ctx context.Context, db *sql.DB, customerFilter string) (p50, p95, p99, maxMs float64, err error) {
	var whereClause string
	var args []any
	if customerFilter == "" {
		whereClause = "status = 'succeeded'"
	} else if strings.Contains(customerFilter, "%") {
		whereClause = "status = 'succeeded' AND customer_id LIKE $1"
		args = append(args, customerFilter)
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

func queryAttemptDurationPercentiles(ctx context.Context, db *sql.DB) (p50, p95, p99, maxMs float64, err error) {
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

func queryTotalDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries`).Scan(&n)
	return n, err
}

func queryFirstTerminalLatencyMs(ctx context.Context, db *sql.DB, customerID string) (float64, error) {
	var ms float64
	err := db.QueryRowContext(ctx, `
		SELECT EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000
		FROM deliveries
		WHERE customer_id = $1
		  AND status IN ('succeeded', 'dead')
		ORDER BY updated_at
		LIMIT 1
	`, customerID).Scan(&ms)
	if err != nil {
		return 0, fmt.Errorf("query first terminal latency: %w", err)
	}
	return ms, nil
}

func boolStr(v bool) string {
	if v {
		return "✅ Yes"
	}
	return "❌ No"
}

func passFail(v bool) string {
	if v {
		return "**PASS**"
	}
	return "**FAIL**"
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
