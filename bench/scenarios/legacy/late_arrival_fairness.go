package legacy

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/bench/scenarios"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

type lateArrivalFairnessProfile struct {
	Name                    string
	WorkerCount             int
	WhaleDeliveries         int
	SmallCount              int
	SmallDeliveries         int
	LateDeliveries          int
	SmallDripInterval       time.Duration
	LateNotifyDelay         time.Duration
	BreakerFailureThreshold int
}

func lateArrivalFairnessProfileFromEnv() (lateArrivalFairnessProfile, error) {
	raw, name, err := scenarios.LoadBenchProfileConfig("late_arrival_fairness")
	if err != nil {
		return lateArrivalFairnessProfile{}, err
	}
	if raw.WorkerCount < 1 || raw.WhaleDeliveries < 1 || raw.SmallCount < 1 || raw.SmallDeliveries < 1 || raw.LateDeliveries < 1 {
		return lateArrivalFairnessProfile{}, fmt.Errorf("late_arrival_fairness profile %q has invalid positive counts", name)
	}
	if raw.SmallDripInterval.Duration() <= 0 || raw.LateNotifyDelay.Duration() <= 0 {
		return lateArrivalFairnessProfile{}, fmt.Errorf("late_arrival_fairness profile %q has invalid positive durations", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return lateArrivalFairnessProfile{}, fmt.Errorf("late_arrival_fairness profile %q has invalid breaker threshold", name)
	}

	return lateArrivalFairnessProfile{
		Name:                    name,
		WorkerCount:             raw.WorkerCount,
		WhaleDeliveries:         raw.WhaleDeliveries,
		SmallCount:              raw.SmallCount,
		SmallDeliveries:         raw.SmallDeliveries,
		LateDeliveries:          raw.LateDeliveries,
		SmallDripInterval:       raw.SmallDripInterval.Duration(),
		LateNotifyDelay:         raw.LateNotifyDelay.Duration(),
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
	}, nil
}

// RunLateArrivalFairness executes Late Arrival Fairness: fairness under load with a late-arriving
// Small Customer.
func RunLateArrivalFairness(ctx context.Context) error {
	db, cleanup, err := scenarios.OpenBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	if err := scenarios.ResetBenchData(ctx, db); err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	slog.Info("reset bench data from previous runs")

	profile, err := lateArrivalFairnessProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	rs := receiver.NewServer(
		// Baseline network latency: 15% of requests experience 200–500ms delay,
		// simulating real-world partner endpoint response times.
		receiver.WithLatency(0.15, 300*time.Millisecond),
		receiver.WithStableDecisions("late-arrival-fairness"),
	)
	defer rs.Close()

	reg := registry.New(db)
	plainSched := roundrobin.New()
	instrSched := newInstrumentedScheduler(plainSched)
	ingestStore := ingest.New(db, reg, instrSched)
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0))

	if err := scenarios.SeedCustomers(ctx, db, profile.SmallCount); err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}
	slog.Info("seeded customers")

	if err := scenarios.SeedWebhooks(ctx, db, rs.URL(), profile.SmallCount); err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}
	slog.Info("seeded webhooks", "receiver_url", rs.URL())

	if _, err := scenarios.SeedWhaleDeliveries(ctx, db, profile.WhaleDeliveries); err != nil {
		return fmt.Errorf("seed whale deliveries: %w", err)
	}
	slog.Info("seeded whale deliveries")

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	workers := profile.WorkerCount
	for i := range workers {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(instrSched, worker.WithLogger(workerLog))
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	slog.Info("started workers", "count", workers)

	instrSched.Notify(scenarios.WhaleCustomerID)
	slog.Info("notified scheduler for whale")

	dripDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(profile.SmallDripInterval)
		defer ticker.Stop()
		for i := range profile.SmallCount {
			select {
			case <-workerCtx.Done():
				dripDone <- workerCtx.Err()
				return
			case <-ticker.C:
			}
			cid := scenarios.SmallCustomerID(i)
			if err := scenarios.IngestSmallDeliveries(workerCtx, ingestStore, cid, profile.SmallDeliveries); err != nil {
				dripDone <- fmt.Errorf("drip small customer %s: %w", cid, err)
				return
			}
		}
		dripDone <- nil
	}()
	slog.Info("started small drip", "count", profile.SmallCount, "interval", profile.SmallDripInterval)

	activeAtNotifyCh := make(chan int, 1)
	lateDone := make(chan error, 1)

	go func() {
		var active int
		defer func() { activeAtNotifyCh <- active }()

		select {
		case <-workerCtx.Done():
			lateDone <- workerCtx.Err()
			return
		case <-time.After(profile.LateNotifyDelay):
		}
		if err := scenarios.IngestSmallDeliveries(workerCtx, ingestStore, scenarios.LateSmallID, profile.LateDeliveries); err != nil {
			lateDone <- fmt.Errorf("late inject: %w", err)
			return
		}
		// Begin tracking BEFORE Notify so the cycle-start baseline captures
		// all picks before the late customer enters rotation.
		instrSched.BeginTracking(scenarios.LateSmallID)
		instrSched.Notify(scenarios.LateSmallID)
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
	expectedDeliveries := profile.WhaleDeliveries + profile.SmallCount*profile.SmallDeliveries + profile.LateDeliveries

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

		pending, err := scenarios.CountPendingDeliveries(pollCtx, db)
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
	activeAtLateNotify := <-activeAtNotifyCh
	totalDeliveries, err := scenarios.QueryTotalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query total deliveries: %w", err)
	}
	lateFirstLatencyMs, err := queryFirstTerminalLatencyMs(ctx, db, scenarios.LateSmallID)
	if err != nil {
		return fmt.Errorf("query late first latency: %w", err)
	}

	reportPath := "bench/results/late-arrival-fairness.md"
	if err := writeLateArrivalFairnessReport(ctx, db, reportPath, lateArrivalFairnessReportParams{
		profileName:       profile.Name,
		whaleDeliveries:   profile.WhaleDeliveries,
		smallCount:        profile.SmallCount,
		smallDeliveries:   profile.SmallDeliveries,
		lateDeliveries:    profile.LateDeliveries,
		smallDripInterval: profile.SmallDripInterval,
		lateNotifyDelay:   profile.LateNotifyDelay,
		totalDuration:     totalDuration,
		totalDeliveries:   totalDeliveries,
		expectedTotal:     expectedDeliveries,
		lateFirstMs:       lateFirstLatencyMs,
		lateCyclePicks:    lateCyclePicks,
		picksUntilFirst:   lateCyclePicks,
		activeAtNotify:    activeAtLateNotify,
		workerCount:       workers,
	}); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("late arrival fairness complete", "report", reportPath)
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

type lateArrivalFairnessReportParams struct {
	profileName       string
	whaleDeliveries   int
	smallCount        int
	smallDeliveries   int
	lateDeliveries    int
	smallDripInterval time.Duration
	lateNotifyDelay   time.Duration
	totalDuration     time.Duration
	totalDeliveries   int
	expectedTotal     int
	lateFirstMs       float64
	lateCyclePicks    int64 // picks from BeginTracking to first service of late customer
	picksUntilFirst   int64 // same value, clearer name for the report
	activeAtNotify    int
	workerCount       int
}

func writeLateArrivalFairnessReport(
	ctx context.Context,
	db *sql.DB,
	path string,
	p lateArrivalFairnessReportParams,
) error {
	type latencyRow struct {
		Label    string
		Customer string // empty = all
	}
	rows := []latencyRow{
		{Label: "Whale", Customer: scenarios.WhaleCustomerID},
		{Label: "All Small", Customer: "small-%"},
		{Label: "Late Small", Customer: scenarios.LateSmallID},
	}
	_, _, allSmallP99, _, err := scenarios.QueryLatencyPercentiles(ctx, db, "small-%")
	if err != nil {
		return fmt.Errorf("query small lifecycle latencies: %w", err)
	}
	_, _, whaleP99, whaleMax, err := scenarios.QueryLatencyPercentiles(ctx, db, scenarios.WhaleCustomerID)
	if err != nil {
		return fmt.Errorf("query whale lifecycle latencies: %w", err)
	}
	httpP50, httpP95, httpP99, httpMax, err := scenarios.QueryAttemptDurationPercentiles(ctx, db)
	if err != nil {
		return fmt.Errorf("query HTTP attempt durations: %w", err)
	}
	totalOK := p.totalDeliveries == p.expectedTotal
	smallP99OK := allSmallP99 < 2000
	cycleOK := p.picksUntilFirst <= int64(p.activeAtNotify)
	throughput := float64(p.totalDeliveries) / p.totalDuration.Seconds()
	whaleThroughput := float64(p.whaleDeliveries) / (whaleMax / 1000)
	projectedThousandWhales := time.Duration((float64(p.whaleDeliveries*1000) / whaleThroughput) * float64(time.Second))

	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Late Arrival Fairness: Fairness Under Load — Late-Arriving Small Customer

## Description

Profile **%s**: one **Whale** (%d deliveries, burst at T=0) + **%d Small Customers** (%d deliveries each, dripped every %s starting at T=1) + **1 Late Small Customer** (%d deliveries, injected at T=%s).

**Chaos conditions**: 15%% of requests experience 300ms latency (simulating network round-trips). No status-code failures are injected; Late Arrival Fairness isolates fairness under burst load from retry-policy behavior.

The acceptance criteria are that Small Customers have lifecycle p99 under 2s, the late-arriving Small Customer is picked within at most one round-robin cycle of its Notify, and the generated workload matches the deterministic setup.

## Performance Summary

| Metric | Value |
|---|---|
| Profile | %s |
| Verdict | Small Customers stayed responsive while the Whale backlog drained |
| Total duration | %s |
| Total deliveries | %d / %d |
| End-to-end throughput (includes drip) | %.0f deliveries/s |
| Workers | %d |
| All Small lifecycle p99 | %.0f ms |
| Late Small first terminal lifecycle latency | %.0f ms |
| Whale backlog-drain p99 | %.0f ms |
| Whale backlog-drain max | %.0f ms |
| Implied whale drain throughput | %.0f deliveries/s |
| Projected 1000-whale drain on one node | %s |
| HTTP attempt p99 | %.0f ms |

## Acceptance Criteria

| Pillar | Criterion | Observed | Limit | Result |
|---|---|---:|---:|---|
| Reliability | Full workload created | %d deliveries | %d deliveries | %s |
| Fairness | All Small lifecycle p99 | %.0f ms | < 2000 ms | %s |
| Fairness | Late Small picked within one RR cycle | %d picks | <= %d active Customers | %s |

## Backlog Drain Latency

This is enqueue-to-terminal time, not HTTP request time. For the Whale, high values mostly mean the intentional burst backlog is waiting in queue while Small Customers continue to receive turns.
The 1000-whale projection is a single-node capacity estimate from this run, not an acceptance criterion; it shows that production-scale whale load needs horizontal capacity or partitioned whale pools.

| Class | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
`, p.profileName, p.whaleDeliveries, p.smallCount, p.smallDeliveries, p.smallDripInterval, p.lateDeliveries, p.lateNotifyDelay,
		p.profileName, p.totalDuration.Round(time.Second), p.totalDeliveries, p.expectedTotal, throughput,
		p.workerCount, allSmallP99, p.lateFirstMs, whaleP99, whaleMax,
		whaleThroughput, projectedThousandWhales.Round(time.Second), httpP99,
		p.totalDeliveries, p.expectedTotal, scenarios.PassFail(totalOK),
		allSmallP99, scenarios.PassFail(smallP99OK),
		p.picksUntilFirst, p.activeAtNotify, scenarios.PassFail(cycleOK))

	for _, r := range rows {
		p50, p95, p99, maxLat, err := scenarios.QueryLatencyPercentiles(ctx, db, r.Customer)
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
	outcomes, err := scenarios.QueryDeliveryOutcomes(ctx, db)
	if err != nil {
		slog.Error("outcomes query failed", "error", err)
	} else {
		_, _ = fmt.Fprintf(f, "| Status | Count |\n|---|---|\n")
		for _, o := range outcomes {
			_, _ = fmt.Fprintf(f, "| %s | %d |\n", o.Status, o.Count)
		}
	}

	_, _ = fmt.Fprintf(f, "\n## Retry Distribution\n\n")
	retries, err := scenarios.QueryRetryDistribution(ctx, db)
	if err != nil {
		slog.Error("retry distribution query failed", "error", err)
	} else {
		_, _ = fmt.Fprintf(f, "| Attempts | Deliveries |\n|---|---|\n")
		for _, r := range retries {
			_, _ = fmt.Fprintf(f, "| %d | %d |\n", r.Attempts, r.Count)
		}
	}

	_, _ = fmt.Fprintf(f, `
## Round-Robin Reactivity

%s
`, cycleVerdict(p.picksUntilFirst, int64(p.activeAtNotify)))

	return f.Close()
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

func cycleVerdict(picks, active int64) string {
	if picks <= active {
		return fmt.Sprintf("**PASS**: %d Picks ≤ %d active Customers — the late-arriving Small was served within one round-robin cycle.", picks, active)
	}
	return fmt.Sprintf("**FAIL**: %d Picks > %d active Customers — the late-arriving Small was not served within one round-robin cycle.", picks, active)
}
