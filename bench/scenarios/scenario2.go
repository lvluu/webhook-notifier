package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	fatCustomerPrefix = "fat"
	fatCount          = 8
	deliveriesPerFat  = 1250
)

func fatCustomerID(i int) string {
	return fmt.Sprintf("%s-%02d", fatCustomerPrefix, i)
}

// RunScenario2 executes Scenario 2: happy-case throughput with no chaos.
//
// Setup: 8 customers with 1250 deliveries each (10,000 total), burst at T=0.
// Chaos: none (0% latency, 0% errors).
// Expected: throughput significantly above scenario 1 (~17.6/s), targeting
// the SCALABILITY_REVIEW normal range (2,000-15,000 deliveries/sec).
func RunScenario2(ctx context.Context) error {
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

	if err := resetBenchData(ctx, db); err != nil {
		return fmt.Errorf("reset bench data: %w", err)
	}
	slog.Info("reset bench data from previous runs")

	rs := receiver.NewServer()
	defer rs.Close()

	plainSched := roundrobin.New()
	store := outbox.New(db)

	if err := seedScenario2Customers(ctx, db); err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}
	slog.Info("seeded customers", "count", fatCount)

	if err := seedScenario2Webhooks(ctx, db, rs.URL()); err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}
	slog.Info("seeded webhooks", "receiver_url", rs.URL())

	totalDeliveries := fatCount * deliveriesPerFat

	if err := seedScenario2Deliveries(ctx, db); err != nil {
		return fmt.Errorf("seed deliveries: %w", err)
	}
	slog.Info("seeded deliveries", "total", totalDeliveries)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup
	for i := range workerCount {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(plainSched, worker.WithLogger(slog.Default()))
			if err := w.Run(workerCtx, store); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	slog.Info("started workers", "count", workerCount)

	for i := 0; i < fatCount; i++ {
		plainSched.Notify(fatCustomerID(i))
	}
	slog.Info("notified scheduler for all fat customers")

	slog.Info("waiting for all deliveries to complete")
	startTime := time.Now()

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Minute)
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
		if elapsed%5 == 0 || elapsed < 5*time.Second {
			slog.Info("waiting for completion",
				"pending", pending,
				"elapsed_seconds", int(elapsed.Seconds()),
			)
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
	throughput := float64(totalDeliveries) / totalDuration.Seconds()
	slog.Info("all deliveries completed",
		"duration_seconds", int(totalDuration.Seconds()),
		"throughput_per_sec", int(throughput),
	)

	reportPath := "bench/results/scenario-2.md"
	if err := writeScenario2Report(ctx, db, reportPath, scenario2ReportParams{
		totalDuration:    totalDuration,
		totalDeliveries:  totalDeliveries,
		throughputPerSec: throughput,
	}); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("scenario 2 complete", "report", reportPath)
	return nil
}

func seedScenario2Customers(ctx context.Context, db *sql.DB) error {
	for i := 0; i < fatCount; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO customers (id, weight) VALUES ($1, 1) ON CONFLICT (id) DO NOTHING`,
			fatCustomerID(i),
		); err != nil {
			return fmt.Errorf("seed customer %s: %w", fatCustomerID(i), err)
		}
	}
	return nil
}

func seedScenario2Webhooks(ctx context.Context, db *sql.DB, receiverBaseURL string) error {
	for i := 0; i < fatCount; i++ {
		cid := fatCustomerID(i)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active)
			VALUES ($1, $2, $3, $4, true)
			ON CONFLICT DO NOTHING
		`, cid, receiverBaseURL+"/hooks/"+cid, "", fmt.Sprintf("{%s}", eventType)); err != nil {
			return fmt.Errorf("seed webhook for %s: %w", cid, err)
		}
	}
	return nil
}

func seedScenario2Deliveries(ctx context.Context, db *sql.DB) error {
	for i := 0; i < fatCount; i++ {
		cid := fatCustomerID(i)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", cid, err)
		}

		var eventID string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO events (customer_id, type, payload)
			VALUES ($1, $2, '{}')
			RETURNING id
		`, cid, eventType).Scan(&eventID)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert event for %s: %w", cid, err)
		}

		var webhookID string
		err = tx.QueryRowContext(ctx, `
			SELECT id FROM webhooks WHERE customer_id = $1 LIMIT 1
		`, cid).Scan(&webhookID)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("lookup webhook for %s: %w", cid, err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
			SELECT $1, $2, $3, 'pending', 0, now()
			FROM generate_series(1, $4)
		`, eventID, webhookID, cid, deliveriesPerFat)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("bulk insert deliveries for %s: %w", cid, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit seed tx for %s: %w", cid, err)
		}
	}

	return nil
}

type scenario2ReportParams struct {
	totalDuration    time.Duration
	totalDeliveries  int
	throughputPerSec float64
}

// writeScenario2Report writes a throughput-focused report that compares
// results against SCALABILITY_REVIEW volume targets.
func writeScenario2Report(
	ctx context.Context,
	db *sql.DB,
	path string,
	p scenario2ReportParams,
) error {
	type latencyRow struct {
		Label    string
		Customer string
	}
	rows := []latencyRow{
		{Label: "Fat Customers (P50/P95/P99/Max)", Customer: "fat-%"},
		{Label: "Overall (P50/P95/P99/Max)", Customer: ""},
	}

	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	normalTarget := "2,000-15,000"
	peakTarget := "20,000-150,000"

	var verdict string
	switch {
	case p.throughputPerSec >= 20000:
		verdict = fmt.Sprintf("Peak range (%.0f/s) — meets SCALABILITY_REVIEW peak target of %s/s", p.throughputPerSec, peakTarget)
	case p.throughputPerSec >= 2000:
		verdict = fmt.Sprintf("Normal range (%.0f/s) — meets SCALABILITY_REVIEW normal target of %s/s", p.throughputPerSec, normalTarget)
	default:
		verdict = fmt.Sprintf("Below target (%.0f/s) — does not meet SCALABILITY_REVIEW normal target of %s/s", p.throughputPerSec, normalTarget)
	}

	_, _ = fmt.Fprintf(f, `# Scenario 2: Happy-Case Throughput

## Description

**%d deliveries** across **%d customers** (%d each), burst at T=0. No chaos injection (0%% latency, 0%% errors). Measures raw throughput of the claim→POST→mark pipeline.

Compared to Scenario 1 (~17.6 deliveries/s with 5%% 500s + 10%% whale 502s + 15%% latency + 1s drip).

## Results

| Metric | Value |
|---|---|
| Total duration | %s |
| Total deliveries | %d |
| Workers | %d |
| Throughput | **%.0f deliveries/s** |
| SCALABILITY_REVIEW: Normal target | %s/s |
| SCALABILITY_REVIEW: Peak target | %s/s |
| Verdict | %s |

`,
		p.totalDeliveries, fatCount, deliveriesPerFat,
		p.totalDuration.Round(time.Second), p.totalDeliveries,
		workerCount, p.throughputPerSec,
		normalTarget, peakTarget, verdict)

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

	return f.Close()
}
