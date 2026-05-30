package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
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
	scenario3CustomerID         = "scenario-3-customer"
	scenario3EventCount         = 1000
	scenario3WebhookCount       = 2
	scenario3ExpectedDeliveries = scenario3EventCount * scenario3WebhookCount
	scenario3ReportPath         = "bench/results/scenario-3.md"
)

var scenario3Webhooks = []struct {
	Label string
	Path  string
}{
	{Label: "good", Path: "/hooks/good"},
	{Label: "misconfigured-404", Path: "/hooks/misconfigured-404"},
}

// RunScenario3 executes Scenario 3: terminal classification fast-fail.
func RunScenario3(ctx context.Context) error {
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

	rs := receiver.NewServer(
		receiver.WithStableDecisions("scenario-3"),
		receiver.WithRouteStatus("/hooks/misconfigured-404", 1.0, 404),
	)
	defer rs.Close()

	if err := seedScenario3Customer(ctx, db); err != nil {
		return fmt.Errorf("seed customer: %w", err)
	}
	if err := seedScenario3Webhooks(ctx, db, rs.URL()); err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}

	plainSched := roundrobin.New()
	reg := registry.New(db)
	ingestStore := ingest.New(db, reg, plainSched)
	outboxStore := outbox.New(db)

	startTime := time.Now()
	if err := submitScenario3Events(ctx, ingestStore); err != nil {
		return fmt.Errorf("submit events: %w", err)
	}

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	workers := scenarioWorkerCount()
	for i := range workers {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(plainSched, worker.WithLogger(workerLog))
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	slog.Info("started workers", "count", workers)

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
		select {
		case <-pollCtx.Done():
			return fmt.Errorf("deliveries not drained within timeout: %d pending", pending)
		case <-time.After(time.Second):
		}
	}

	cancelWorkers()
	workerWg.Wait()

	if err := writeScenario3Report(ctx, db, scenario3ReportPath, scenario3ReportParams{
		totalDuration: time.Since(startTime),
		workerCount:   workers,
	}); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("scenario 3 complete", "report", scenario3ReportPath)
	return nil
}

func seedScenario3Customer(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO customers (id, weight)
		VALUES ($1, 1)
		ON CONFLICT (id) DO NOTHING
	`, scenario3CustomerID)
	if err != nil {
		return fmt.Errorf("insert customer: %w", err)
	}
	return nil
}

func seedScenario3Webhooks(ctx context.Context, db *sql.DB, receiverBaseURL string) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, len(scenario3Webhooks)*4)
	for i, webhook := range scenario3Webhooks {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 4
		_, _ = fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", base+1, base+2, base+3, base+4)
		args = append(args,
			scenario3CustomerID,
			receiverBaseURL+webhook.Path,
			"",
			fmt.Sprintf("{%s}", eventType),
		)
	}
	sb.WriteString(" ON CONFLICT DO NOTHING")

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert webhooks: %w", err)
	}
	return nil
}

func submitScenario3Events(ctx context.Context, store *ingest.Store) error {
	for range scenario3EventCount {
		if _, err := store.Submit(ctx, ingest.Event{
			CustomerID: scenario3CustomerID,
			Type:       eventType,
			Payload:    []byte(`{"event_name":"subscriber.created"}`),
		}); err != nil {
			return fmt.Errorf("submit event: %w", err)
		}
	}
	return nil
}

type scenario3ReportParams struct {
	totalDuration time.Duration
	workerCount   int
}

func writeScenario3Report(ctx context.Context, db *sql.DB, path string, p scenario3ReportParams) error {
	totalDeliveries, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query total deliveries: %w", err)
	}
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query terminal deliveries: %w", err)
	}
	good, err := queryScenario3WebhookResult(ctx, db, "/hooks/good")
	if err != nil {
		return fmt.Errorf("query good webhook result: %w", err)
	}
	misconfigured, err := queryScenario3WebhookResult(ctx, db, "/hooks/misconfigured-404")
	if err != nil {
		return fmt.Errorf("query misconfigured webhook result: %w", err)
	}
	dlqRows, err := queryScenario3DLQReasons(ctx, db)
	if err != nil {
		return fmt.Errorf("query dlq reasons: %w", err)
	}
	attempts, err := queryScenario3AttemptsDistribution(ctx, db)
	if err != nil {
		return fmt.Errorf("query attempts distribution: %w", err)
	}
	outcomes, err := queryDeliveryOutcomes(ctx, db)
	if err != nil {
		return fmt.Errorf("query outcomes: %w", err)
	}

	totalOK := totalDeliveries == scenario3ExpectedDeliveries && terminal == scenario3ExpectedDeliveries
	goodOK := good.Total == scenario3EventCount && good.Succeeded == scenario3EventCount
	misconfiguredOK := misconfigured.Total == scenario3EventCount &&
		misconfigured.Dead == scenario3EventCount &&
		misconfigured.Terminal4xxDLQ == scenario3EventCount &&
		misconfigured.OneAttempt == scenario3EventCount &&
		misconfigured.RetryWaste == 0

	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Scenario 3: Terminal Classification Fast-Fail

## Description

1 Customer with 2 Webhooks: one healthy URL returning 2xx and one misconfigured URL returning 404. 1000 Events fan out to **2000 expected Deliveries**.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | %s |
| Events submitted | %d |
| Deliveries created | %d / %d |
| Workers | %d |
| Terminal Deliveries | %d / %d |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| 100%% Deliveries terminal | %d / %d | %d / %d | %s |
| Good Webhook Deliveries succeeded | %d / %d | %d / %d | %s |
| 404 Webhook Deliveries dead with terminal_4xx | %d / %d | %d / %d | %s |
| 404 Webhook retry waste | %d Deliveries with >1 Attempt | 0 | %s |

## Per-Webhook Attempts Per Delivery

| Webhook | Attempts | Deliveries |
|---|---:|---:|
`,
		p.totalDuration.Round(time.Second),
		scenario3EventCount,
		totalDeliveries, scenario3ExpectedDeliveries,
		p.workerCount,
		terminal, totalDeliveries,
		terminal, totalDeliveries, scenario3ExpectedDeliveries, scenario3ExpectedDeliveries, passFail(totalOK),
		good.Succeeded, good.Total, scenario3EventCount, scenario3EventCount, passFail(goodOK),
		misconfigured.Terminal4xxDLQ, misconfigured.Total, scenario3EventCount, scenario3EventCount, passFail(misconfiguredOK),
		misconfigured.RetryWaste, passFail(misconfigured.RetryWaste == 0),
	)

	for _, row := range attempts {
		_, _ = fmt.Fprintf(f, "| %s | %d | %d |\n", row.Webhook, row.Attempts, row.Count)
	}

	_, _ = fmt.Fprintf(f, "\n## DLQ Rows By Reason\n\n")
	_, _ = fmt.Fprintf(f, "| Reason | Rows |\n|---|---:|\n")
	for _, row := range dlqRows {
		_, _ = fmt.Fprintf(f, "| %s | %d |\n", row.Reason, row.Count)
	}

	_, _ = fmt.Fprintf(f, "\n## Delivery Outcomes\n\n")
	_, _ = fmt.Fprintf(f, "| Status | Count |\n|---|---:|\n")
	for _, o := range outcomes {
		_, _ = fmt.Fprintf(f, "| %s | %d |\n", o.Status, o.Count)
	}

	return f.Close()
}

type scenario3WebhookResult struct {
	Total          int
	Succeeded      int
	Dead           int
	OneAttempt     int
	RetryWaste     int
	Terminal4xxDLQ int
}

func queryScenario3WebhookResult(ctx context.Context, db *sql.DB, path string) (scenario3WebhookResult, error) {
	var r scenario3WebhookResult
	err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE d.status = 'succeeded'),
			COUNT(*) FILTER (WHERE d.status = 'dead'),
			COUNT(*) FILTER (WHERE d.attempt_count = 1),
			COUNT(*) FILTER (WHERE d.attempt_count > 1),
			COUNT(*) FILTER (WHERE dl.reason = $2)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		LEFT JOIN dead_letters dl ON dl.delivery_id = d.id
		WHERE w.url LIKE '%' || $1
	`, path, retry.ReasonTerminal4xx).Scan(
		&r.Total,
		&r.Succeeded,
		&r.Dead,
		&r.OneAttempt,
		&r.RetryWaste,
		&r.Terminal4xxDLQ,
	)
	if err != nil {
		return scenario3WebhookResult{}, fmt.Errorf("query webhook result: %w", err)
	}
	return r, nil
}

type scenario3AttemptsRow struct {
	Webhook  string
	Attempts int
	Count    int
}

func queryScenario3AttemptsDistribution(ctx context.Context, db *sql.DB) ([]scenario3AttemptsRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			CASE
				WHEN w.url LIKE '%/hooks/good' THEN 'good'
				WHEN w.url LIKE '%/hooks/misconfigured-404' THEN 'misconfigured-404'
				ELSE w.url
			END AS webhook,
			d.attempt_count,
			COUNT(*)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		GROUP BY webhook, d.attempt_count
		ORDER BY webhook, d.attempt_count
	`)
	if err != nil {
		return nil, fmt.Errorf("query attempts per webhook: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []scenario3AttemptsRow
	for rows.Next() {
		var r scenario3AttemptsRow
		if err := rows.Scan(&r.Webhook, &r.Attempts, &r.Count); err != nil {
			return nil, fmt.Errorf("scan attempts row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate attempts rows: %w", err)
	}
	return out, nil
}

type scenario3DLQReasonRow struct {
	Reason string
	Count  int
}

func queryScenario3DLQReasons(ctx context.Context, db *sql.DB) ([]scenario3DLQReasonRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT reason, COUNT(*)
		FROM dead_letters
		GROUP BY reason
		ORDER BY reason
	`)
	if err != nil {
		return nil, fmt.Errorf("query dlq reasons: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []scenario3DLQReasonRow
	for rows.Next() {
		var r scenario3DLQReasonRow
		if err := rows.Scan(&r.Reason, &r.Count); err != nil {
			return nil, fmt.Errorf("scan dlq reason row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dlq reason rows: %w", err)
	}
	return out, nil
}
