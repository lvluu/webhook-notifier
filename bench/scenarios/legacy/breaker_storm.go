package legacy

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/bench/scenarios"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	breakerStormReportPath          = "bench/results/breaker-storm.md"
	breakerStormWebhookPathPrefix   = "/hooks/breaker-storm-customer-"
	breakerStormBreakerPollInterval = 10 * time.Millisecond
	breakerStormBreakerPollTimeout  = 5 * time.Minute
)

type breakerStormProfile struct {
	Name                    string
	WorkerCount             int
	CustomerCount           int
	DeliveriesPerCustomer   int
	RequestTimeout          time.Duration
	DiscoveryInterval       time.Duration
	DiscoveryEmptyBackoff   time.Duration
	DiscoveryErrorBackoff   time.Duration
	DiscoveryMaxBackoff     time.Duration
	DiscoveryLimit          int
	MinBatchHint            int
	BreakerFailureThreshold int
	BreakerInitialCooldown  time.Duration
	BreakerMaxCooldown      time.Duration
	RetryMaxAttempts        int
	RetryBaseDelay          time.Duration
	RetryCapDelay           time.Duration
}

func breakerStormProfileFromEnv() (breakerStormProfile, error) {
	raw, name, err := scenarios.LoadBenchProfileConfig("breaker_storm")
	if err != nil {
		return breakerStormProfile{}, err
	}
	if raw.WorkerCount < 1 || raw.CustomerCount < 1 || raw.DeliveriesPerCustomer < 1 {
		return breakerStormProfile{}, fmt.Errorf("breaker_storm profile %q has invalid positive counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 || raw.DiscoveryInterval.Duration() <= 0 ||
		raw.DiscoveryMaxBackoff.Duration() <= 0 {
		return breakerStormProfile{}, fmt.Errorf("breaker_storm profile %q has invalid durations", name)
	}
	if raw.DiscoveryLimit < 1 || raw.BreakerFailureThreshold < 1 || raw.MinBatchHint < 1 {
		return breakerStormProfile{}, fmt.Errorf("breaker_storm profile %q has invalid limits", name)
	}
	if raw.BreakerInitialCooldown.Duration() <= 0 || raw.BreakerMaxCooldown.Duration() <= 0 {
		return breakerStormProfile{}, fmt.Errorf("breaker_storm profile %q has invalid breaker cooldowns", name)
	}
	if raw.RetryMaxAttempts < 1 || raw.RetryBaseDelay.Duration() <= 0 || raw.RetryCapDelay.Duration() <= 0 {
		return breakerStormProfile{}, fmt.Errorf("breaker_storm profile %q has invalid retry config", name)
	}
	return breakerStormProfile{
		Name:                    name,
		WorkerCount:             raw.WorkerCount,
		CustomerCount:           raw.CustomerCount,
		DeliveriesPerCustomer:   raw.DeliveriesPerCustomer,
		RequestTimeout:          raw.RequestTimeout.Duration(),
		DiscoveryInterval:       raw.DiscoveryInterval.Duration(),
		DiscoveryEmptyBackoff:   raw.DiscoveryEmptyBackoff.Duration(),
		DiscoveryErrorBackoff:   raw.DiscoveryErrorBackoff.Duration(),
		DiscoveryMaxBackoff:     raw.DiscoveryMaxBackoff.Duration(),
		DiscoveryLimit:          raw.DiscoveryLimit,
		MinBatchHint:            raw.MinBatchHint,
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
		BreakerInitialCooldown:  raw.BreakerInitialCooldown.Duration(),
		BreakerMaxCooldown:      raw.BreakerMaxCooldown.Duration(),
		RetryMaxAttempts:        raw.RetryMaxAttempts,
		RetryBaseDelay:          raw.RetryBaseDelay.Duration(),
		RetryCapDelay:           raw.RetryCapDelay.Duration(),
	}, nil
}

func breakerStormCustomerID(i int) string {
	return fmt.Sprintf("breaker-storm-customer-%02d", i+1)
}

func breakerStormCustomerIDs(count int) []string {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = breakerStormCustomerID(i)
	}
	return ids
}

type breakerStormResult struct {
	Duration           time.Duration
	BaselineGoroutines int
	FinalGoroutines    int
	CustomerCount      int
	DeliveriesPerCust  int
	TotalAttempts      int
	TotalPending       int
	TotalSucceeded     int
	TotalDead          int
	BreakerSnapshots   []breakerIsolationBreakerSnapshot
}

// RunBreakerStorm executes the internal hardening Breaker Storm, which is not part
// of the assignment-facing proof map. All webhooks return 5xx, workers claim
// batches of deliveries per Customer, and the batch breaker bookkeeping path
// (MarkRetryBatch → batchRecordBreakerFailure) handles every failure in O(1)
// set-based SQL instead of N+1 per-item queries. The scenario verifies that
// all breakers open correctly under mass downstream outage.
func RunBreakerStorm(ctx context.Context) error {
	db, cleanup, err := scenarios.OpenBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := breakerStormProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	result, err := runBreakerStormPhase(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("run breaker storm: %w", err)
	}

	if err := writeBreakerStormReport(breakerStormReportPath, profile, result); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("breaker storm complete", "profile", profile.Name, "report", breakerStormReportPath)
	return nil
}

func runBreakerStormPhase(ctx context.Context, db *sql.DB, profile breakerStormProfile) (breakerStormResult, error) {
	if err := scenarios.ResetBenchData(ctx, db); err != nil {
		return breakerStormResult{}, fmt.Errorf("reset bench data: %w", err)
	}

	slog.Info("running breaker storm", "customers", profile.CustomerCount,
		"deliveries_per_customer", profile.DeliveriesPerCustomer,
		"workers", profile.WorkerCount, "min_batch_hint", profile.MinBatchHint)

	baselineGoroutines := runtime.NumGoroutine()

	rs, err := newBreakerStormReceiver(profile.CustomerCount)
	if err != nil {
		return breakerStormResult{}, err
	}
	defer rs.Close()

	allCustomerIDs := breakerStormCustomerIDs(profile.CustomerCount)

	if err := seedBreakerStormCustomers(ctx, db, profile.CustomerCount); err != nil {
		return breakerStormResult{}, fmt.Errorf("seed customers: %w", err)
	}
	if err := seedBreakerStormWebhooks(ctx, db, rs.URL(), profile.CustomerCount); err != nil {
		return breakerStormResult{}, fmt.Errorf("seed webhooks: %w", err)
	}
	if err := seedBreakerStormDeliveries(ctx, db, profile.CustomerCount, profile.DeliveriesPerCustomer); err != nil {
		return breakerStormResult{}, fmt.Errorf("seed deliveries: %w", err)
	}

	sched := roundrobin.New()
	outboxStore := outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: profile.BreakerFailureThreshold,
		InitialCooldown:  profile.BreakerInitialCooldown,
		MaxCooldown:      profile.BreakerMaxCooldown,
	}))
	httpClient := worker.NewHTTPClient(worker.DefaultHTTPConfig())
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	defer cancelDiscovery()

	var workerWg sync.WaitGroup
	for i := 0; i < profile.WorkerCount; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(
				sched,
				worker.WithHTTPClient(httpClient),
				worker.WithRequestTimeout(profile.RequestTimeout),
				worker.WithRetryPolicy(retry.New(retry.Config{
					MaxAttempts: profile.RetryMaxAttempts,
					BaseDelay:   profile.RetryBaseDelay,
					CapDelay:    profile.RetryCapDelay,
				})),
				worker.WithLogger(workerLog),
				worker.WithMinBatchHint(profile.MinBatchHint),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "scenario", "7", "worker_id", id, "error", err)
			}
		}(i)
	}

	discoveryDone := make(chan error, 1)
	go func() {
		discoveryDone <- worker.RunClaimableCustomerDiscovery(discoveryCtx, outboxStore, sched, worker.DiscoveryConfig{
			Interval:     profile.DiscoveryInterval,
			EmptyBackoff: profile.DiscoveryEmptyBackoff,
			ErrorBackoff: profile.DiscoveryErrorBackoff,
			MaxBackoff:   profile.DiscoveryMaxBackoff,
			Limit:        profile.DiscoveryLimit,
		})
	}()

	started := time.Now()

	breakersOpened, err := waitForAllBreakerStormBreakers(ctx, db, allCustomerIDs, profile.BreakerFailureThreshold)
	if err != nil {
		cancelDiscovery()
		cancelWorkers()
		workerWg.Wait()
		<-discoveryDone
		return breakerStormResult{}, err
	}

	time.Sleep(500 * time.Millisecond)

	cancelDiscovery()
	cancelWorkers()
	workerWg.Wait()
	<-discoveryDone

	rs.Close()
	time.Sleep(250 * time.Millisecond)

	result := breakerStormResult{
		Duration:           time.Since(started),
		BaselineGoroutines: baselineGoroutines,
		FinalGoroutines:    runtime.NumGoroutine(),
		CustomerCount:      profile.CustomerCount,
		DeliveriesPerCust:  profile.DeliveriesPerCustomer,
		BreakerSnapshots:   breakersOpened,
	}

	totalAttempts, err := countAllDeliveryAttempts(ctx, db)
	if err != nil {
		return breakerStormResult{}, fmt.Errorf("count total attempts: %w", err)
	}
	result.TotalAttempts = totalAttempts

	totalPending, err := countDeliveriesPendingOrRetry(ctx, db)
	if err != nil {
		return breakerStormResult{}, fmt.Errorf("count pending deliveries: %w", err)
	}
	result.TotalPending = totalPending

	totalSucceeded, err := countDeliveriesByStatus(ctx, db, "succeeded")
	if err != nil {
		return breakerStormResult{}, fmt.Errorf("count succeeded deliveries: %w", err)
	}
	result.TotalSucceeded = totalSucceeded

	totalDead, err := countDeliveriesByStatus(ctx, db, "dead")
	if err != nil {
		return breakerStormResult{}, fmt.Errorf("count dead deliveries: %w", err)
	}
	result.TotalDead = totalDead

	return result, nil
}

func newBreakerStormReceiver(customerCount int) (*receiver.ReceiverServer, error) {
	opts := []receiver.Option{receiver.WithStableDecisions("breaker-storm")}
	for i := 0; i < customerCount; i++ {
		path := breakerStormWebhookPath(i)
		opts = append(opts, receiver.WithRouteStatus(path, 1.0, 500))
	}
	return receiver.NewServer(opts...), nil
}

func breakerStormWebhookPath(i int) string {
	return fmt.Sprintf("%s%02d", breakerStormWebhookPathPrefix, i+1)
}

func seedBreakerStormCustomers(ctx context.Context, db *sql.DB, customerCount int) error {
	ids := breakerStormCustomerIDs(customerCount)
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, len(ids)*2)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d)", i*2+1, i*2+2)
		args = append(args, id, 1)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert customers: %w", err)
	}
	return nil
}

func seedBreakerStormWebhooks(ctx context.Context, db *sql.DB, receiverBaseURL string, customerCount int) error {
	ids := breakerStormCustomerIDs(customerCount)
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, len(ids)*4)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", i*4+1, i*4+2, i*4+3, i*4+4)
		args = append(args,
			id,
			receiverBaseURL+breakerStormWebhookPath(i),
			"",
			fmt.Sprintf("{%s}", scenarios.EventType),
		)
	}
	sb.WriteString(" ON CONFLICT DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert webhooks: %w", err)
	}
	return nil
}

func seedBreakerStormDeliveries(ctx context.Context, db *sql.DB, customerCount, deliveriesPerCustomer int) error {
	for _, customerID := range breakerStormCustomerIDs(customerCount) {
		webhookID, err := breakerStormWebhookID(ctx, db, customerID)
		if err != nil {
			return err
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", customerID, err)
		}

		var eventID string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO events (customer_id, type, payload)
			VALUES ($1, $2, $3)
			RETURNING id
		`, customerID, scenarios.EventType, []byte(`{"scenario":"7"}`)).Scan(&eventID)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert event for %s: %w", customerID, err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
			SELECT $1, $2, $3, 'pending', 0, now()
			FROM generate_series(1, $4)
		`, eventID, webhookID, customerID, deliveriesPerCustomer)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert deliveries for %s: %w", customerID, err)
		}

		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("commit deliveries for %s: %w", customerID, err)
		}
	}
	return nil
}

func breakerStormWebhookID(ctx context.Context, db *sql.DB, customerID string) (string, error) {
	var id string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM webhooks
		WHERE customer_id = $1
		ORDER BY created_at
		LIMIT 1
	`, customerID).Scan(&id); err != nil {
		return "", fmt.Errorf("lookup webhook for %s: %w", customerID, err)
	}
	return id, nil
}

func breakerStormBreakerSnapshot(ctx context.Context, db *sql.DB, customerID string) (string, string, int, error) {
	var webhookID, state string
	var failureCount int
	if err := db.QueryRowContext(ctx, `
		SELECT id::text, breaker_state, breaker_failure_count
		FROM webhooks
		WHERE customer_id = $1
		ORDER BY created_at
		LIMIT 1
	`, customerID).Scan(&webhookID, &state, &failureCount); err != nil {
		return "", "", 0, fmt.Errorf("query breaker for %s: %w", customerID, err)
	}
	return webhookID, state, failureCount, nil
}

func waitForAllBreakerStormBreakers(ctx context.Context, db *sql.DB, customerIDs []string, threshold int) ([]breakerIsolationBreakerSnapshot, error) {
	pollCtx, cancel := context.WithTimeout(ctx, breakerStormBreakerPollTimeout)
	defer cancel()

	remaining := make(map[string]bool, len(customerIDs))
	for _, id := range customerIDs {
		remaining[id] = true
	}

	snapshots := make(map[string]breakerIsolationBreakerSnapshot, len(customerIDs))

	for {
		for customerID := range remaining {
			_, state, failureCount, err := breakerStormBreakerSnapshot(pollCtx, db, customerID)
			if err != nil {
				return nil, err
			}
			if state == outbox.BreakerStateOpen && failureCount >= threshold {
				delete(remaining, customerID)
				snapshots[customerID] = breakerIsolationBreakerSnapshot{
					State:        state,
					FailureCount: failureCount,
				}
			}
		}

		if len(remaining) == 0 {
			break
		}

		select {
		case <-pollCtx.Done():
			var missing []string
			for id := range remaining {
				missing = append(missing, id)
			}
			return nil, fmt.Errorf("timed out waiting for %d/%d breakers to open: %v", len(remaining), len(customerIDs), missing)
		case <-time.After(breakerStormBreakerPollInterval):
		}
	}

	result := make([]breakerIsolationBreakerSnapshot, 0, len(snapshots))
	for _, s := range snapshots {
		result = append(result, s)
	}
	return result, nil
}

func countDeliveriesPendingOrRetry(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM deliveries
		WHERE status NOT IN ('succeeded', 'dead')
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending or retry: %w", err)
	}
	return n, nil
}

func countDeliveriesByStatus(ctx context.Context, db *sql.DB, status string) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries WHERE status = $1
	`, status).Scan(&n); err != nil {
		return 0, fmt.Errorf("count deliveries by status %s: %w", status, err)
	}
	return n, nil
}

func countAllDeliveryAttempts(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count delivery attempts: %w", err)
	}
	return n, nil
}

func writeBreakerStormReport(path string, profile breakerStormProfile, result breakerStormResult) error {
	goroutineDelta := result.FinalGoroutines - result.BaselineGoroutines
	goroutineOK := goroutineDelta <= 8
	totalDeliveries := result.CustomerCount * result.DeliveriesPerCust
	allBreakersOpen := len(result.BreakerSnapshots) == result.CustomerCount
	minFailureCount := 0
	if len(result.BreakerSnapshots) > 0 {
		minFailureCount = result.BreakerSnapshots[0].FailureCount
		for _, s := range result.BreakerSnapshots {
			if s.FailureCount < minFailureCount {
				minFailureCount = s.FailureCount
			}
		}
	}
	resultOK := allBreakersOpen && minFailureCount >= profile.BreakerFailureThreshold && goroutineOK

	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Breaker Storm: Mass Breaker Tripping Under Batch Mark

## Description

Profile **%s**: %d Customers each own one Webhook. All webhooks point at a
500-ing Integration Partner endpoint, simulating a downstream outage.
Workers claim batches of up to **%d** deliveries per customer and mark
them concurrently. Each batch of failures exercises the O(1) batch breaker
bookkeeping path (`+"`batchRecordBreakerFailure`"+`), which replaces the
N+1 per-item query pattern with set-based SQL.

The breaker opens after **%d** consecutive 5xx responses.

| Metric | Value |
|---|---|
| Customers | %d |
| Deliveries per Customer | %d |
| Total Deliveries | %d |
| Workers | %d |
| Min Batch Hint | %d |
| Total Attempts | %d |
| Duration | %s |
| Baseline Goroutines | %d |
| Final Goroutines | %d |
| Goroutine Delta | %d |

## Delivery Outcomes

| Status | Count |
|---|---|
| Pending / Retry (skipped by breaker) | %d |
| Succeeded | %d |
| Dead | %d |

## Breaker States

| State | Count |
|---|---|
| Open | %d / %d |
| Half-Open | 0 / %d |
| Closed | 0 / %d |
| Min failure_count across open breakers | %d |

## Acceptance Criteria

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| All breakers opened | %d / %d | %d | %s |
| Breaker failure_count >= threshold | %d | >= %d | %s |
| Goroutine count stable | %d delta | <= 8 | %s |
| Overall result | %s | PASS | %s |
`,
		profile.Name,
		result.CustomerCount,
		profile.MinBatchHint,
		profile.BreakerFailureThreshold,
		result.CustomerCount,
		result.DeliveriesPerCust,
		totalDeliveries,
		profile.WorkerCount,
		profile.MinBatchHint,
		result.TotalAttempts,
		result.Duration.Round(time.Millisecond),
		result.BaselineGoroutines,
		result.FinalGoroutines,
		goroutineDelta,
		result.TotalPending,
		result.TotalSucceeded,
		result.TotalDead,
		len(result.BreakerSnapshots),
		result.CustomerCount,
		result.CustomerCount,
		result.CustomerCount,
		minFailureCount,
		len(result.BreakerSnapshots),
		result.CustomerCount,
		result.CustomerCount,
		scenarios.PassFail(allBreakersOpen),
		minFailureCount,
		profile.BreakerFailureThreshold,
		scenarios.PassFail(minFailureCount >= profile.BreakerFailureThreshold),
		goroutineDelta,
		scenarios.PassFail(goroutineOK),
		scenarios.PassFail(resultOK),
		scenarios.PassFail(resultOK),
	)

	return f.Close()
}
