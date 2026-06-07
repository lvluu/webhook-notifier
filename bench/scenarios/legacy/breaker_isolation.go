// Package legacy contains issue-scoped benchmark scenarios kept for reference.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/bench/scenarios"
	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	breakerIsolationHealthyLatency           = 10 * time.Millisecond
	breakerIsolationControlSeed              = "scenario-5-control"
	breakerIsolationDeadSeed                 = "scenario-5-dead"
	breakerIsolationReportPath               = "bench/results/breaker-isolation.md"
	breakerIsolationDeadCustomerIndex        = 0
	breakerIsolationHealthyLatencyPathPrefix = "/hooks/customer-"
)

type breakerIsolationProfile struct {
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
	BreakerFailureThreshold int
	BreakerInitialCooldown  time.Duration
	BreakerMaxCooldown      time.Duration
}

func breakerIsolationProfileFromEnv() (breakerIsolationProfile, error) {
	raw, name, err := scenarios.LoadBenchProfileConfig("breaker_isolation")
	if err != nil {
		return breakerIsolationProfile{}, err
	}
	if raw.WorkerCount < 1 || raw.CustomerCount < 1 || raw.DeliveriesPerCustomer < 1 {
		return breakerIsolationProfile{}, fmt.Errorf("breaker_isolation profile %q has invalid positive counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 || raw.DiscoveryInterval.Duration() <= 0 || raw.DiscoveryEmptyBackoff.Duration() < 0 || raw.DiscoveryErrorBackoff.Duration() < 0 || raw.DiscoveryMaxBackoff.Duration() <= 0 {
		return breakerIsolationProfile{}, fmt.Errorf("breaker_isolation profile %q has invalid durations", name)
	}
	if raw.DiscoveryLimit < 1 || raw.BreakerFailureThreshold < 1 {
		return breakerIsolationProfile{}, fmt.Errorf("breaker_isolation profile %q has invalid limits", name)
	}
	if raw.BreakerInitialCooldown.Duration() <= 0 || raw.BreakerMaxCooldown.Duration() <= 0 {
		return breakerIsolationProfile{}, fmt.Errorf("breaker_isolation profile %q has invalid breaker cooldowns", name)
	}
	return breakerIsolationProfile{
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
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
		BreakerInitialCooldown:  raw.BreakerInitialCooldown.Duration(),
		BreakerMaxCooldown:      raw.BreakerMaxCooldown.Duration(),
	}, nil
}

func breakerIsolationCustomerID(i int) string {
	return fmt.Sprintf("breaker-isolation-customer-%02d", i+1)
}

func breakerIsolationCustomerIDs(count int) []string {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = breakerIsolationCustomerID(i)
	}
	return ids
}

type breakerIsolationBreakerSnapshot struct {
	State              string
	FailureCount       int
	ProbeDeliveryID    string
	ProbeDeliveryValid bool
	TripCount          float64
	OpenMetric         float64
	HalfOpenMetric     float64
	ClosedMetric       float64
}

type breakerIsolationRunResult struct {
	Name                    string
	Duration                time.Duration
	HealthyLatencies        []scenarios.BreakerIsolationCustomerLatency
	DeadAttempts            int
	DeadPending             int
	DeadState               string
	BaselineGoroutines      int
	FinalGoroutines         int
	BreakerOpenSnapshot     breakerIsolationBreakerSnapshot
	BreakerHalfOpenSnapshot breakerIsolationBreakerSnapshot
	BreakerFinalSnapshot    breakerIsolationBreakerSnapshot
}

// RunBreakerIsolation executes the legacy Breaker Isolation, folded into
// partner_resilience.go and kept for reference until retirement.
func RunBreakerIsolation(ctx context.Context) error {
	observe.RegisterMetrics(prometheus.NewRegistry())
	db, cleanup, err := scenarios.OpenBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := breakerIsolationProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	control, err := runBreakerIsolationPhase(ctx, db, profile, breakerIsolationPhaseConfig{
		name:         "control",
		receiverSeed: breakerIsolationControlSeed,
		deadPartner:  false,
	})
	if err != nil {
		return fmt.Errorf("run control phase: %w", err)
	}

	dead, err := runBreakerIsolationPhase(ctx, db, profile, breakerIsolationPhaseConfig{
		name:         "dead-partner",
		receiverSeed: breakerIsolationDeadSeed,
		deadPartner:  true,
	})
	if err != nil {
		return fmt.Errorf("run dead-partner phase: %w", err)
	}

	if err := writeBreakerIsolationReport(breakerIsolationReportPath, profile.Name, profile.CustomerCount, profile.DeliveriesPerCustomer, profile.BreakerFailureThreshold, control, dead); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("breaker isolation complete", "profile", profile.Name, "report", breakerIsolationReportPath)
	return nil
}

type breakerIsolationPhaseConfig struct {
	name         string
	receiverSeed string
	deadPartner  bool
}

func runBreakerIsolationPhase(ctx context.Context, db *sql.DB, profile breakerIsolationProfile, cfg breakerIsolationPhaseConfig) (breakerIsolationRunResult, error) {
	if err := scenarios.ResetBenchData(ctx, db); err != nil {
		return breakerIsolationRunResult{}, fmt.Errorf("reset bench data: %w", err)
	}
	slog.Info("running breaker isolation phase", "phase", cfg.name)

	rs, err := newBreakerIsolationReceiver(cfg, profile.CustomerCount)
	if err != nil {
		return breakerIsolationRunResult{}, err
	}
	defer rs.Close()

	if err := seedBreakerIsolationCustomers(ctx, db, profile.CustomerCount); err != nil {
		return breakerIsolationRunResult{}, fmt.Errorf("seed customers: %w", err)
	}
	if err := seedBreakerIsolationWebhooks(ctx, db, rs.URL(), profile.CustomerCount); err != nil {
		return breakerIsolationRunResult{}, fmt.Errorf("seed webhooks: %w", err)
	}
	if err := seedBreakerIsolationDeliveries(ctx, db, profile.CustomerCount, profile.DeliveriesPerCustomer); err != nil {
		return breakerIsolationRunResult{}, fmt.Errorf("seed deliveries: %w", err)
	}

	allCustomerIDs := breakerIsolationCustomerIDs(profile.CustomerCount)
	healthyCustomerIDs := allCustomerIDs[1:]
	deadCustomerID := allCustomerIDs[breakerIsolationDeadCustomerIndex]
	baselineGoroutines := runtime.NumGoroutine()

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
	workerCount := profile.WorkerCount
	for i := 0; i < workerCount; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(
				sched,
				worker.WithHTTPClient(httpClient),
				worker.WithRequestTimeout(profile.RequestTimeout),
				worker.WithRetryPolicy(retry.New(retry.Config{
					MaxAttempts: 20,
					BaseDelay:   50 * time.Millisecond,
					CapDelay:    50 * time.Millisecond,
				})),
				worker.WithLogger(workerLog),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "phase", cfg.name, "worker_id", id, "error", err)
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

	var result breakerIsolationRunResult
	result.Name = cfg.name
	result.BaselineGoroutines = baselineGoroutines

	if cfg.deadPartner {
		openSnap, halfOpenSnap, finalSnap, err := waitForBreakerIsolationBreakerTransitions(ctx, db, deadCustomerID, profile.BreakerFailureThreshold)
		if err != nil {
			cancelDiscovery()
			cancelWorkers()
			workerWg.Wait()
			<-discoveryDone
			return breakerIsolationRunResult{}, err
		}
		result.BreakerOpenSnapshot = openSnap
		result.BreakerHalfOpenSnapshot = halfOpenSnap
		result.BreakerFinalSnapshot = finalSnap
	}

	pollCtx, cancelPoll := context.WithTimeout(ctx, 20*time.Minute)
	defer cancelPoll()

	for {
		healthyPending, err := countPendingDeliveriesForCustomers(pollCtx, db, healthyCustomerIDs)
		if err != nil {
			cancelDiscovery()
			cancelWorkers()
			workerWg.Wait()
			<-discoveryDone
			return breakerIsolationRunResult{}, fmt.Errorf("count healthy pending deliveries: %w", err)
		}
		if healthyPending == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			cancelDiscovery()
			cancelWorkers()
			workerWg.Wait()
			<-discoveryDone
			return breakerIsolationRunResult{}, fmt.Errorf("healthy deliveries not drained within timeout: %d pending", healthyPending)
		case <-time.After(250 * time.Millisecond):
		}
	}

	cancelDiscovery()
	cancelWorkers()
	workerWg.Wait()
	<-discoveryDone

	rs.Close()
	time.Sleep(250 * time.Millisecond)

	result.Duration = time.Since(started)
	result.FinalGoroutines = runtime.NumGoroutine()

	latencies, err := queryHealthyCustomerLatencies(ctx, db, breakerIsolationCustomerID(breakerIsolationDeadCustomerIndex))
	if err != nil {
		return breakerIsolationRunResult{}, fmt.Errorf("query healthy customer latencies: %w", err)
	}
	result.HealthyLatencies = latencies

	if cfg.deadPartner {
		attempts, err := countDeliveryAttemptsForCustomer(ctx, db, deadCustomerID)
		if err != nil {
			return breakerIsolationRunResult{}, fmt.Errorf("count dead customer attempts: %w", err)
		}
		result.DeadAttempts = attempts

		pending, err := countPendingDeliveriesForCustomers(ctx, db, []string{deadCustomerID})
		if err != nil {
			return breakerIsolationRunResult{}, fmt.Errorf("count dead customer pending: %w", err)
		}
		result.DeadPending = pending
		result.DeadState = result.BreakerFinalSnapshot.State
	}

	return result, nil
}

func newBreakerIsolationReceiver(cfg breakerIsolationPhaseConfig, customerCount int) (*receiver.ReceiverServer, error) {
	opts := []receiver.Option{receiver.WithStableDecisions(cfg.receiverSeed)}
	for i := 0; i < customerCount; i++ {
		path := breakerIsolationWebhookPath(i)
		if cfg.deadPartner && i == breakerIsolationDeadCustomerIndex {
			opts = append(opts, receiver.WithRouteHang(path))
			continue
		}
		opts = append(opts, receiver.WithRouteLatency(path, 1.0, breakerIsolationHealthyLatency))
	}
	return receiver.NewServer(opts...), nil
}

func breakerIsolationWebhookPath(i int) string {
	return breakerIsolationHealthyLatencyPathPrefix + fmt.Sprintf("%02d", i+1)
}

func seedBreakerIsolationCustomers(ctx context.Context, db *sql.DB, customerCount int) error {
	ids := breakerIsolationCustomerIDs(customerCount)
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

func seedBreakerIsolationWebhooks(ctx context.Context, db *sql.DB, receiverBaseURL string, customerCount int) error {
	ids := breakerIsolationCustomerIDs(customerCount)
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
			receiverBaseURL+breakerIsolationWebhookPath(i),
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

func seedBreakerIsolationDeliveries(ctx context.Context, db *sql.DB, customerCount int, deliveriesPerCustomer int) error {
	for i, customerID := range breakerIsolationCustomerIDs(customerCount) {
		webhookID, err := breakerIsolationWebhookID(ctx, db, customerID)
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
		`, customerID, scenarios.EventType, []byte(fmt.Sprintf(`{"event_name":"bench.breaker_isolation","customer_index":%d}`, i+1))).Scan(&eventID)
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

func breakerIsolationWebhookID(ctx context.Context, db *sql.DB, customerID string) (string, error) {
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

func countPendingDeliveriesForCustomers(ctx context.Context, db *sql.DB, customerIDs []string) (int, error) {
	if len(customerIDs) == 0 {
		return 0, nil
	}
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM deliveries
		WHERE customer_id = ANY($1)
		  AND status NOT IN ('succeeded', 'dead')
	`, customerIDs).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending deliveries: %w", err)
	}
	return n, nil
}

func countDeliveryAttemptsForCustomer(ctx context.Context, db *sql.DB, customerID string) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM delivery_attempts da
		JOIN deliveries d ON d.id = da.delivery_id
		WHERE d.customer_id = $1
	`, customerID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count delivery attempts: %w", err)
	}
	return n, nil
}

func queryHealthyCustomerLatencies(ctx context.Context, db *sql.DB, deadCustomerID string) ([]scenarios.BreakerIsolationCustomerLatency, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			d.customer_id,
			COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0)
		FROM deliveries d
		WHERE d.customer_id <> $1
		  AND d.status = 'succeeded'
		GROUP BY d.customer_id
		ORDER BY d.customer_id
	`, deadCustomerID)
	if err != nil {
		return nil, fmt.Errorf("query healthy latencies: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var latencies []scenarios.BreakerIsolationCustomerLatency
	for rows.Next() {
		var row scenarios.BreakerIsolationCustomerLatency
		if err := rows.Scan(&row.CustomerID, &row.ControlP95); err != nil {
			return nil, fmt.Errorf("scan healthy latency: %w", err)
		}
		latencies = append(latencies, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate healthy latencies: %w", err)
	}
	return latencies, nil
}

func compareBreakerIsolationLatencies(control, dead []scenarios.BreakerIsolationCustomerLatency) ([]scenarios.BreakerIsolationCustomerLatency, bool, error) {
	if len(control) != len(dead) {
		return nil, false, fmt.Errorf("healthy latency rows mismatch: control=%d dead=%d", len(control), len(dead))
	}
	comparisons := make([]scenarios.BreakerIsolationCustomerLatency, len(control))
	ok := true
	for i := range control {
		if control[i].CustomerID != dead[i].CustomerID {
			return nil, false, fmt.Errorf("healthy latency row mismatch at %d: control=%s dead=%s", i, control[i].CustomerID, dead[i].CustomerID)
		}
		if control[i].ControlP95 == 0 {
			return nil, false, fmt.Errorf("control P95 is zero — cannot compare latencies for %s", control[i].CustomerID)
		}
		ratio := dead[i].ControlP95 / control[i].ControlP95
		if ratio > 1.10 {
			ok = false
		}
		comparisons[i] = scenarios.BreakerIsolationCustomerLatency{
			CustomerID: control[i].CustomerID,
			ControlP95: control[i].ControlP95,
			DeadP95:    dead[i].ControlP95,
			Ratio:      ratio,
		}
	}
	return comparisons, ok, nil
}

func snapshotBreakerIsolationBreakerMetrics(ctx context.Context, db *sql.DB, customerID string) (breakerIsolationBreakerSnapshot, error) {
	var snap breakerIsolationBreakerSnapshot
	var webhookID string
	err := db.QueryRowContext(ctx, `
		SELECT id::text, breaker_state, breaker_failure_count, COALESCE(breaker_probe_delivery_id::text, ''), breaker_probe_delivery_id IS NOT NULL
		FROM webhooks
		WHERE customer_id = $1
		ORDER BY created_at
		LIMIT 1
	`, customerID).Scan(&webhookID, &snap.State, &snap.FailureCount, &snap.ProbeDeliveryID, &snap.ProbeDeliveryValid)
	if err != nil {
		return breakerIsolationBreakerSnapshot{}, fmt.Errorf("query breaker snapshot: %w", err)
	}
	snap.OpenMetric = testutil.ToFloat64(observe.WebhookBreakerState.WithLabelValues(webhookID, outbox.BreakerStateOpen))
	snap.HalfOpenMetric = testutil.ToFloat64(observe.WebhookBreakerState.WithLabelValues(webhookID, outbox.BreakerStateHalfOpen))
	snap.ClosedMetric = testutil.ToFloat64(observe.WebhookBreakerState.WithLabelValues(webhookID, outbox.BreakerStateClosed))
	snap.TripCount = testutil.ToFloat64(observe.WebhookBreakerTripsTotal.WithLabelValues(retry.ReasonTimeout))
	return snap, nil
}

func waitForBreakerIsolationBreakerTransitions(ctx context.Context, db *sql.DB, customerID string, threshold int) (breakerIsolationBreakerSnapshot, breakerIsolationBreakerSnapshot, breakerIsolationBreakerSnapshot, error) {
	var openSnap, halfOpenSnap, finalSnap breakerIsolationBreakerSnapshot
	var haveOpen, haveHalfOpen, haveFinal bool

	totalTimeout, pollInterval := scenarios.BreakerIsolationBreakerPollingConfig()

	pollCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for {
		snap, err := snapshotBreakerIsolationBreakerMetrics(pollCtx, db, customerID)
		if err != nil {
			return breakerIsolationBreakerSnapshot{}, breakerIsolationBreakerSnapshot{}, breakerIsolationBreakerSnapshot{}, err
		}

		switch {
		case snap.State == outbox.BreakerStateOpen && snap.FailureCount >= threshold && !haveOpen:
			openSnap = snap
			haveOpen = true
		case snap.State == outbox.BreakerStateHalfOpen && snap.ProbeDeliveryValid && !haveHalfOpen:
			halfOpenSnap = snap
			haveHalfOpen = true
		case snap.State == outbox.BreakerStateOpen && snap.FailureCount > threshold && haveHalfOpen:
			finalSnap = snap
			haveFinal = true
		}

		if haveOpen && haveHalfOpen && haveFinal {
			return openSnap, halfOpenSnap, finalSnap, nil
		}

		select {
		case <-pollCtx.Done():
			return breakerIsolationBreakerSnapshot{}, breakerIsolationBreakerSnapshot{}, breakerIsolationBreakerSnapshot{}, fmt.Errorf("timed out waiting for breaker transitions: open=%v half_open=%v final=%v", haveOpen, haveHalfOpen, haveFinal)
		case <-time.After(pollInterval):
		}
	}
}

func writeBreakerIsolationReport(path, profileName string, customerCount, deliveriesPerCustomer, breakerThreshold int, control, dead breakerIsolationRunResult) error {
	comparisons, withinLimit, err := compareBreakerIsolationLatencies(control.HealthyLatencies, dead.HealthyLatencies)
	if err != nil {
		return err
	}

	goroutineDelta := dead.FinalGoroutines - dead.BaselineGoroutines
	goroutineOK := goroutineDelta <= 8
	deadAttemptsLimit := deliveriesPerCustomer / 10
	deadAttemptsOK := dead.DeadAttempts > 0 && dead.DeadAttempts <= deadAttemptsLimit
	skippedOK := dead.DeadPending > 0
	tripsOK := dead.BreakerOpenSnapshot.TripCount >= 1 && dead.BreakerFinalSnapshot.TripCount >= 2
	transitionsOK := dead.BreakerOpenSnapshot.State == outbox.BreakerStateOpen &&
		dead.BreakerHalfOpenSnapshot.State == outbox.BreakerStateHalfOpen &&
		dead.BreakerFinalSnapshot.State == outbox.BreakerStateOpen
	resultOK := withinLimit && goroutineOK && deadAttemptsOK && skippedOK && tripsOK && transitionsOK

	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Breaker Isolation: Dead Partner Circuit Breaker + Pool Protection

## Description

Profile **%s**: %d Customers each own one Webhook. Customer 1 points at a dead Integration Partner endpoint that hangs until the worker request timeout. Customers 2-%d point at a healthy 2xx receiver with 10ms latency. Each Customer gets %d Deliveries, for %d total Deliveries per run.

The benchmark runs two phases:

1. Control: all %d Customers point at healthy endpoints.
2. Dead Partner: Customer 1 is dead, the breaker opens after %d consecutive failures, then a single half-open probe is exercised after cooldown.

The dead-customer attempt count includes a small number of in-flight claims already in progress before the breaker state suppresses new work; the report caps that to keep the benchmark aligned with the worker's batch-claim behavior.

## Performance Summary

| Phase | Duration | Baseline Goroutines | Final Goroutines | Dead Customer Attempts | Dead Customer Pending |
|---|---:|---:|---:|---:|---:|
| Control | %s | %d | %d | - | - |
| Dead Partner | %s | %d | %d | %d | %d |

## Healthy Customer p95

| Customer | Control p95 | Dead Partner p95 | Ratio |
|---|---:|---:|---:|
`, profileName, customerCount, customerCount, deliveriesPerCustomer, customerCount*deliveriesPerCustomer,
		customerCount, breakerThreshold,
		control.Duration.Round(time.Millisecond),
		control.BaselineGoroutines,
		control.FinalGoroutines,
		dead.Duration.Round(time.Millisecond),
		dead.BaselineGoroutines,
		dead.FinalGoroutines,
		dead.DeadAttempts,
		dead.DeadPending,
	)

	for _, row := range comparisons {
		_, _ = fmt.Fprintf(f, "| %s | %.2f ms | %.2f ms | %.2f× |\n", row.CustomerID, row.ControlP95, row.DeadP95, row.Ratio)
	}

	_, _ = fmt.Fprintf(f, `
## Breaker Snapshots

| Snapshot | State | Failure Count | Probe Delivery | Breaker Open Metric | Breaker Half-Open Metric | Breaker Closed Metric | Timeout Trips |
|---|---|---:|---|---:|---:|---:|---:|
| Open | %s | %d | %s | %.0f | %.0f | %.0f | %.0f |
| Half-open | %s | %d | %s | %.0f | %.0f | %.0f | %.0f |
| Final | %s | %d | %s | %.0f | %.0f | %.0f | %.0f |

## Acceptance Criteria

| Pillar | Criterion | Observed | Limit | Result |
|---|---|---:|---:|---|
| Fairness | Healthy Customer p95 within 10%% of control | %s | <= 1.10x | %s |
| Reliability | Customer 1 breaker opens after threshold | %d failures | >= %d | %s |
| Reliability | Customer 1 half-open probe observed | %s | true | %s |
| Reliability | Customer 1 deliveries skipped after open | %d pending | > 0 | %s |
| Reliability | Customer 1 dead-customer attempts before breaker hold | %d attempts | <= %d | %s |
| Reliability | Breaker trip metric recorded | %.0f | >= 1 | %s |
| Reliability | Goroutine count returned near baseline | %d delta | <= 8 | %s |
| Reliability | Overall result | %s | PASS | %s |
`,
		dead.BreakerOpenSnapshot.State,
		dead.BreakerOpenSnapshot.FailureCount,
		probeID(dead.BreakerOpenSnapshot),
		dead.BreakerOpenSnapshot.OpenMetric,
		dead.BreakerOpenSnapshot.HalfOpenMetric,
		dead.BreakerOpenSnapshot.ClosedMetric,
		dead.BreakerOpenSnapshot.TripCount,
		dead.BreakerHalfOpenSnapshot.State,
		dead.BreakerHalfOpenSnapshot.FailureCount,
		probeID(dead.BreakerHalfOpenSnapshot),
		dead.BreakerHalfOpenSnapshot.OpenMetric,
		dead.BreakerHalfOpenSnapshot.HalfOpenMetric,
		dead.BreakerHalfOpenSnapshot.ClosedMetric,
		dead.BreakerHalfOpenSnapshot.TripCount,
		dead.BreakerFinalSnapshot.State,
		dead.BreakerFinalSnapshot.FailureCount,
		probeID(dead.BreakerFinalSnapshot),
		dead.BreakerFinalSnapshot.OpenMetric,
		dead.BreakerFinalSnapshot.HalfOpenMetric,
		dead.BreakerFinalSnapshot.ClosedMetric,
		dead.BreakerFinalSnapshot.TripCount,
		scenarios.PassFail(withinLimit),
		scenarios.PassFail(withinLimit),
		dead.BreakerOpenSnapshot.FailureCount,
		breakerThreshold,
		scenarios.PassFail(dead.BreakerOpenSnapshot.FailureCount >= breakerThreshold),
		scenarios.PassFail(dead.BreakerHalfOpenSnapshot.ProbeDeliveryValid),
		scenarios.PassFail(dead.BreakerHalfOpenSnapshot.ProbeDeliveryValid),
		dead.DeadPending,
		scenarios.PassFail(skippedOK),
		dead.DeadAttempts,
		deadAttemptsLimit,
		scenarios.PassFail(deadAttemptsOK),
		dead.BreakerFinalSnapshot.TripCount,
		scenarios.PassFail(tripsOK),
		goroutineDelta,
		scenarios.PassFail(goroutineOK),
		scenarios.PassFail(resultOK),
		scenarios.PassFail(resultOK),
	)

	return f.Close()
}

func probeID(snap breakerIsolationBreakerSnapshot) string {
	if snap.ProbeDeliveryValid && snap.ProbeDeliveryID != "" {
		return snap.ProbeDeliveryID
	}
	return "-"
}
