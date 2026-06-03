package scenarios

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
	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	scenario5HealthyLatency           = 10 * time.Millisecond
	scenario5ControlSeed              = "scenario-5-control"
	scenario5DeadSeed                 = "scenario-5-dead"
	scenario5ReportPath               = "bench/results/scenario-5.md"
	scenario5DeadCustomerIndex        = 0
	scenario5HealthyLatencyPathPrefix = "/hooks/customer-"
)

type scenario5Profile struct {
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

func scenario5ProfileFromEnv() (scenario5Profile, error) {
	raw, name, err := loadBenchProfileConfig("scenario5")
	if err != nil {
		return scenario5Profile{}, err
	}
	if raw.WorkerCount < 1 || raw.CustomerCount < 1 || raw.DeliveriesPerCustomer < 1 {
		return scenario5Profile{}, fmt.Errorf("scenario5 profile %q has invalid positive counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 || raw.DiscoveryInterval.Duration() <= 0 || raw.DiscoveryEmptyBackoff.Duration() < 0 || raw.DiscoveryErrorBackoff.Duration() < 0 || raw.DiscoveryMaxBackoff.Duration() <= 0 {
		return scenario5Profile{}, fmt.Errorf("scenario5 profile %q has invalid durations", name)
	}
	if raw.DiscoveryLimit < 1 || raw.BreakerFailureThreshold < 1 {
		return scenario5Profile{}, fmt.Errorf("scenario5 profile %q has invalid limits", name)
	}
	if raw.BreakerInitialCooldown.Duration() <= 0 || raw.BreakerMaxCooldown.Duration() <= 0 {
		return scenario5Profile{}, fmt.Errorf("scenario5 profile %q has invalid breaker cooldowns", name)
	}
	return scenario5Profile{
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

func scenario5CustomerID(i int) string {
	return fmt.Sprintf("scenario-5-customer-%02d", i+1)
}

func scenario5CustomerIDs(count int) []string {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = scenario5CustomerID(i)
	}
	return ids
}

type scenario5CustomerLatency struct {
	CustomerID string
	ControlP95 float64
	DeadP95    float64
	Ratio      float64
}

type scenario5BreakerSnapshot struct {
	State              string
	FailureCount       int
	ProbeDeliveryID    string
	ProbeDeliveryValid bool
	TripCount          float64
	OpenMetric         float64
	HalfOpenMetric     float64
	ClosedMetric       float64
}

type scenario5RunResult struct {
	Name                    string
	Duration                time.Duration
	HealthyLatencies        []scenario5CustomerLatency
	DeadAttempts            int
	DeadPending             int
	DeadState               string
	BaselineGoroutines      int
	FinalGoroutines         int
	BreakerOpenSnapshot     scenario5BreakerSnapshot
	BreakerHalfOpenSnapshot scenario5BreakerSnapshot
	BreakerFinalSnapshot    scenario5BreakerSnapshot
}

// Legacy scenario — folded into partner_resilience.go. Kept for reference until retirement.
// RunScenario5 executes Scenario 5: a dead partner that trips the breaker
// without degrading healthy Customers.
func RunScenario5(ctx context.Context) error {
	observe.RegisterMetrics(prometheus.NewRegistry())
	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := scenario5ProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	control, err := runScenario5Phase(ctx, db, profile, scenario5PhaseConfig{
		name:         "control",
		receiverSeed: scenario5ControlSeed,
		deadPartner:  false,
	})
	if err != nil {
		return fmt.Errorf("run control phase: %w", err)
	}

	dead, err := runScenario5Phase(ctx, db, profile, scenario5PhaseConfig{
		name:         "dead-partner",
		receiverSeed: scenario5DeadSeed,
		deadPartner:  true,
	})
	if err != nil {
		return fmt.Errorf("run dead-partner phase: %w", err)
	}

	if err := writeScenario5Report(scenario5ReportPath, profile.Name, profile.CustomerCount, profile.DeliveriesPerCustomer, profile.BreakerFailureThreshold, control, dead); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("scenario 5 complete", "profile", profile.Name, "report", scenario5ReportPath)
	return nil
}

type scenario5PhaseConfig struct {
	name         string
	receiverSeed string
	deadPartner  bool
}

func runScenario5Phase(ctx context.Context, db *sql.DB, profile scenario5Profile, cfg scenario5PhaseConfig) (scenario5RunResult, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return scenario5RunResult{}, fmt.Errorf("reset bench data: %w", err)
	}
	slog.Info("running scenario 5 phase", "phase", cfg.name)

	rs, err := newScenario5Receiver(cfg, profile.CustomerCount)
	if err != nil {
		return scenario5RunResult{}, err
	}
	defer rs.Close()

	if err := seedScenario5Customers(ctx, db, profile.CustomerCount); err != nil {
		return scenario5RunResult{}, fmt.Errorf("seed customers: %w", err)
	}
	if err := seedScenario5Webhooks(ctx, db, rs.URL(), profile.CustomerCount); err != nil {
		return scenario5RunResult{}, fmt.Errorf("seed webhooks: %w", err)
	}
	if err := seedScenario5Deliveries(ctx, db, profile.CustomerCount, profile.DeliveriesPerCustomer); err != nil {
		return scenario5RunResult{}, fmt.Errorf("seed deliveries: %w", err)
	}

	allCustomerIDs := scenario5CustomerIDs(profile.CustomerCount)
	healthyCustomerIDs := allCustomerIDs[1:]
	deadCustomerID := allCustomerIDs[scenario5DeadCustomerIndex]
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

	var result scenario5RunResult
	result.Name = cfg.name
	result.BaselineGoroutines = baselineGoroutines

	if cfg.deadPartner {
		openSnap, halfOpenSnap, finalSnap, err := waitForScenario5BreakerTransitions(ctx, db, deadCustomerID, profile.BreakerFailureThreshold)
		if err != nil {
			cancelDiscovery()
			cancelWorkers()
			workerWg.Wait()
			<-discoveryDone
			return scenario5RunResult{}, err
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
			return scenario5RunResult{}, fmt.Errorf("count healthy pending deliveries: %w", err)
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
			return scenario5RunResult{}, fmt.Errorf("healthy deliveries not drained within timeout: %d pending", healthyPending)
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

	latencies, err := queryHealthyCustomerLatencies(ctx, db, scenario5CustomerID(scenario5DeadCustomerIndex))
	if err != nil {
		return scenario5RunResult{}, fmt.Errorf("query healthy customer latencies: %w", err)
	}
	result.HealthyLatencies = latencies

	if cfg.deadPartner {
		attempts, err := countDeliveryAttemptsForCustomer(ctx, db, deadCustomerID)
		if err != nil {
			return scenario5RunResult{}, fmt.Errorf("count dead customer attempts: %w", err)
		}
		result.DeadAttempts = attempts

		pending, err := countPendingDeliveriesForCustomers(ctx, db, []string{deadCustomerID})
		if err != nil {
			return scenario5RunResult{}, fmt.Errorf("count dead customer pending: %w", err)
		}
		result.DeadPending = pending
		result.DeadState = result.BreakerFinalSnapshot.State
	}

	return result, nil
}

func newScenario5Receiver(cfg scenario5PhaseConfig, customerCount int) (*receiver.ReceiverServer, error) {
	opts := []receiver.Option{receiver.WithStableDecisions(cfg.receiverSeed)}
	for i := 0; i < customerCount; i++ {
		path := scenario5WebhookPath(i)
		if cfg.deadPartner && i == scenario5DeadCustomerIndex {
			opts = append(opts, receiver.WithRouteHang(path))
			continue
		}
		opts = append(opts, receiver.WithRouteLatency(path, 1.0, scenario5HealthyLatency))
	}
	return receiver.NewServer(opts...), nil
}

func scenario5WebhookPath(i int) string {
	return scenario5HealthyLatencyPathPrefix + fmt.Sprintf("%02d", i+1)
}

func seedScenario5Customers(ctx context.Context, db *sql.DB, customerCount int) error {
	ids := scenario5CustomerIDs(customerCount)
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

func seedScenario5Webhooks(ctx context.Context, db *sql.DB, receiverBaseURL string, customerCount int) error {
	ids := scenario5CustomerIDs(customerCount)
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
			receiverBaseURL+scenario5WebhookPath(i),
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

func seedScenario5Deliveries(ctx context.Context, db *sql.DB, customerCount int, deliveriesPerCustomer int) error {
	for i, customerID := range scenario5CustomerIDs(customerCount) {
		webhookID, err := scenario5WebhookID(ctx, db, customerID)
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
		`, customerID, eventType, []byte(fmt.Sprintf(`{"event_name":"bench.scenario5","customer_index":%d}`, i+1))).Scan(&eventID)
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

func scenario5WebhookID(ctx context.Context, db *sql.DB, customerID string) (string, error) {
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

func queryHealthyCustomerLatencies(ctx context.Context, db *sql.DB, deadCustomerID string) ([]scenario5CustomerLatency, error) {
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

	var latencies []scenario5CustomerLatency
	for rows.Next() {
		var row scenario5CustomerLatency
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

func compareScenario5Latencies(control, dead []scenario5CustomerLatency) ([]scenario5CustomerLatency, bool, error) {
	if len(control) != len(dead) {
		return nil, false, fmt.Errorf("healthy latency rows mismatch: control=%d dead=%d", len(control), len(dead))
	}
	comparisons := make([]scenario5CustomerLatency, len(control))
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
		comparisons[i] = scenario5CustomerLatency{
			CustomerID: control[i].CustomerID,
			ControlP95: control[i].ControlP95,
			DeadP95:    dead[i].ControlP95,
			Ratio:      ratio,
		}
	}
	return comparisons, ok, nil
}

func snapshotScenario5BreakerMetrics(ctx context.Context, db *sql.DB, customerID string) (scenario5BreakerSnapshot, error) {
	var snap scenario5BreakerSnapshot
	var webhookID string
	err := db.QueryRowContext(ctx, `
		SELECT id::text, breaker_state, breaker_failure_count, COALESCE(breaker_probe_delivery_id::text, ''), breaker_probe_delivery_id IS NOT NULL
		FROM webhooks
		WHERE customer_id = $1
		ORDER BY created_at
		LIMIT 1
	`, customerID).Scan(&webhookID, &snap.State, &snap.FailureCount, &snap.ProbeDeliveryID, &snap.ProbeDeliveryValid)
	if err != nil {
		return scenario5BreakerSnapshot{}, fmt.Errorf("query breaker snapshot: %w", err)
	}
	snap.OpenMetric = testutil.ToFloat64(observe.WebhookBreakerState.WithLabelValues(webhookID, outbox.BreakerStateOpen))
	snap.HalfOpenMetric = testutil.ToFloat64(observe.WebhookBreakerState.WithLabelValues(webhookID, outbox.BreakerStateHalfOpen))
	snap.ClosedMetric = testutil.ToFloat64(observe.WebhookBreakerState.WithLabelValues(webhookID, outbox.BreakerStateClosed))
	snap.TripCount = testutil.ToFloat64(observe.WebhookBreakerTripsTotal.WithLabelValues(retry.ReasonTimeout))
	return snap, nil
}

func waitForScenario5BreakerTransitions(ctx context.Context, db *sql.DB, customerID string, threshold int) (scenario5BreakerSnapshot, scenario5BreakerSnapshot, scenario5BreakerSnapshot, error) {
	var openSnap, halfOpenSnap, finalSnap scenario5BreakerSnapshot
	var haveOpen, haveHalfOpen, haveFinal bool

	totalTimeout, pollInterval := scenario5BreakerPollingConfig()

	pollCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for {
		snap, err := snapshotScenario5BreakerMetrics(pollCtx, db, customerID)
		if err != nil {
			return scenario5BreakerSnapshot{}, scenario5BreakerSnapshot{}, scenario5BreakerSnapshot{}, err
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
			return scenario5BreakerSnapshot{}, scenario5BreakerSnapshot{}, scenario5BreakerSnapshot{}, fmt.Errorf("timed out waiting for breaker transitions: open=%v half_open=%v final=%v", haveOpen, haveHalfOpen, haveFinal)
		case <-time.After(pollInterval):
		}
	}
}

func scenario5BreakerPollingConfig() (time.Duration, time.Duration) {
	profileName, err := benchProfileNameFromEnv()
	if err != nil {
		profileName = benchProfileDev
	}

	totalTimeout := 5 * time.Minute
	if profileName != benchProfileStress {
		totalTimeout = 90 * time.Second
	}

	cooldown := scenario5BreakerInitialCooldownFromProfile()
	pollInterval := 500 * time.Millisecond
	if cooldown > 0 {
		pollInterval = cooldown / 20
	}
	if pollInterval < 250*time.Millisecond {
		pollInterval = 250 * time.Millisecond
	}
	if pollInterval > 2*time.Second {
		pollInterval = 2 * time.Second
	}

	return totalTimeout, pollInterval
}

func scenario5BreakerInitialCooldownFromProfile() time.Duration {
	raw, _, err := loadBenchProfileConfig("scenario5")
	if err != nil {
		return 0
	}
	return raw.BreakerInitialCooldown.Duration()
}

func writeScenario5Report(path, profileName string, customerCount, deliveriesPerCustomer, breakerThreshold int, control, dead scenario5RunResult) error {
	comparisons, withinLimit, err := compareScenario5Latencies(control.HealthyLatencies, dead.HealthyLatencies)
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

	_, _ = fmt.Fprintf(f, `# Scenario 5: Dead Partner Circuit Breaker + Pool Protection

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
		passFail(withinLimit),
		passFail(withinLimit),
		dead.BreakerOpenSnapshot.FailureCount,
		breakerThreshold,
		passFail(dead.BreakerOpenSnapshot.FailureCount >= breakerThreshold),
		passFail(dead.BreakerHalfOpenSnapshot.ProbeDeliveryValid),
		passFail(dead.BreakerHalfOpenSnapshot.ProbeDeliveryValid),
		dead.DeadPending,
		passFail(skippedOK),
		dead.DeadAttempts,
		deadAttemptsLimit,
		passFail(deadAttemptsOK),
		dead.BreakerFinalSnapshot.TripCount,
		passFail(tripsOK),
		goroutineDelta,
		passFail(goroutineOK),
		passFail(resultOK),
		passFail(resultOK),
	)

	return f.Close()
}

func probeID(snap scenario5BreakerSnapshot) string {
	if snap.ProbeDeliveryValid && snap.ProbeDeliveryID != "" {
		return snap.ProbeDeliveryID
	}
	return "-"
}
