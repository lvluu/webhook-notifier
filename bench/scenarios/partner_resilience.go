package scenarios

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const partnerResilienceReportPath = "bench/results/partner-resilience.md"

// ---------------------------------------------------------------------------
// Profile
// ---------------------------------------------------------------------------

type partnerResilienceProfile struct {
	Name                    string
	WorkerCount             int
	CustomerCount           int
	DeliveriesPerCustomer   int
	EventsPerCustomer       int
	SubmitInterval          time.Duration
	OutageDelay             time.Duration
	OutageDuration          time.Duration
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

func partnerResilienceProfileFromEnv() (partnerResilienceProfile, error) {
	raw, name, err := LoadBenchProfileConfig("partner_resilience")
	if err != nil {
		return partnerResilienceProfile{}, err
	}
	if raw.WorkerCount < 1 || raw.CustomerCount < 1 {
		return partnerResilienceProfile{}, fmt.Errorf("partner_resilience profile %q has invalid counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return partnerResilienceProfile{}, fmt.Errorf("partner_resilience profile %q requires positive request_timeout", name)
	}
	if raw.BreakerFailureThreshold < 1 {
		return partnerResilienceProfile{}, fmt.Errorf("partner_resilience profile %q requires breaker_failure_threshold >= 1", name)
	}

	return partnerResilienceProfile{
		Name:                    name,
		WorkerCount:             raw.WorkerCount,
		CustomerCount:           raw.CustomerCount,
		DeliveriesPerCustomer:   raw.DeliveriesPerCustomer,
		EventsPerCustomer:       raw.EventsPerCustomer,
		SubmitInterval:          raw.SubmitInterval.Duration(),
		OutageDelay:             raw.OutageDelay.Duration(),
		OutageDuration:          raw.OutageDuration.Duration(),
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

func partnerResilienceDiscoveryConfig(profile partnerResilienceProfile) worker.DiscoveryConfig {
	return worker.DiscoveryConfig{
		Interval:     profile.DiscoveryInterval,
		EmptyBackoff: profile.DiscoveryEmptyBackoff,
		ErrorBackoff: profile.DiscoveryErrorBackoff,
		MaxBackoff:   profile.DiscoveryMaxBackoff,
		Limit:        profile.DiscoveryLimit,
	}
}

// ---------------------------------------------------------------------------
// Top-level runner
// ---------------------------------------------------------------------------

// RunPartnerResilience executes the folded partner reliability drill-down.
func RunPartnerResilience(ctx context.Context) error {
	observe.RegisterMetrics(prometheus.NewRegistry())
	db, cleanup, err := OpenBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := partnerResilienceProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	var results partnerResilienceResults

	// Phase A — transient/recovering partners (from Transient Recovery)
	phaseA, err := runPartnerResiliencePhaseA(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("phase A (transient/recovering): %w", err)
	}
	results.PhaseA = phaseA

	// Phase B — terminal 4xx fast-fail (from Terminal Fast-Fail)
	phaseB, err := runPartnerResiliencePhaseB(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("phase B (terminal 4xx): %w", err)
	}
	results.PhaseB = phaseB

	// Phase C — dead partner breaker isolation (from Breaker Isolation)
	phaseC, err := runPartnerResiliencePhaseC(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("phase C (breaker isolation): %w", err)
	}
	results.PhaseC = phaseC

	if err := writePartnerResilienceReport(partnerResilienceReportPath, profile, results); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if !partnerResiliencePassed(results) {
		return fmt.Errorf("partner resilience hard gate failed")
	}

	slog.Info("partner resilience scenario complete", "profile", profile.Name, "report", partnerResilienceReportPath)
	return nil
}

type partnerResilienceResults struct {
	PhaseA partnerResiliencePhaseAResult
	PhaseB partnerResiliencePhaseBResult
	PhaseC partnerResiliencePhaseCResult
}

func partnerResiliencePassed(results partnerResilienceResults) bool {
	return results.PhaseA.AllTerminalOK &&
		results.PhaseA.RecoveryOK &&
		results.PhaseA.PartnerAOK &&
		results.PhaseA.JitterOK &&
		results.PhaseB.AllTerminalOK &&
		results.PhaseB.GoodOK &&
		results.PhaseB.BadOK &&
		results.PhaseC.HealthyWithinLimit &&
		results.PhaseC.BreakerOpened &&
		results.PhaseC.BreakerHalfOpen &&
		results.PhaseC.BreakerReopened &&
		results.PhaseC.GoroutineOK &&
		results.PhaseC.DeadAttemptsOK &&
		results.PhaseC.SkippedOK
}

// ---------------------------------------------------------------------------
// Phase A — Transient + Recovering Partners (Transient Recovery)
// ---------------------------------------------------------------------------

type partnerResiliencePhaseAResult struct {
	TotalEvents        int
	ExpectedDeliveries int
	TotalDeliveries    int
	TerminalDeliveries int
	PartnerAP99Ms      float64
	RecoverySucceeded  int
	RecoveryTotal      int
	JitterMaxPerSecond int
	JitterTotal        int
	AllTerminalOK      bool
	RecoveryOK         bool
	PartnerAOK         bool
	JitterOK           bool
}

var partnerResiliencePhaseAPartners = []struct {
	name string
	host string
	path string
}{
	{name: "Partner A", host: "pr-partner-a.test", path: "/hooks/partner-a"},
	{name: "Partner B", host: "pr-partner-b.test", path: "/hooks/partner-b"},
	{name: "Partner C", host: "pr-partner-c.test", path: "/hooks/partner-c"},
}

func runPartnerResiliencePhaseA(ctx context.Context, db *sql.DB, profile partnerResilienceProfile) (partnerResiliencePhaseAResult, error) {
	if err := ResetBenchData(ctx, db); err != nil {
		return partnerResiliencePhaseAResult{}, fmt.Errorf("reset: %w", err)
	}

	scenarioStart := time.Now()
	outageStart := scenarioStart.Add(profile.OutageDelay)
	outageEnd := outageStart.Add(profile.OutageDuration)

	rs := receiver.NewServer(
		receiver.WithStableDecisions("pr-phase-a"),
		receiver.WithRouteStatus("/hooks/partner-b", 0.30, http.StatusInternalServerError),
		receiver.WithRouteStatusFunc("/hooks/partner-c", func(_ *http.Request, now time.Time) int {
			if !now.Before(outageStart) && now.Before(outageEnd) {
				return http.StatusServiceUnavailable
			}
			return 0
		}),
	)
	defer rs.Close()

	totalEvents := profile.CustomerCount * profile.EventsPerCustomer
	expectedDeliveries := totalEvents * len(partnerResiliencePhaseAPartners)

	if err := seedPartnerResiliencePhaseACustomers(ctx, db, profile.CustomerCount); err != nil {
		return partnerResiliencePhaseAResult{}, err
	}
	if err := seedPartnerResiliencePhaseAWebhooks(ctx, db, profile.CustomerCount); err != nil {
		return partnerResiliencePhaseAResult{}, err
	}

	plainSched := roundrobin.New()
	reg := registry.New(db)
	ingestStore := ingest.New(db, reg, plainSched)
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(false))

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	client, err := partnerResilienceHTTPClient(rs.URL())
	if err != nil {
		return partnerResiliencePhaseAResult{}, fmt.Errorf("http client: %w", err)
	}
	hostLimiter := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     16,
		AcquireTimeout: 250 * time.Millisecond,
		RequeueDelay:   100 * time.Millisecond,
	})
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	var workerWg sync.WaitGroup
	for i := 0; i < profile.WorkerCount; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(
				plainSched,
				worker.WithHTTPClient(client),
				worker.WithHostLimiter(hostLimiter),
				worker.WithRetryPolicy(retry.New(retry.Config{
					MaxAttempts: 20,
					BaseDelay:   retry.DefaultBaseDelay,
					CapDelay:    15 * time.Second,
				})),
				worker.WithLogger(workerLog),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("partner-resilience phase-A worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	defer cancelDiscovery()
	discoveryDone := make(chan error, 1)
	go func() {
		discoveryDone <- worker.RunClaimableCustomerDiscovery(discoveryCtx, outboxStore, plainSched, partnerResilienceDiscoveryConfig(profile))
	}()

	if err := submitPartnerResiliencePhaseAEvents(ctx, ingestStore, profile.CustomerCount, profile.EventsPerCustomer, profile.SubmitInterval); err != nil {
		return partnerResiliencePhaseAResult{}, err
	}

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelPoll()
	for {
		pending, err := CountPendingDeliveries(pollCtx, db)
		if err != nil {
			return partnerResiliencePhaseAResult{}, fmt.Errorf("poll: %w", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			return partnerResiliencePhaseAResult{}, fmt.Errorf("drain timeout: %d pending", pending)
		case <-time.After(time.Second):
		}
	}
	cancelDiscovery()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return partnerResiliencePhaseAResult{}, fmt.Errorf("claimable discovery: %w", err)
	}
	cancelWorkers()
	workerWg.Wait()

	totalDeliveries, _ := QueryTotalDeliveries(ctx, db)
	terminal, _ := QueryTerminalDeliveries(ctx, db)
	partnerA, _ := queryPartnerLatencyExact(ctx, db, "pr-partner-a.test")
	recovery, _ := queryPartnerRecoveryExact(ctx, db, "pr-partner-c.test", outageStart, outageEnd)
	jitter, _ := queryPartnerRetryBucketsExact(ctx, db, "pr-partner-c.test", outageEnd)

	const partnerAP99LimitMs = 2000.0
	partnerAOK := partnerA.P99Ms < partnerAP99LimitMs
	jitterLimit := int(float64(jitter.Total) * 0.25)
	if jitter.Total > 0 && jitterLimit < 1 {
		jitterLimit = 1
	}
	jitterOK := jitter.Total == 0 || jitter.MaxPerSecond <= jitterLimit

	return partnerResiliencePhaseAResult{
		TotalEvents:        totalEvents,
		ExpectedDeliveries: expectedDeliveries,
		TotalDeliveries:    totalDeliveries,
		TerminalDeliveries: terminal,
		PartnerAP99Ms:      partnerA.P99Ms,
		RecoverySucceeded:  recovery.Succeeded,
		RecoveryTotal:      recovery.Total,
		JitterMaxPerSecond: jitter.MaxPerSecond,
		JitterTotal:        jitter.Total,
		AllTerminalOK:      totalDeliveries == expectedDeliveries && terminal == totalDeliveries,
		RecoveryOK:         recovery.Total == recovery.Succeeded,
		PartnerAOK:         partnerAOK,
		JitterOK:           jitterOK,
	}, nil
}

// ---------------------------------------------------------------------------
// Phase B — Terminal 4xx Fast-Fail (Terminal Fast-Fail)
// ---------------------------------------------------------------------------

type partnerResiliencePhaseBResult struct {
	EventCount         int
	ExpectedDeliveries int
	TotalDeliveries    int
	TerminalDeliveries int
	GoodSucceeded      int
	GoodTotal          int
	BadDead            int
	BadTotal           int
	RetryWaste         int
	AllTerminalOK      bool
	GoodOK             bool
	BadOK              bool
}

func runPartnerResiliencePhaseB(ctx context.Context, db *sql.DB, profile partnerResilienceProfile) (partnerResiliencePhaseBResult, error) {
	if err := ResetBenchData(ctx, db); err != nil {
		return partnerResiliencePhaseBResult{}, fmt.Errorf("reset: %w", err)
	}

	eventCount := profile.EventsPerCustomer
	customerID := "pr-phase-b-customer"

	rs := receiver.NewServer(
		receiver.WithStableDecisions("pr-phase-b"),
		receiver.WithRouteStatus("/hooks/misconfigured-404", 1.0, http.StatusNotFound),
	)
	defer rs.Close()

	if _, err := db.ExecContext(ctx, `INSERT INTO customers (id, class) VALUES ($1, 1) ON CONFLICT DO NOTHING`, customerID); err != nil {
		return partnerResiliencePhaseBResult{}, fmt.Errorf("seed customer: %w", err)
	}
	goodURL := rs.URL() + "/hooks/good"
	badURL := rs.URL() + "/hooks/misconfigured-404"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES
		($1, $2, '', '{bench.test}', true),
		($1, $3, '', '{bench.test}', true)
		ON CONFLICT DO NOTHING
	`, customerID, goodURL, badURL); err != nil {
		return partnerResiliencePhaseBResult{}, fmt.Errorf("seed webhooks: %w", err)
	}

	plainSched := roundrobin.New()
	reg := registry.New(db)
	ingestStore := ingest.New(db, reg, plainSched)
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(false))

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	var workerWg sync.WaitGroup
	for i := 0; i < 4; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(plainSched, worker.WithLogger(workerLog))
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("partner-resilience phase-B worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	defer cancelDiscovery()
	discoveryDone := make(chan error, 1)
	go func() {
		discoveryDone <- worker.RunClaimableCustomerDiscovery(discoveryCtx, outboxStore, plainSched, partnerResilienceDiscoveryConfig(profile))
	}()

	for range eventCount {
		if _, err := ingestStore.Submit(ctx, ingest.Event{
			CustomerID: customerID,
			Type:       EventType,
			Payload:    []byte(`{"event_name":"subscriber.created"}`),
		}); err != nil {
			return partnerResiliencePhaseBResult{}, fmt.Errorf("submit: %w", err)
		}
	}

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelPoll()
	for {
		pending, err := CountPendingDeliveries(pollCtx, db)
		if err != nil {
			return partnerResiliencePhaseBResult{}, fmt.Errorf("poll: %w", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			return partnerResiliencePhaseBResult{}, fmt.Errorf("drain timeout: %d pending", pending)
		case <-time.After(time.Second):
		}
	}
	cancelDiscovery()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return partnerResiliencePhaseBResult{}, fmt.Errorf("claimable discovery: %w", err)
	}
	cancelWorkers()
	workerWg.Wait()

	expectedDeliveries := eventCount * 2
	totalDeliveries, _ := QueryTotalDeliveries(ctx, db)
	terminal, _ := QueryTerminalDeliveries(ctx, db)

	good := partnerResilienceQueryWebhookResult(ctx, db, goodURL)
	bad := partnerResilienceQueryWebhookResult(ctx, db, badURL)

	return partnerResiliencePhaseBResult{
		EventCount:         eventCount,
		ExpectedDeliveries: expectedDeliveries,
		TotalDeliveries:    totalDeliveries,
		TerminalDeliveries: terminal,
		GoodSucceeded:      good.Succeeded,
		GoodTotal:          good.Total,
		BadDead:            bad.Dead,
		BadTotal:           bad.Total,
		RetryWaste:         bad.RetryWaste,
		AllTerminalOK:      totalDeliveries == expectedDeliveries && terminal == expectedDeliveries,
		GoodOK:             good.Total == eventCount && good.Succeeded == eventCount,
		BadOK:              bad.Total == eventCount && bad.Dead == eventCount && bad.RetryWaste == 0,
	}, nil
}

type partnerResilienceWebhookResult struct {
	Total      int
	Succeeded  int
	Dead       int
	RetryWaste int
}

func partnerResilienceQueryWebhookResult(ctx context.Context, db *sql.DB, webhookURL string) partnerResilienceWebhookResult {
	var r partnerResilienceWebhookResult
	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE d.status = 'succeeded'),
			COUNT(*) FILTER (WHERE d.status = 'dead'),
			COUNT(*) FILTER (WHERE d.attempt_count > 1)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url = $1
	`, webhookURL).Scan(&r.Total, &r.Succeeded, &r.Dead, &r.RetryWaste); err != nil {
		return partnerResilienceWebhookResult{}
	}
	return r
}

// ---------------------------------------------------------------------------
// Phase C — Dead Partner Breaker Isolation (Breaker Isolation)
// ---------------------------------------------------------------------------

type partnerResiliencePhaseCResult struct {
	ControlDuration    time.Duration
	DeadDuration       time.Duration
	BaselineGoroutines int
	FinalGoroutines    int
	DeadAttempts       int
	DeadAttemptsLimit  int
	DeadPending        int
	HealthyWithinLimit bool
	BreakerOpened      bool
	BreakerHalfOpen    bool
	BreakerReopened    bool
	GoroutineOK        bool
	DeadAttemptsOK     bool
	SkippedOK          bool
}

const (
	prPhaseCControlSeed    = "pr-phase-c-control"
	prPhaseCDeadSeed       = "pr-phase-c-dead"
	prPhaseCHealthyLatency = 10 * time.Millisecond
)

func runPartnerResiliencePhaseC(ctx context.Context, db *sql.DB, profile partnerResilienceProfile) (partnerResiliencePhaseCResult, error) {
	control, err := runPartnerResiliencePhaseCRun(ctx, db, profile, false)
	if err != nil {
		return partnerResiliencePhaseCResult{}, fmt.Errorf("control run: %w", err)
	}
	dead, err := runPartnerResiliencePhaseCRun(ctx, db, profile, true)
	if err != nil {
		return partnerResiliencePhaseCResult{}, fmt.Errorf("dead-partner run: %w", err)
	}

	withinLimit := true
	for i, lat := range control.HealthyLatencies {
		if i >= len(dead.HealthyLatencies) {
			break
		}
		if lat.ControlP95 == 0 {
			return partnerResiliencePhaseCResult{}, fmt.Errorf("control P95 is zero — cannot compare latencies for %s", lat.CustomerID)
		}
		ratio := dead.HealthyLatencies[i].ControlP95 / lat.ControlP95
		if ratio > 1.10 {
			withinLimit = false
		}
	}

	goroutineDelta := dead.FinalGoroutines - dead.BaselineGoroutines
	goroutineOK := goroutineDelta <= 8
	deadAttemptsLimit := profile.DeliveriesPerCustomer
	deadAttemptsOK := dead.DeadAttempts > 0 && dead.DeadAttempts <= deadAttemptsLimit
	skippedOK := dead.DeadPending > 0

	return partnerResiliencePhaseCResult{
		ControlDuration:    control.Duration,
		DeadDuration:       dead.Duration,
		BaselineGoroutines: dead.BaselineGoroutines,
		FinalGoroutines:    dead.FinalGoroutines,
		DeadAttempts:       dead.DeadAttempts,
		DeadAttemptsLimit:  deadAttemptsLimit,
		DeadPending:        dead.DeadPending,
		HealthyWithinLimit: withinLimit,
		BreakerOpened:      dead.BreakerOpen,
		BreakerHalfOpen:    dead.BreakerHalfOpen,
		BreakerReopened:    dead.BreakerReopened,
		GoroutineOK:        goroutineOK,
		DeadAttemptsOK:     deadAttemptsOK,
		SkippedOK:          skippedOK,
	}, nil
}

type prPhaseCRunResult struct {
	Duration           time.Duration
	BaselineGoroutines int
	FinalGoroutines    int
	HealthyLatencies   []BreakerIsolationCustomerLatency
	DeadAttempts       int
	DeadPending        int
	BreakerOpen        bool
	BreakerHalfOpen    bool
	BreakerReopened    bool
}

func runPartnerResiliencePhaseCRun(ctx context.Context, db *sql.DB, profile partnerResilienceProfile, deadPartner bool) (prPhaseCRunResult, error) {
	if err := ResetBenchData(ctx, db); err != nil {
		return prPhaseCRunResult{}, fmt.Errorf("reset: %w", err)
	}

	customerCount := profile.CustomerCount
	seed := prPhaseCControlSeed
	if deadPartner {
		seed = prPhaseCDeadSeed
	}

	opts := []receiver.Option{receiver.WithStableDecisions(seed)}
	for i := 0; i < customerCount; i++ {
		path := fmt.Sprintf("/hooks/pr-customer-%02d", i+1)
		if deadPartner && i == 0 {
			opts = append(opts, receiver.WithRouteHang(path))
		} else {
			opts = append(opts, receiver.WithRouteLatency(path, 1.0, prPhaseCHealthyLatency))
		}
	}
	rs := receiver.NewServer(opts...)
	defer rs.Close()

	ids := make([]string, customerCount)
	for i := range ids {
		if i == 0 {
			ids[i] = "pr-phase-c-zdead-customer"
		} else {
			ids[i] = fmt.Sprintf("pr-phase-c-customer-%02d", i)
		}
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, customerCount*2)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d)", i*2+1, i*2+2)
		args = append(args, id, 1)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return prPhaseCRunResult{}, fmt.Errorf("seed customers: %w", err)
	}

	webhookArgs := make([]any, 0, customerCount*4)
	phaseCHosts := make([]string, 0, customerCount)
	for i, id := range ids {
		host := fmt.Sprintf("pr-phase-c-partner-%02d.test", i+1)
		path := fmt.Sprintf("/hooks/pr-customer-%02d", i+1)
		phaseCHosts = append(phaseCHosts, host)
		webhookArgs = append(webhookArgs, id, "http://"+host+path, "", "{bench.test}")
	}
	sb2 := strings.Builder{}
	sb2.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	for i := range ids {
		if i > 0 {
			sb2.WriteString(", ")
		}
		fmt.Fprintf(&sb2, "($%d, $%d, $%d, $%d, true)", i*4+1, i*4+2, i*4+3, i*4+4)
	}
	sb2.WriteString(" ON CONFLICT DO NOTHING")
	if _, err := db.ExecContext(ctx, sb2.String(), webhookArgs...); err != nil {
		return prPhaseCRunResult{}, fmt.Errorf("seed webhooks: %w", err)
	}

	for _, customerID := range ids {
		var webhookID string
		if err := db.QueryRowContext(ctx, `SELECT id::text FROM webhooks WHERE customer_id = $1 ORDER BY created_at LIMIT 1`, customerID).Scan(&webhookID); err != nil {
			return prPhaseCRunResult{}, fmt.Errorf("lookup webhook: %w", err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return prPhaseCRunResult{}, fmt.Errorf("begin tx: %w", err)
		}
		var eventID string
		if err := tx.QueryRowContext(ctx, `INSERT INTO events (customer_id, type, payload) VALUES ($1, $2, $3) RETURNING id`,
			customerID, EventType, []byte(`{}`)).Scan(&eventID); err != nil {
			_ = tx.Rollback()
			return prPhaseCRunResult{}, fmt.Errorf("insert event: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
			SELECT $1, $2, $3, 'pending', 0, now() FROM generate_series(1, $4)`,
			eventID, webhookID, customerID, profile.DeliveriesPerCustomer); err != nil {
			_ = tx.Rollback()
			return prPhaseCRunResult{}, fmt.Errorf("insert deliveries: %w", err)
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return prPhaseCRunResult{}, fmt.Errorf("commit: %w", err)
		}
	}

	baseline := runtime.NumGoroutine()
	sched := roundrobin.New()
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0))
	webhookBreaker, err := newBenchBreaker(ctx, db, profile.BreakerFailureThreshold, profile.BreakerInitialCooldown, profile.BreakerMaxCooldown)
	if err != nil {
		return prPhaseCRunResult{}, err
	}
	httpClient, err := partnerResilienceHTTPClientForHosts(rs.URL(), phaseCHosts)
	if err != nil {
		return prPhaseCRunResult{}, fmt.Errorf("http client: %w", err)
	}
	hostLimiter := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     1,
		AcquireTimeout: 25 * time.Millisecond,
		RequeueDelay:   50 * time.Millisecond,
	})
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
			w := worker.New(sched,
				worker.WithHTTPClient(httpClient),
				worker.WithRequestTimeout(profile.RequestTimeout),
				worker.WithHostLimiter(hostLimiter),
				worker.WithMinBatchHint(1),
				worker.WithDeliveryConcurrency(1),
				worker.WithRetryPolicy(retry.New(retry.Config{MaxAttempts: 20, BaseDelay: 50 * time.Millisecond, CapDelay: 50 * time.Millisecond})),
				worker.WithBreaker(webhookBreaker),
				worker.WithLogger(workerLog))
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("phase-C worker failed", "worker_id", id, "error", err)
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
	var breakerOpen, breakerHalfOpen, breakerReopened bool

	if deadPartner {
		deadCustomerID := ids[0]
		totalTimeout, pollInterval := BreakerIsolationBreakerPollingConfig()
		pollCtx, cancel := context.WithTimeout(ctx, totalTimeout)
		defer cancel()
	breakerPollLoop:
		for {
			var state string
			var fc int
			if err := db.QueryRowContext(pollCtx, `SELECT breaker_state, breaker_failure_count FROM webhooks WHERE customer_id = $1 ORDER BY created_at LIMIT 1`, deadCustomerID).Scan(&state, &fc); err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					break breakerPollLoop
				}
				return prPhaseCRunResult{}, fmt.Errorf("poll breaker state: %w", err)
			}
			switch {
			case state == outbox.BreakerStateOpen && fc >= profile.BreakerFailureThreshold && !breakerOpen:
				breakerOpen = true
			case state == outbox.BreakerStateHalfOpen && !breakerHalfOpen:
				breakerHalfOpen = true
			case state == outbox.BreakerStateOpen && fc > profile.BreakerFailureThreshold && breakerHalfOpen && !breakerReopened:
				breakerReopened = true
			}
			if breakerOpen && breakerHalfOpen && breakerReopened {
				break
			}
			select {
			case <-pollCtx.Done():
				break breakerPollLoop
			case <-time.After(pollInterval):
			}
		}
		if !breakerOpen || !breakerHalfOpen || !breakerReopened {
			return prPhaseCRunResult{}, fmt.Errorf(
				"breaker transition timeout: open=%t half_open=%t reopened=%t",
				breakerOpen,
				breakerHalfOpen,
				breakerReopened,
			)
		}
	}

	healthyIDs := ids[1:]
	pollCtx, cancelPoll := context.WithTimeout(ctx, 20*time.Minute)
	defer cancelPoll()
	for {
		var n int
		if err := db.QueryRowContext(pollCtx, `SELECT COUNT(*) FROM deliveries WHERE customer_id = ANY($1) AND status NOT IN ('succeeded', 'dead')`, healthyIDs).Scan(&n); err != nil {
			return prPhaseCRunResult{}, fmt.Errorf("poll healthy deliveries: %w", err)
		}
		if n == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			return prPhaseCRunResult{}, fmt.Errorf("healthy drain timeout: %d pending", n)
		case <-time.After(250 * time.Millisecond):
		}
	}

	cancelDiscovery()
	cancelWorkers()
	workerWg.Wait()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return prPhaseCRunResult{}, fmt.Errorf("claimable discovery: %w", err)
	}

	rs.Close()
	time.Sleep(250 * time.Millisecond)

	result := prPhaseCRunResult{
		Duration:           time.Since(started),
		BaselineGoroutines: baseline,
		FinalGoroutines:    runtime.NumGoroutine(),
		BreakerOpen:        breakerOpen,
		BreakerHalfOpen:    breakerHalfOpen,
		BreakerReopened:    breakerReopened,
	}

	rows, err := db.QueryContext(ctx, `SELECT d.customer_id, COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY da.duration_ms), 0)
		FROM deliveries d JOIN delivery_attempts da ON da.delivery_id = d.id WHERE d.customer_id <> $1 AND d.status = 'succeeded' AND da.attempt_number = 1 GROUP BY d.customer_id ORDER BY d.customer_id`, ids[0])
	if err != nil {
		return prPhaseCRunResult{}, fmt.Errorf("query healthy latencies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var lat BreakerIsolationCustomerLatency
		if err := rows.Scan(&lat.CustomerID, &lat.ControlP95); err != nil {
			return prPhaseCRunResult{}, fmt.Errorf("scan healthy latency: %w", err)
		}
		result.HealthyLatencies = append(result.HealthyLatencies, lat)
	}
	if err := rows.Err(); err != nil {
		return prPhaseCRunResult{}, fmt.Errorf("iterate healthy latencies: %w", err)
	}

	if deadPartner {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts da JOIN deliveries d ON d.id = da.delivery_id WHERE d.customer_id = $1`, ids[0]).Scan(&result.DeadAttempts); err != nil {
			return prPhaseCRunResult{}, fmt.Errorf("query dead partner attempts: %w", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE customer_id = $1 AND status NOT IN ('succeeded', 'dead')`, ids[0]).Scan(&result.DeadPending); err != nil {
			return prPhaseCRunResult{}, fmt.Errorf("query dead partner pending deliveries: %w", err)
		}
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// Helpers (exact URL matching — no LIKE suffix patterns)
// ---------------------------------------------------------------------------

func partnerResilienceHTTPClient(receiverURL string) (*http.Client, error) {
	hosts := make([]string, 0, len(partnerResiliencePhaseAPartners))
	for _, p := range partnerResiliencePhaseAPartners {
		hosts = append(hosts, p.host)
	}
	return partnerResilienceHTTPClientForHosts(receiverURL, hosts)
}

func partnerResilienceHTTPClientForHosts(receiverURL string, hosts []string) (*http.Client, error) {
	u, err := url.Parse(receiverURL)
	if err != nil {
		return nil, fmt.Errorf("parse receiver url: %w", err)
	}
	receiverAddr := u.Host
	fakeHosts := map[string]bool{}
	for _, host := range hosts {
		fakeHosts[host] = true
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err == nil && fakeHosts[host] {
				return dialer.DialContext(ctx, network, receiverAddr)
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}
	return &http.Client{Transport: transport, Timeout: 15 * time.Second}, nil
}

func partnerResiliencePhaseACustomerID(i int) string {
	return fmt.Sprintf("pr-phase-a-customer-%02d", i)
}

func seedPartnerResiliencePhaseACustomers(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, customerCount*2)
	for i := 0; i < customerCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d)", i*2+1, i*2+2)
		args = append(args, partnerResiliencePhaseACustomerID(i), 1)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert customers: %w", err)
	}
	return nil
}

func seedPartnerResiliencePhaseAWebhooks(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, customerCount*len(partnerResiliencePhaseAPartners)*4)
	row := 0
	for i := 0; i < customerCount; i++ {
		customerID := partnerResiliencePhaseACustomerID(i)
		for _, partner := range partnerResiliencePhaseAPartners {
			if row > 0 {
				sb.WriteString(", ")
			}
			base := row * 4
			fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", base+1, base+2, base+3, base+4)
			args = append(args, customerID, fmt.Sprintf("http://%s%s", partner.host, partner.path), "", "{bench.test}")
			row++
		}
	}
	sb.WriteString(" ON CONFLICT DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert webhooks: %w", err)
	}
	return nil
}

func submitPartnerResiliencePhaseAEvents(ctx context.Context, store *ingest.Store, customerCount, eventsPerCustomer int, interval time.Duration) error {
	for i := 0; i < customerCount; i++ {
		customerID := partnerResiliencePhaseACustomerID(i)
		for range eventsPerCustomer {
			if _, err := store.Submit(ctx, ingest.Event{
				CustomerID: customerID,
				Type:       EventType,
				Payload:    []byte(`{"event_name":"subscriber.created"}`),
			}); err != nil {
				return fmt.Errorf("submit: %w", err)
			}
			if interval > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(interval):
				}
			}
		}
	}
	return nil
}

func queryPartnerLatencyExact(ctx context.Context, db *sql.DB, host string) (PartnerLatencyStats, error) {
	var stats PartnerLatencyStats
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url LIKE 'http://' || $1 || '%'
		  AND d.status IN ('succeeded', 'dead')
	`, host).Scan(&stats.P99Ms)
	return stats, err
}

func queryPartnerRecoveryExact(ctx context.Context, db *sql.DB, host string, outageStart, outageEnd time.Time) (PartnerRecoveryStats, error) {
	var stats PartnerRecoveryStats
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE d.status = 'succeeded' AND d.updated_at >= $3)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url LIKE 'http://' || $1 || '%'
		  AND d.created_at >= $2 AND d.created_at < $3
	`, host, outageStart, outageEnd).Scan(&stats.Total, &stats.Succeeded)
	return stats, err
}

func queryPartnerRetryBucketsExact(ctx context.Context, db *sql.DB, host string, recoveryAt time.Time) (RetryBucketStats, error) {
	var stats RetryBucketStats
	err := db.QueryRowContext(ctx, `
		WITH buckets AS (
			SELECT date_trunc('second', da.sent_at) AS bucket, COUNT(*) AS attempts
			FROM delivery_attempts da
			JOIN deliveries d ON d.id = da.delivery_id
			JOIN webhooks w ON w.id = d.webhook_id
			WHERE w.url LIKE 'http://' || $1 || '%'
			  AND da.attempt_number > 1 AND da.sent_at >= $2
			GROUP BY bucket
		)
		SELECT COALESCE(SUM(attempts), 0), COALESCE(MAX(attempts), 0) FROM buckets
	`, host, recoveryAt).Scan(&stats.Total, &stats.MaxPerSecond)
	return stats, err
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

func writePartnerResilienceReport(path string, profile partnerResilienceProfile, results partnerResilienceResults) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Partner Resilience: Reliability Drill-Down

## Description

Profile **%s**: consolidated reliability proof covering transient partner
recovery, terminal 4xx fast-fail, and dead-partner circuit-breaker isolation.

Folded from the legacy Scenarios 2, 3, and 5.

## Phase A — Transient + Recovering Partners

| Metric | Value |
|---|---:|
| Events | %d |
| Expected Deliveries | %d |
| Total Deliveries | %d |
| Terminal Deliveries | %d |
| Partner A p99 (ms) | %.2f |
| Recovery: Successfully recovered | %d / %d |
| Jitter: max retries per second post-recovery | %d / %d total |

| Gate | OK |
|---|---|
| 100%% terminal | %s |
| Recovery complete | %s |
| Partner A p99 unaffected | %s |
| No post-recovery retry spike | %s |

## Phase B — Terminal 4xx Fast-Fail

| Metric | Value |
|---|---:|
| Events submitted | %d |
| Expected Deliveries | %d |
| Total Deliveries | %d |
| Terminal Deliveries | %d |
| Good webhook: succeeded / total | %d / %d |
| Bad webhook: dead / total | %d / %d |
| Bad webhook: retry waste (deliveries with >1 attempt) | %d |

| Gate | OK |
|---|---|
| 100%% terminal | %s |
| Good webhook all succeeded | %s |
| Bad webhook all dead, no retry waste | %s |

## Phase C — Dead Partner Breaker Isolation

| Metric | Control | Dead Partner |
|---|---|:---:|---:|
| Duration | %s | %s |
| Goroutines (baseline → final) | %d | %d |
| Dead-customer attempts | — | %d |
| Dead-customer attempt cap | — | %d |
| Dead-customer pending (skipped) | — | %d |

| Gate | OK |
|---|---|
| Healthy Customer p95 within 10%% of control | %s |
| Breaker opened | %s |
| Half-open probe observed | %s |
| Breaker re-opened after probe | %s |
| Goroutine count stable | %s |
| Dead-customer attempt cap met | %s |
| Deliveries skipped after breaker open | %s |
`,
		profile.Name,
		results.PhaseA.TotalEvents, results.PhaseA.ExpectedDeliveries,
		results.PhaseA.TotalDeliveries, results.PhaseA.TerminalDeliveries,
		results.PhaseA.PartnerAP99Ms,
		results.PhaseA.RecoverySucceeded, results.PhaseA.RecoveryTotal,
		results.PhaseA.JitterMaxPerSecond, results.PhaseA.JitterTotal,
		PassFail(results.PhaseA.AllTerminalOK), PassFail(results.PhaseA.RecoveryOK),
		PassFail(results.PhaseA.PartnerAOK), PassFail(results.PhaseA.JitterOK),
		results.PhaseB.EventCount, results.PhaseB.ExpectedDeliveries,
		results.PhaseB.TotalDeliveries, results.PhaseB.TerminalDeliveries,
		results.PhaseB.GoodSucceeded, results.PhaseB.GoodTotal,
		results.PhaseB.BadDead, results.PhaseB.BadTotal,
		results.PhaseB.RetryWaste,
		PassFail(results.PhaseB.AllTerminalOK), PassFail(results.PhaseB.GoodOK), PassFail(results.PhaseB.BadOK),
		results.PhaseC.ControlDuration.Round(time.Millisecond), results.PhaseC.DeadDuration.Round(time.Millisecond),
		results.PhaseC.BaselineGoroutines, results.PhaseC.FinalGoroutines,
		results.PhaseC.DeadAttempts, results.PhaseC.DeadAttemptsLimit, results.PhaseC.DeadPending,
		PassFail(results.PhaseC.HealthyWithinLimit), PassFail(results.PhaseC.BreakerOpened),
		PassFail(results.PhaseC.BreakerHalfOpen), PassFail(results.PhaseC.BreakerReopened),
		PassFail(results.PhaseC.GoroutineOK), PassFail(results.PhaseC.DeadAttemptsOK), PassFail(results.PhaseC.SkippedOK),
	)
	return f.Close()
}
