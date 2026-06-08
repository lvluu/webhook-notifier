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

// ---------------------------------------------------------------------------
// Profile
// ---------------------------------------------------------------------------

type reliabilityProfile struct {
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

func benchmarkProfileFromEnv(scenario string) (reliabilityProfile, error) {
	raw, name, err := loadBenchProfileConfig(scenario)
	if err != nil {
		return reliabilityProfile{}, err
	}
	if raw.WorkerCount < 1 || raw.CustomerCount < 1 {
		return reliabilityProfile{}, fmt.Errorf("%s profile %q has invalid counts", scenario, name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return reliabilityProfile{}, fmt.Errorf("%s profile %q requires positive request_timeout", scenario, name)
	}
	if raw.BreakerFailureThreshold < 1 {
		return reliabilityProfile{}, fmt.Errorf("%s profile %q requires breaker_failure_threshold >= 1", scenario, name)
	}
	if scenario == "reliability" && raw.EventsPerCustomer < 1 {
		return reliabilityProfile{}, fmt.Errorf("reliability profile %q requires events_per_customer >= 1", name)
	}
	if scenario == "partner_protection" && raw.DeliveriesPerCustomer < 1 {
		return reliabilityProfile{}, fmt.Errorf("partner_protection profile %q requires deliveries_per_customer >= 1", name)
	}

	return reliabilityProfile{
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

func reliabilityProfileFromEnv() (reliabilityProfile, error) {
	return benchmarkProfileFromEnv("reliability")
}

func partnerProtectionProfileFromEnv() (reliabilityProfile, error) {
	return benchmarkProfileFromEnv("partner_protection")
}

func reliabilityDiscoveryConfig(profile reliabilityProfile) worker.DiscoveryConfig {
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

// RunReliability validates retry recovery and terminal failure handling.
func RunReliability(ctx context.Context) error {
	observe.RegisterMetrics(prometheus.NewRegistry())
	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	profile, err := reliabilityProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	var results reliabilityResults

	// Phase A — transient/recovering partners.
	phaseA, err := runReliabilityPhaseA(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("phase A (transient/recovering): %w", err)
	}
	results.PhaseA = phaseA

	// Phase B — terminal 4xx fast-fail.
	phaseB, err := runReliabilityPhaseB(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("phase B (terminal 4xx): %w", err)
	}
	results.PhaseB = phaseB

	reportPath := benchmarkReportPath("reliability", profile.Name)
	if err := writeReliabilityReport(reportPath, profile, results); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if !reliabilityPassed(results) {
		return fmt.Errorf("reliability hard gate failed")
	}

	slog.Info("reliability scenario complete", "profile", profile.Name, "report", reportPath)
	return nil
}

// RunPartnerProtection executes the dead-partner breaker and healthy-Customer
// isolation proof corresponding to the Partner Protection dashboard row.
func RunPartnerProtection(ctx context.Context) error {
	observe.RegisterMetrics(prometheus.NewRegistry())
	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}
	profile, err := partnerProtectionProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	result, err := runPartnerProtection(ctx, db, profile)
	if err != nil {
		return fmt.Errorf("partner protection: %w", err)
	}
	reportPath := benchmarkReportPath("partner-protection", profile.Name)
	if err := writePartnerProtectionReport(reportPath, profile, result); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if !partnerProtectionPassed(result) {
		return fmt.Errorf("partner protection hard gate failed")
	}

	slog.Info("partner protection scenario complete", "profile", profile.Name, "report", reportPath)
	return nil
}

func partnerProtectionPassed(result partnerProtectionResult) bool {
	return result.HealthyWithinLimit &&
		result.BreakerOpened &&
		result.BreakerHalfOpen &&
		result.BreakerReopened &&
		result.GoroutineOK &&
		result.DeadAttemptsOK &&
		result.SkippedOK
}

type reliabilityResults struct {
	PhaseA reliabilityPhaseAResult
	PhaseB reliabilityPhaseBResult
}

func reliabilityPassed(results reliabilityResults) bool {
	return results.PhaseA.AllTerminalOK &&
		results.PhaseA.RecoveryOK &&
		results.PhaseA.PartnerAOK &&
		results.PhaseA.JitterOK &&
		results.PhaseB.AllTerminalOK &&
		results.PhaseB.GoodOK &&
		results.PhaseB.BadOK
}

// ---------------------------------------------------------------------------
// Phase A — Transient + Recovering Partners
// ---------------------------------------------------------------------------

type reliabilityPhaseAResult struct {
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

var reliabilityPhaseAPartners = []struct {
	name string
	host string
	path string
}{
	{name: "Partner A", host: "pr-partner-a.test", path: "/hooks/partner-a"},
	{name: "Partner B", host: "pr-partner-b.test", path: "/hooks/partner-b"},
	{name: "Partner C", host: "pr-partner-c.test", path: "/hooks/partner-c"},
}

func runReliabilityPhaseA(ctx context.Context, db *sql.DB, profile reliabilityProfile) (reliabilityPhaseAResult, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("reset: %w", err)
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
	expectedDeliveries := totalEvents * len(reliabilityPhaseAPartners)

	if err := seedReliabilityPhaseACustomers(ctx, db, profile.CustomerCount); err != nil {
		return reliabilityPhaseAResult{}, err
	}
	if err := seedReliabilityPhaseAWebhooks(ctx, db, profile.CustomerCount); err != nil {
		return reliabilityPhaseAResult{}, err
	}

	plainSched := roundrobin.New()
	reg := registry.New(db)
	ingestStore := ingest.New(db, reg, plainSched)
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(false))

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	client, err := reliabilityHTTPClient(rs.URL())
	if err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("http client: %w", err)
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
				slog.Error("reliability phase-A worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	defer cancelDiscovery()
	discoveryDone := make(chan error, 1)
	go func() {
		discoveryDone <- worker.RunClaimableCustomerDiscovery(discoveryCtx, outboxStore, plainSched, reliabilityDiscoveryConfig(profile))
	}()

	if err := submitReliabilityPhaseAEvents(ctx, ingestStore, profile.CustomerCount, profile.EventsPerCustomer, profile.SubmitInterval); err != nil {
		return reliabilityPhaseAResult{}, err
	}

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelPoll()
	for {
		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return reliabilityPhaseAResult{}, fmt.Errorf("poll: %w", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			return reliabilityPhaseAResult{}, fmt.Errorf("drain timeout: %d pending", pending)
		case <-time.After(time.Second):
		}
	}
	cancelDiscovery()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return reliabilityPhaseAResult{}, fmt.Errorf("claimable discovery: %w", err)
	}
	cancelWorkers()
	workerWg.Wait()

	totalDeliveries, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("query total deliveries: %w", err)
	}
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("query terminal deliveries: %w", err)
	}
	partnerA, err := queryPartnerLatencyExact(ctx, db, "pr-partner-a.test")
	if err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("query healthy partner latency: %w", err)
	}
	recovery, err := queryPartnerRecoveryExact(ctx, db, "pr-partner-c.test", outageStart, outageEnd)
	if err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("query recovery: %w", err)
	}
	jitter, err := queryPartnerRetryBucketsExact(ctx, db, "pr-partner-c.test", outageEnd)
	if err != nil {
		return reliabilityPhaseAResult{}, fmt.Errorf("query retry buckets: %w", err)
	}

	const partnerAP99LimitMs = 2000.0
	partnerAOK := partnerA.P99Ms < partnerAP99LimitMs
	jitterLimit := max((jitter.Total+3)/4, profile.WorkerCount*2)
	jitterOK := jitter.Total == 0 || jitter.MaxPerSecond <= jitterLimit

	return reliabilityPhaseAResult{
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
// Phase B — Terminal 4xx Fast-Fail
// ---------------------------------------------------------------------------

type reliabilityPhaseBResult struct {
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

func runReliabilityPhaseB(ctx context.Context, db *sql.DB, profile reliabilityProfile) (reliabilityPhaseBResult, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("reset: %w", err)
	}

	eventCount := profile.EventsPerCustomer
	customerID := "pr-phase-b-customer"

	rs := receiver.NewServer(
		receiver.WithStableDecisions("pr-phase-b"),
		receiver.WithRouteStatus("/hooks/misconfigured-404", 1.0, http.StatusNotFound),
	)
	defer rs.Close()

	if _, err := db.ExecContext(ctx, `INSERT INTO customers (id, class) VALUES ($1, 1) ON CONFLICT DO NOTHING`, customerID); err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("seed customer: %w", err)
	}
	goodURL := rs.URL() + "/hooks/good"
	badURL := rs.URL() + "/hooks/misconfigured-404"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES
		($1, $2, '', '{bench.test}', true),
		($1, $3, '', '{bench.test}', true)
		ON CONFLICT DO NOTHING
	`, customerID, goodURL, badURL); err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("seed webhooks: %w", err)
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
				slog.Error("reliability phase-B worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	defer cancelDiscovery()
	discoveryDone := make(chan error, 1)
	go func() {
		discoveryDone <- worker.RunClaimableCustomerDiscovery(discoveryCtx, outboxStore, plainSched, reliabilityDiscoveryConfig(profile))
	}()

	for range eventCount {
		if _, err := ingestStore.Submit(ctx, ingest.Event{
			CustomerID: customerID,
			Type:       eventType,
			Payload:    []byte(`{"event_name":"subscriber.created"}`),
		}); err != nil {
			return reliabilityPhaseBResult{}, fmt.Errorf("submit: %w", err)
		}
	}

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelPoll()
	for {
		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return reliabilityPhaseBResult{}, fmt.Errorf("poll: %w", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			return reliabilityPhaseBResult{}, fmt.Errorf("drain timeout: %d pending", pending)
		case <-time.After(time.Second):
		}
	}
	cancelDiscovery()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return reliabilityPhaseBResult{}, fmt.Errorf("claimable discovery: %w", err)
	}
	cancelWorkers()
	workerWg.Wait()

	expectedDeliveries := eventCount * 2
	totalDeliveries, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("query total deliveries: %w", err)
	}
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("query terminal deliveries: %w", err)
	}

	good, err := reliabilityQueryWebhookResult(ctx, db, goodURL)
	if err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("query good webhook: %w", err)
	}
	bad, err := reliabilityQueryWebhookResult(ctx, db, badURL)
	if err != nil {
		return reliabilityPhaseBResult{}, fmt.Errorf("query bad webhook: %w", err)
	}

	return reliabilityPhaseBResult{
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

type reliabilityWebhookResult struct {
	Total      int
	Succeeded  int
	Dead       int
	RetryWaste int
}

func reliabilityQueryWebhookResult(ctx context.Context, db *sql.DB, webhookURL string) (reliabilityWebhookResult, error) {
	var r reliabilityWebhookResult
	err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE d.status = 'succeeded'),
			COUNT(*) FILTER (WHERE d.status = 'dead'),
			COUNT(*) FILTER (WHERE d.attempt_count > 1)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url = $1
	`, webhookURL).Scan(&r.Total, &r.Succeeded, &r.Dead, &r.RetryWaste)
	return r, err
}

// ---------------------------------------------------------------------------
// Partner Protection — Dead Partner Isolation
// ---------------------------------------------------------------------------

type partnerProtectionResult struct {
	ControlDuration    time.Duration
	DeadDuration       time.Duration
	WorstControlP95Ms  float64
	WorstDeadP95Ms     float64
	WorstHealthyRatio  float64
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
	partnerProtectionControlSeed    = "partner-protection-control"
	partnerProtectionDeadSeed       = "partner-protection-dead"
	partnerProtectionHealthyLatency = 10 * time.Millisecond
)

func runPartnerProtection(ctx context.Context, db *sql.DB, profile reliabilityProfile) (partnerProtectionResult, error) {
	control, err := runPartnerProtectionRun(ctx, db, profile, false)
	if err != nil {
		return partnerProtectionResult{}, fmt.Errorf("control run: %w", err)
	}
	dead, err := runPartnerProtectionRun(ctx, db, profile, true)
	if err != nil {
		return partnerProtectionResult{}, fmt.Errorf("dead-partner run: %w", err)
	}

	worstControlP95, worstDeadP95, worstRatio, withinLimit, err := comparePartnerProtectionLatencies(control.HealthyLatencies, dead.HealthyLatencies)
	if err != nil {
		return partnerProtectionResult{}, err
	}

	goroutineDelta := dead.FinalGoroutines - dead.BaselineGoroutines
	goroutineOK := goroutineDelta <= 8
	deadAttemptsLimit := profile.DeliveriesPerCustomer
	deadAttemptsOK := dead.DeadAttempts > 0 && dead.DeadAttempts <= deadAttemptsLimit
	skippedOK := dead.DeadPending > 0

	return partnerProtectionResult{
		ControlDuration:    control.Duration,
		DeadDuration:       dead.Duration,
		WorstControlP95Ms:  worstControlP95,
		WorstDeadP95Ms:     worstDeadP95,
		WorstHealthyRatio:  worstRatio,
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

func comparePartnerProtectionLatencies(control, dead []healthyCustomerLatency) (float64, float64, float64, bool, error) {
	if len(control) == 0 || len(control) != len(dead) {
		return 0, 0, 0, false, fmt.Errorf("healthy latency rows mismatch: control=%d dead=%d", len(control), len(dead))
	}

	withinLimit := true
	var worstControlP95, worstDeadP95, worstRatio float64
	for i := range control {
		if control[i].CustomerID != dead[i].CustomerID {
			return 0, 0, 0, false, fmt.Errorf("healthy latency row mismatch at %d: control=%s dead=%s", i, control[i].CustomerID, dead[i].CustomerID)
		}
		if control[i].ControlP95 <= 0 {
			return 0, 0, 0, false, fmt.Errorf("control P95 is zero: cannot compare latencies for %s", control[i].CustomerID)
		}

		ratio := dead[i].ControlP95 / control[i].ControlP95
		if ratio > worstRatio {
			worstControlP95 = control[i].ControlP95
			worstDeadP95 = dead[i].ControlP95
			worstRatio = ratio
		}
		if ratio > 1.10 {
			withinLimit = false
		}
	}
	return worstControlP95, worstDeadP95, worstRatio, withinLimit, nil
}

type partnerProtectionRunResult struct {
	Duration           time.Duration
	BaselineGoroutines int
	FinalGoroutines    int
	HealthyLatencies   []healthyCustomerLatency
	DeadAttempts       int
	DeadPending        int
	BreakerOpen        bool
	BreakerHalfOpen    bool
	BreakerReopened    bool
}

func runPartnerProtectionRun(ctx context.Context, db *sql.DB, profile reliabilityProfile, deadPartner bool) (partnerProtectionRunResult, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return partnerProtectionRunResult{}, fmt.Errorf("reset: %w", err)
	}

	customerCount := profile.CustomerCount
	seed := partnerProtectionControlSeed
	if deadPartner {
		seed = partnerProtectionDeadSeed
	}

	opts := []receiver.Option{receiver.WithStableDecisions(seed)}
	for i := 0; i < customerCount; i++ {
		path := fmt.Sprintf("/hooks/pr-customer-%02d", i+1)
		if deadPartner && i == 0 {
			opts = append(opts, receiver.WithRouteHang(path))
		} else {
			opts = append(opts, receiver.WithRouteLatency(path, 1.0, partnerProtectionHealthyLatency))
		}
	}
	rs := receiver.NewServer(opts...)
	defer rs.Close()

	ids := make([]string, customerCount)
	for i := range ids {
		if i == 0 {
			ids[i] = "partner-protection-zdead-customer"
		} else {
			ids[i] = fmt.Sprintf("partner-protection-customer-%02d", i)
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
		return partnerProtectionRunResult{}, fmt.Errorf("seed customers: %w", err)
	}

	webhookArgs := make([]any, 0, customerCount*4)
	phaseCHosts := make([]string, 0, customerCount)
	for i, id := range ids {
		host := fmt.Sprintf("partner-protection-%02d.test", i+1)
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
		return partnerProtectionRunResult{}, fmt.Errorf("seed webhooks: %w", err)
	}

	for _, customerID := range ids {
		var webhookID string
		if err := db.QueryRowContext(ctx, `SELECT id::text FROM webhooks WHERE customer_id = $1 ORDER BY created_at LIMIT 1`, customerID).Scan(&webhookID); err != nil {
			return partnerProtectionRunResult{}, fmt.Errorf("lookup webhook: %w", err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return partnerProtectionRunResult{}, fmt.Errorf("begin tx: %w", err)
		}
		var eventID string
		if err := tx.QueryRowContext(ctx, `INSERT INTO events (customer_id, type, payload) VALUES ($1, $2, $3) RETURNING id`,
			customerID, eventType, []byte(`{}`)).Scan(&eventID); err != nil {
			_ = tx.Rollback()
			return partnerProtectionRunResult{}, fmt.Errorf("insert event: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
			SELECT $1, $2, $3, 'pending', 0, now() FROM generate_series(1, $4)`,
			eventID, webhookID, customerID, profile.DeliveriesPerCustomer); err != nil {
			_ = tx.Rollback()
			return partnerProtectionRunResult{}, fmt.Errorf("insert deliveries: %w", err)
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return partnerProtectionRunResult{}, fmt.Errorf("commit: %w", err)
		}
	}

	baseline := runtime.NumGoroutine()
	sched := roundrobin.New()
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0))
	webhookBreaker, err := newBenchBreaker(ctx, db, profile.BreakerFailureThreshold, profile.BreakerInitialCooldown, profile.BreakerMaxCooldown)
	if err != nil {
		return partnerProtectionRunResult{}, err
	}
	httpClient, err := reliabilityHTTPClientForHosts(rs.URL(), phaseCHosts)
	if err != nil {
		return partnerProtectionRunResult{}, fmt.Errorf("http client: %w", err)
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
				slog.Error("partner-protection worker failed", "worker_id", id, "error", err)
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
		totalTimeout, pollInterval := partnerProtectionPollingConfig()
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
				return partnerProtectionRunResult{}, fmt.Errorf("poll breaker state: %w", err)
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
			return partnerProtectionRunResult{}, fmt.Errorf(
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
			return partnerProtectionRunResult{}, fmt.Errorf("poll healthy deliveries: %w", err)
		}
		if n == 0 {
			break
		}
		select {
		case <-pollCtx.Done():
			return partnerProtectionRunResult{}, fmt.Errorf("healthy drain timeout: %d pending", n)
		case <-time.After(250 * time.Millisecond):
		}
	}

	cancelDiscovery()
	cancelWorkers()
	workerWg.Wait()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return partnerProtectionRunResult{}, fmt.Errorf("claimable discovery: %w", err)
	}

	rs.Close()
	time.Sleep(250 * time.Millisecond)

	result := partnerProtectionRunResult{
		Duration:           time.Since(started),
		BaselineGoroutines: baseline,
		FinalGoroutines:    runtime.NumGoroutine(),
		BreakerOpen:        breakerOpen,
		BreakerHalfOpen:    breakerHalfOpen,
		BreakerReopened:    breakerReopened,
	}

	rows, err := db.QueryContext(ctx, `SELECT d.customer_id, COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (da.sent_at - d.created_at)) * 1000), 0)
		FROM deliveries d JOIN delivery_attempts da ON da.delivery_id = d.id WHERE d.customer_id <> $1 AND d.status = 'succeeded' AND da.attempt_number = 1 GROUP BY d.customer_id ORDER BY d.customer_id`, ids[0])
	if err != nil {
		return partnerProtectionRunResult{}, fmt.Errorf("query healthy latencies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var lat healthyCustomerLatency
		if err := rows.Scan(&lat.CustomerID, &lat.ControlP95); err != nil {
			return partnerProtectionRunResult{}, fmt.Errorf("scan healthy latency: %w", err)
		}
		result.HealthyLatencies = append(result.HealthyLatencies, lat)
	}
	if err := rows.Err(); err != nil {
		return partnerProtectionRunResult{}, fmt.Errorf("iterate healthy latencies: %w", err)
	}

	if deadPartner {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts da JOIN deliveries d ON d.id = da.delivery_id WHERE d.customer_id = $1`, ids[0]).Scan(&result.DeadAttempts); err != nil {
			return partnerProtectionRunResult{}, fmt.Errorf("query dead partner attempts: %w", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE customer_id = $1 AND status NOT IN ('succeeded', 'dead')`, ids[0]).Scan(&result.DeadPending); err != nil {
			return partnerProtectionRunResult{}, fmt.Errorf("query dead partner pending deliveries: %w", err)
		}
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// Helpers (exact URL matching — no LIKE suffix patterns)
// ---------------------------------------------------------------------------

func reliabilityHTTPClient(receiverURL string) (*http.Client, error) {
	hosts := make([]string, 0, len(reliabilityPhaseAPartners))
	for _, p := range reliabilityPhaseAPartners {
		hosts = append(hosts, p.host)
	}
	return reliabilityHTTPClientForHosts(receiverURL, hosts)
}

func reliabilityHTTPClientForHosts(receiverURL string, hosts []string) (*http.Client, error) {
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

func reliabilityPhaseACustomerID(i int) string {
	return fmt.Sprintf("pr-phase-a-customer-%02d", i)
}

func seedReliabilityPhaseACustomers(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, customerCount*2)
	for i := 0; i < customerCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d)", i*2+1, i*2+2)
		args = append(args, reliabilityPhaseACustomerID(i), 1)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert customers: %w", err)
	}
	return nil
}

func seedReliabilityPhaseAWebhooks(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, customerCount*len(reliabilityPhaseAPartners)*4)
	row := 0
	for i := 0; i < customerCount; i++ {
		customerID := reliabilityPhaseACustomerID(i)
		for _, partner := range reliabilityPhaseAPartners {
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

func submitReliabilityPhaseAEvents(ctx context.Context, store *ingest.Store, customerCount, eventsPerCustomer int, interval time.Duration) error {
	for i := 0; i < customerCount; i++ {
		customerID := reliabilityPhaseACustomerID(i)
		for range eventsPerCustomer {
			if _, err := store.Submit(ctx, ingest.Event{
				CustomerID: customerID,
				Type:       eventType,
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

func queryPartnerLatencyExact(ctx context.Context, db *sql.DB, host string) (partnerLatencyStats, error) {
	var stats partnerLatencyStats
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url LIKE 'http://' || $1 || '%'
		  AND d.status IN ('succeeded', 'dead')
	`, host).Scan(&stats.P99Ms)
	return stats, err
}

func queryPartnerRecoveryExact(ctx context.Context, db *sql.DB, host string, outageStart, outageEnd time.Time) (partnerRecoveryStats, error) {
	var stats partnerRecoveryStats
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE d.status = 'succeeded' AND d.updated_at >= $3)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url LIKE 'http://' || $1 || '%'
		  AND d.created_at >= $2 AND d.created_at < $3
	`, host, outageStart, outageEnd).Scan(&stats.Total, &stats.Succeeded)
	return stats, err
}

func queryPartnerRetryBucketsExact(ctx context.Context, db *sql.DB, host string, recoveryAt time.Time) (retryBucketStats, error) {
	var stats retryBucketStats
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

func writeReliabilityReport(path string, profile reliabilityProfile, results reliabilityResults) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Reliability

## Description

Profile **%s**: validates transient partner recovery, retry distribution,
terminal 4xx fast-fail, DLQ classification, and complete terminal outcomes.

## Architecture Settings

| Module | Phase A: transient recovery | Phase B: terminal 4xx |
|---|---|---|
| Topology | %d in-process Workers | 4 in-process Workers |
| Scheduler | Equal-share round-robin | Equal-share round-robin |
| Ingest | Real `+"`ingest.Store.Submit`"+` with Registry cache | Real `+"`ingest.Store.Submit`"+` with Registry cache |
| Discovery | DB discovery: interval %s, empty backoff %s, limit %d | Same discovery configuration |
| Circuit breaker | Disabled in Outbox and Worker to isolate RetryPolicy recovery | Disabled to prove 4xx fast-fail without breaker interference |
| HostLimiter | Enabled: cap 16 per hostname, 100ms Requeue delay | Disabled |
| Retry | 20 attempts, 1s base delay, 15s cap delay; MaxAge disabled | Worker defaults: 8 attempts, 1s base delay, 5m cap delay; MaxAge disabled |
| HTTP | Shared rewritten test client; Worker default 30s request timeout | Default Worker client and 30s request timeout |

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
`,
		profile.Name,
		profile.WorkerCount, profile.DiscoveryInterval,
		profile.DiscoveryEmptyBackoff, profile.DiscoveryLimit,
		results.PhaseA.TotalEvents, results.PhaseA.ExpectedDeliveries,
		results.PhaseA.TotalDeliveries, results.PhaseA.TerminalDeliveries,
		results.PhaseA.PartnerAP99Ms,
		results.PhaseA.RecoverySucceeded, results.PhaseA.RecoveryTotal,
		results.PhaseA.JitterMaxPerSecond, results.PhaseA.JitterTotal,
		passFail(results.PhaseA.AllTerminalOK), passFail(results.PhaseA.RecoveryOK),
		passFail(results.PhaseA.PartnerAOK), passFail(results.PhaseA.JitterOK),
		results.PhaseB.EventCount, results.PhaseB.ExpectedDeliveries,
		results.PhaseB.TotalDeliveries, results.PhaseB.TerminalDeliveries,
		results.PhaseB.GoodSucceeded, results.PhaseB.GoodTotal,
		results.PhaseB.BadDead, results.PhaseB.BadTotal,
		results.PhaseB.RetryWaste,
		passFail(results.PhaseB.AllTerminalOK), passFail(results.PhaseB.GoodOK), passFail(results.PhaseB.BadOK),
	)
	return f.Close()
}

func writePartnerProtectionReport(path string, profile reliabilityProfile, result partnerProtectionResult) error {
	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Partner Protection

## Description

Profile **%s**: validates the dashboard's Partner Protection category with one
dead Integration Partner and healthy Customers on distinct hostnames. The hard
gates cover breaker transitions, bounded dead-partner Attempts, skipped backlog,
stable goroutine count, and healthy-Customer First-Attempt Queue Latency.

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | %d in-process Workers sharing one Postgres database and Scheduler |
| Scheduler | Equal-share round-robin; weighted scheduling disabled |
| Ingest | Bypassed; one Event and %d Deliveries per Customer are seeded directly |
| Discovery | DB discovery: interval %s, empty backoff %s, limit %d; no PgNotify path |
| Circuit breaker | Enabled: in-memory ADR-0014 decisions plus persisted claim filtering; threshold %d, cooldown %s, max cooldown %s |
| HostLimiter | Enabled with cap 1 per hostname, 50ms Requeue delay; each Customer uses a distinct hostname |
| Worker execution | `+"`MinBatchHint=1`"+`, `+"`DeliveryConcurrency=1`"+` per Worker |
| Retry | 20 attempts, fixed 50ms retry delay; request timeout %s; MaxAge disabled |

| Metric | Control | Dead Partner |
|---|---:|---:|
| Duration | %s | %s |
| Worst healthy Customer p95 First-Attempt Queue Latency | %.2f ms | %.2f ms |
| Worst healthy Customer ratio | - | %.2fx |
| Goroutines (baseline to final) | %d | %d |
| Dead-customer Attempts | - | %d |
| Dead-customer Attempt cap | - | %d |
| Dead-customer pending after breaker open | - | %d |

| Gate | Result |
|---|---|
| Healthy Customer p95 within 10%% of control | %s |
| Breaker opened | %s |
| Half-open probe observed | %s |
| Breaker re-opened after probe | %s |
| Goroutine count stable | %s |
| Dead-customer Attempt cap met | %s |
| Deliveries skipped after breaker open | %s |
`,
		profile.Name,
		profile.WorkerCount, profile.DeliveriesPerCustomer,
		profile.DiscoveryInterval, profile.DiscoveryEmptyBackoff,
		profile.DiscoveryLimit, profile.BreakerFailureThreshold,
		profile.BreakerInitialCooldown, profile.BreakerMaxCooldown,
		profile.RequestTimeout,
		result.ControlDuration.Round(time.Millisecond), result.DeadDuration.Round(time.Millisecond),
		result.WorstControlP95Ms, result.WorstDeadP95Ms, result.WorstHealthyRatio,
		result.BaselineGoroutines, result.FinalGoroutines,
		result.DeadAttempts, result.DeadAttemptsLimit, result.DeadPending,
		passFail(result.HealthyWithinLimit), passFail(result.BreakerOpened),
		passFail(result.BreakerHalfOpen), passFail(result.BreakerReopened),
		passFail(result.GoroutineOK), passFail(result.DeadAttemptsOK), passFail(result.SkippedOK),
	)
	return f.Close()
}
