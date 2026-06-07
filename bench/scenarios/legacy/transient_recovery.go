package legacy

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
	"strings"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/bench/scenarios"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const partnerAP99Limit = 2 * time.Second

var transientRecoveryPartners = []struct {
	name string
	host string
	path string
}{
	{name: "Partner A", host: "tr-partner-a.test", path: "/hooks/partner-a"},
	{name: "Partner B", host: "tr-partner-b.test", path: "/hooks/partner-b"},
	{name: "Partner C", host: "tr-partner-c.test", path: "/hooks/partner-c"},
}

func transientRecoveryCustomerID(i int) string {
	return fmt.Sprintf("transient-recovery-customer-%02d", i)
}

type transientRecoveryProfile struct {
	Name                    string
	CustomerCount           int
	EventsPerCustomer       int
	SubmitInterval          time.Duration
	WorkerCount             int
	OutageDelay             time.Duration
	OutageDuration          time.Duration
	BreakerFailureThreshold int
}

// RunTransientRecovery executes the legacy Transient Recovery, folded into
// partner_resilience.go and kept for reference until retirement.
func RunTransientRecovery(ctx context.Context) error {
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

	profile, err := transientRecoveryProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	scenarioStart := time.Now()
	outageStart := scenarioStart.Add(profile.OutageDelay)
	outageEnd := outageStart.Add(profile.OutageDuration)

	rs := receiver.NewServer(
		receiver.WithStableDecisions("transient-recovery"),
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
	expectedDeliveries := totalEvents * len(transientRecoveryPartners)

	if err := seedTransientRecoveryCustomers(ctx, db, profile.CustomerCount); err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}
	if err := seedTransientRecoveryWebhooks(ctx, db, profile.CustomerCount); err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}

	plainSched := roundrobin.New()
	reg := registry.New(db)
	ingestStore := ingest.New(db, reg, plainSched)
	outboxStore := outbox.New(db, outbox.WithBreakerFiltering(profile.BreakerFailureThreshold > 0))

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	client, err := transientRecoveryHTTPClient(rs.URL())
	if err != nil {
		return fmt.Errorf("create scenario http client: %w", err)
	}
	hostLimiter := worker.NewHostLimiter(worker.HostLimiterConfig{
		MaxPerHost:     16,
		AcquireTimeout: 250 * time.Millisecond,
		RequeueDelay:   100 * time.Millisecond,
	})
	workerLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	var workerWg sync.WaitGroup
	workers := profile.WorkerCount
	for i := range workers {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			w := worker.New(
				plainSched,
				worker.WithHTTPClient(client),
				worker.WithHostLimiter(hostLimiter),
				worker.WithRetryPolicy(transientRecoveryRetryPolicy()),
				worker.WithLogger(workerLog),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	defer cancelDiscovery()
	discoveryDone := make(chan error, 1)
	go func() {
		discoveryDone <- worker.RunClaimableCustomerDiscovery(discoveryCtx, outboxStore, plainSched, worker.DiscoveryConfig{
			Interval:     50 * time.Millisecond,
			EmptyBackoff: 50 * time.Millisecond,
			ErrorBackoff: 50 * time.Millisecond,
			MaxBackoff:   250 * time.Millisecond,
			Limit:        100,
		})
	}()
	slog.Info("started workers", "profile", profile.Name, "count", workers)

	if err := submitTransientRecoveryEvents(ctx, ingestStore, profile.CustomerCount, profile.EventsPerCustomer, profile.SubmitInterval); err != nil {
		return fmt.Errorf("submit events: %w", err)
	}
	slog.Info("submitted transient recovery events", "events", totalEvents, "expected_deliveries", expectedDeliveries)

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelPoll()
	for {
		pending, err := scenarios.CountPendingDeliveries(pollCtx, db)
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

	cancelDiscovery()
	if err := <-discoveryDone; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("claimable discovery: %w", err)
	}
	cancelWorkers()
	workerWg.Wait()

	totalDuration := time.Since(scenarioStart)
	reportPath := "bench/results/transient-recovery.md"
	if err := writeTransientRecoveryReport(ctx, db, reportPath, transientRecoveryReportParams{
		profileName:         profile.Name,
		totalDuration:       totalDuration,
		totalEvents:         totalEvents,
		expectedDeliveries:  expectedDeliveries,
		customerCount:       profile.CustomerCount,
		eventsPerCustomer:   profile.EventsPerCustomer,
		submitInterval:      profile.SubmitInterval,
		workerCount:         workers,
		outageStart:         outageStart,
		outageEnd:           outageEnd,
		jitterBucketPercent: 0.25,
	}); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	slog.Info("transient recovery complete", "profile", profile.Name, "report", reportPath)
	return nil
}

func transientRecoveryProfileFromEnv() (transientRecoveryProfile, error) {
	raw, name, err := scenarios.LoadBenchProfileConfig("transient_recovery")
	if err != nil {
		return transientRecoveryProfile{}, err
	}
	if raw.CustomerCount < 1 || raw.EventsPerCustomer < 1 || raw.WorkerCount < 1 {
		return transientRecoveryProfile{}, fmt.Errorf("transient_recovery profile %q has invalid positive counts", name)
	}
	if raw.SubmitInterval.Duration() < 0 || raw.OutageDelay.Duration() < 0 || raw.OutageDuration.Duration() < 0 {
		return transientRecoveryProfile{}, fmt.Errorf("transient_recovery profile %q has invalid durations", name)
	}
	if raw.BreakerFailureThreshold < 0 {
		return transientRecoveryProfile{}, fmt.Errorf("transient_recovery profile %q has invalid breaker threshold", name)
	}

	return transientRecoveryProfile{
		Name:                    name,
		CustomerCount:           raw.CustomerCount,
		EventsPerCustomer:       raw.EventsPerCustomer,
		SubmitInterval:          raw.SubmitInterval.Duration(),
		WorkerCount:             raw.WorkerCount,
		OutageDelay:             raw.OutageDelay.Duration(),
		OutageDuration:          raw.OutageDuration.Duration(),
		BreakerFailureThreshold: raw.BreakerFailureThreshold,
	}, nil
}

func transientRecoveryHTTPClient(receiverURL string) (*http.Client, error) {
	u, err := url.Parse(receiverURL)
	if err != nil {
		return nil, fmt.Errorf("parse receiver url: %w", err)
	}
	receiverAddr := u.Host
	fakeHosts := make(map[string]bool, len(transientRecoveryPartners))
	for _, p := range transientRecoveryPartners {
		fakeHosts[p.host] = true
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

func transientRecoveryRetryPolicy() retry.RetryPolicy {
	return retry.New(retry.Config{
		MaxAttempts: 20,
		BaseDelay:   retry.DefaultBaseDelay,
		CapDelay:    15 * time.Second,
	})
}

func seedTransientRecoveryCustomers(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, class) VALUES ")
	args := make([]any, 0, customerCount*2)
	for i := 0; i < customerCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d)", i*2+1, i*2+2)
		args = append(args, transientRecoveryCustomerID(i), 1)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert customers: %w", err)
	}
	return nil
}

func seedTransientRecoveryWebhooks(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")

	args := make([]any, 0, customerCount*len(transientRecoveryPartners)*4)
	row := 0
	for i := 0; i < customerCount; i++ {
		customerID := transientRecoveryCustomerID(i)
		for _, partner := range transientRecoveryPartners {
			if row > 0 {
				sb.WriteString(", ")
			}
			base := row * 4
			fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", base+1, base+2, base+3, base+4)
			args = append(args,
				customerID,
				fmt.Sprintf("http://%s%s", partner.host, partner.path),
				"",
				fmt.Sprintf("{%s}", scenarios.EventType),
			)
			row++
		}
	}
	sb.WriteString(" ON CONFLICT DO NOTHING")

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert webhooks: %w", err)
	}
	return nil
}

func submitTransientRecoveryEvents(ctx context.Context, store *ingest.Store, customerCount int, eventsPerCustomer int, interval time.Duration) error {
	for i := 0; i < customerCount; i++ {
		customerID := transientRecoveryCustomerID(i)
		for range eventsPerCustomer {
			if _, err := store.Submit(ctx, ingest.Event{
				CustomerID: customerID,
				Type:       scenarios.EventType,
				Payload:    []byte(`{"event_name":"subscriber.created"}`),
			}); err != nil {
				return fmt.Errorf("submit event for %s: %w", customerID, err)
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

type transientRecoveryReportParams struct {
	profileName         string
	totalDuration       time.Duration
	totalEvents         int
	expectedDeliveries  int
	customerCount       int
	eventsPerCustomer   int
	submitInterval      time.Duration
	workerCount         int
	outageStart         time.Time
	outageEnd           time.Time
	jitterBucketPercent float64
}

func writeTransientRecoveryReport(ctx context.Context, db *sql.DB, path string, p transientRecoveryReportParams) error {
	totalDeliveries, err := scenarios.QueryTotalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query total deliveries: %w", err)
	}
	terminal, err := scenarios.QueryTerminalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query terminal deliveries: %w", err)
	}
	partnerAStats, err := queryPartnerLatency(ctx, db, "tr-partner-a.test")
	if err != nil {
		return fmt.Errorf("query partner a latency: %w", err)
	}
	recovery, err := queryPartnerRecovery(ctx, db, "tr-partner-c.test", p.outageStart, p.outageEnd)
	if err != nil {
		return fmt.Errorf("query partner c recovery: %w", err)
	}
	jitter, err := queryPartnerRetryBuckets(ctx, db, "tr-partner-c.test", p.outageEnd)
	if err != nil {
		return fmt.Errorf("query partner c retry buckets: %w", err)
	}

	terminalOK := totalDeliveries == p.expectedDeliveries && terminal == totalDeliveries
	recoveryOK := recovery.Total == recovery.Succeeded
	partnerAOK := time.Duration(partnerAStats.P99Ms*float64(time.Millisecond)) < partnerAP99Limit
	jitterLimit := int(float64(jitter.Total) * p.jitterBucketPercent)
	if jitter.Total > 0 && jitterLimit < 1 {
		jitterLimit = 1
	}
	jitterOK := jitter.Total == 0 || jitter.MaxPerSecond <= jitterLimit

	f, err := os.Create(path) // #nosec G304 -- benchmark report path is a fixed scenario-owned file.
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Transient Recovery: Transient + Recovering Partners

## Description

Profile **%s**: %d Customers, %d Events per Customer, 3 Webhooks per Customer. Each Event fans out to Partner A, Partner B, and Partner C for **%d expected Deliveries**.

- Partner A: healthy.
- Partner B: deterministic 30%% 5xx.
- Partner C: 503 from %s to %s, then recovers.
- Events are submitted every %s so the outage happens while new Deliveries are
  still being enqueued.

## Performance Summary

| Metric | Value |
|---|---:|
| Total duration | %s |
| Events submitted | %d |
| Deliveries created | %d / %d |
| Workers | %d |
| Terminal Deliveries | %d / %d |

## Acceptance Criteria

| Pillar | Criterion | Observed | Limit | Result |
|---|---|---:|---:|---|
| Reliability | 100%% Deliveries terminal | %d / %d | %d / %d | %s |
| Reliability | Partner C outage-window Deliveries succeeded | %d / %d | %d / %d | %s |
| Reliability | Partner A lifecycle p99 unaffected | %.0f ms | < %.0f ms | %s |
| Reliability | Partner C post-recovery retry spike | %d max/s of %d retries | <= %d max/s | %s |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
`,
		p.profileName, p.customerCount, p.eventsPerCustomer, p.expectedDeliveries,
		p.outageStart.Format(time.RFC3339), p.outageEnd.Format(time.RFC3339), p.submitInterval,
		p.totalDuration.Round(time.Second), p.totalEvents, totalDeliveries, p.expectedDeliveries,
		p.workerCount, terminal, totalDeliveries,
		terminal, totalDeliveries, totalDeliveries, totalDeliveries, scenarios.PassFail(terminalOK),
		recovery.Succeeded, recovery.Total, recovery.Total, recovery.Total, scenarios.PassFail(recoveryOK),
		partnerAStats.P99Ms, float64(partnerAP99Limit/time.Millisecond), scenarios.PassFail(partnerAOK),
		jitter.MaxPerSecond, jitter.Total, jitterLimit, scenarios.PassFail(jitterOK),
	)

	for _, partner := range transientRecoveryPartners {
		stats, err := queryPartnerLatency(ctx, db, partner.host)
		if err != nil {
			return fmt.Errorf("query %s latency: %w", partner.name, err)
		}
		_, _ = fmt.Fprintf(f, "| %s | %.0f ms | %.0f ms | %.0f ms | %.0f ms |\n",
			partner.name, stats.P50Ms, stats.P95Ms, stats.P99Ms, stats.MaxMs)
	}

	_, _ = fmt.Fprintf(f, "\n## Attempts Per Delivery\n\n")
	retries, err := scenarios.QueryRetryDistribution(ctx, db)
	if err != nil {
		return fmt.Errorf("query retry distribution: %w", err)
	}
	_, _ = fmt.Fprintf(f, "| Attempts | Deliveries |\n|---|---:|\n")
	for _, r := range retries {
		_, _ = fmt.Fprintf(f, "| %d | %d |\n", r.Attempts, r.Count)
	}

	_, _ = fmt.Fprintf(f, "\n## Delivery Outcomes\n\n")
	outcomes, err := scenarios.QueryDeliveryOutcomes(ctx, db)
	if err != nil {
		return fmt.Errorf("query outcomes: %w", err)
	}
	_, _ = fmt.Fprintf(f, "| Status | Count |\n|---|---:|\n")
	for _, o := range outcomes {
		_, _ = fmt.Fprintf(f, "| %s | %d |\n", o.Status, o.Count)
	}

	return f.Close()
}

func queryPartnerLatency(ctx context.Context, db *sql.DB, host string) (scenarios.PartnerLatencyStats, error) {
	var stats scenarios.PartnerLatencyStats
	err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0),
			COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0),
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0),
			COALESCE(MAX(EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) * 1000), 0)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url LIKE '%' || $1 || '%'
		  AND d.status IN ('succeeded', 'dead')
	`, host).Scan(&stats.P50Ms, &stats.P95Ms, &stats.P99Ms, &stats.MaxMs)
	if err != nil {
		return scenarios.PartnerLatencyStats{}, fmt.Errorf("query partner latency: %w", err)
	}
	return stats, nil
}

func queryPartnerRecovery(ctx context.Context, db *sql.DB, host string, outageStart time.Time, outageEnd time.Time) (scenarios.PartnerRecoveryStats, error) {
	var stats scenarios.PartnerRecoveryStats
	err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE d.status = 'succeeded' AND d.updated_at >= $3)
		FROM deliveries d
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE w.url LIKE '%' || $1 || '%'
		  AND d.created_at >= $2
		  AND d.created_at < $3
	`, host, outageStart, outageEnd).Scan(&stats.Total, &stats.Succeeded)
	if err != nil {
		return scenarios.PartnerRecoveryStats{}, fmt.Errorf("query partner recovery: %w", err)
	}
	return stats, nil
}

func queryPartnerRetryBuckets(ctx context.Context, db *sql.DB, host string, recoveryAt time.Time) (scenarios.RetryBucketStats, error) {
	var stats scenarios.RetryBucketStats
	err := db.QueryRowContext(ctx, `
		WITH buckets AS (
			SELECT date_trunc('second', da.sent_at) AS bucket, COUNT(*) AS attempts
			FROM delivery_attempts da
			JOIN deliveries d ON d.id = da.delivery_id
			JOIN webhooks w ON w.id = d.webhook_id
			WHERE w.url LIKE '%' || $1 || '%'
			  AND da.attempt_number > 1
			  AND da.sent_at >= $2
			GROUP BY bucket
		)
		SELECT COALESCE(SUM(attempts), 0), COALESCE(MAX(attempts), 0)
		FROM buckets
	`, host, recoveryAt).Scan(&stats.Total, &stats.MaxPerSecond)
	if err != nil {
		return scenarios.RetryBucketStats{}, fmt.Errorf("query partner retry buckets: %w", err)
	}
	return stats, nil
}
