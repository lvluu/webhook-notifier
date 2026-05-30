package scenarios

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
	"github.com/lvluu/webhook-notifier/internal/ingest"
	"github.com/lvluu/webhook-notifier/internal/registry"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler/roundrobin"
	"github.com/lvluu/webhook-notifier/internal/worker"
)

const (
	defaultScenario2CustomerCount     = 10
	defaultScenario2EventsPerCustomer = 100
	defaultScenario2SubmitInterval    = 20 * time.Millisecond
	scenario2OutageDelay              = 5 * time.Second
	scenario2OutageDuration           = 30 * time.Second
	partnerAP99Limit                  = 2 * time.Second
)

var scenario2Partners = []struct {
	name string
	host string
	path string
}{
	{name: "Partner A", host: "partner-a.test", path: "/hooks/partner-a"},
	{name: "Partner B", host: "partner-b.test", path: "/hooks/partner-b"},
	{name: "Partner C", host: "partner-c.test", path: "/hooks/partner-c"},
}

func scenario2CustomerID(i int) string {
	return fmt.Sprintf("scenario-2-customer-%02d", i)
}

type scenario2Profile struct {
	Name              string
	CustomerCount     int
	EventsPerCustomer int
	SubmitInterval    time.Duration
	WorkerCount       int
	ReportPath        string
}

// RunScenario2 executes Scenario 2: transient and recovering Integration
// Partners under Event x Webhook fanout.
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

	scenarioStart := time.Now()
	outageStart := scenarioStart.Add(scenario2OutageDelay)
	outageEnd := outageStart.Add(scenario2OutageDuration)

	rs := receiver.NewServer(
		receiver.WithStableDecisions("scenario-2"),
		receiver.WithRouteStatus("/hooks/partner-b", 0.30, http.StatusInternalServerError),
		receiver.WithRouteStatusFunc("/hooks/partner-c", func(_ *http.Request, now time.Time) int {
			if !now.Before(outageStart) && now.Before(outageEnd) {
				return http.StatusServiceUnavailable
			}
			return 0
		}),
	)
	defer rs.Close()

	profile := scenario2ProfileFromEnv()
	totalEvents := profile.CustomerCount * profile.EventsPerCustomer
	expectedDeliveries := totalEvents * len(scenario2Partners)

	if err := seedScenario2Customers(ctx, db, profile.CustomerCount); err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}
	if err := seedScenario2Webhooks(ctx, db, profile.CustomerCount); err != nil {
		return fmt.Errorf("seed webhooks: %w", err)
	}

	plainSched := roundrobin.New()
	reg := registry.New(db)
	ingestStore := ingest.New(db, reg, plainSched)
	outboxStore := baselineOutboxStore(db)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	client, err := scenario2HTTPClient(rs.URL())
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
				worker.WithRetryPolicy(scenario2RetryPolicy()),
				worker.WithLogger(workerLog),
			)
			if err := w.Run(workerCtx, outboxStore); err != nil && workerCtx.Err() == nil {
				slog.Error("worker failed", "worker_id", id, "error", err)
			}
		}(i)
	}
	slog.Info("started workers", "profile", profile.Name, "count", workers)

	if err := submitScenario2Events(ctx, ingestStore, profile.CustomerCount, profile.EventsPerCustomer, profile.SubmitInterval); err != nil {
		return fmt.Errorf("submit events: %w", err)
	}
	slog.Info("submitted scenario 2 events", "events", totalEvents, "expected_deliveries", expectedDeliveries)

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

	totalDuration := time.Since(scenarioStart)
	if err := writeScenario2Report(ctx, db, profile.ReportPath, scenario2ReportParams{
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

	slog.Info("scenario 2 complete", "profile", profile.Name, "report", profile.ReportPath)
	return nil
}

func scenario2ProfileFromEnv() scenario2Profile {
	name := os.Getenv("BENCH_PROFILE")
	if name == "" {
		name = "baseline"
	}
	reportPath := os.Getenv("BENCH_REPORT_PATH")
	if reportPath == "" {
		reportPath = "bench/results/scenario-2.md"
	}
	return scenario2Profile{
		Name:              name,
		CustomerCount:     scenario2CustomerCount(),
		EventsPerCustomer: scenario2EventsPerCustomer(),
		SubmitInterval:    scenario2SubmitInterval(),
		WorkerCount:       scenarioWorkerCount(),
		ReportPath:        reportPath,
	}
}

func scenario2CustomerCount() int {
	raw := os.Getenv("BENCH_SCENARIO2_CUSTOMERS")
	if raw == "" {
		return defaultScenario2CustomerCount
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return defaultScenario2CustomerCount
	}
	return n
}

func scenario2EventsPerCustomer() int {
	raw := os.Getenv("BENCH_EVENTS_PER_CUSTOMER")
	if raw == "" {
		return defaultScenario2EventsPerCustomer
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return defaultScenario2EventsPerCustomer
	}
	return n
}

func scenario2SubmitInterval() time.Duration {
	raw := os.Getenv("BENCH_SUBMIT_INTERVAL")
	if raw == "" {
		return defaultScenario2SubmitInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return defaultScenario2SubmitInterval
	}
	return d
}

func scenario2HTTPClient(receiverURL string) (*http.Client, error) {
	u, err := url.Parse(receiverURL)
	if err != nil {
		return nil, fmt.Errorf("parse receiver url: %w", err)
	}
	receiverAddr := u.Host
	fakeHosts := make(map[string]bool, len(scenario2Partners))
	for _, p := range scenario2Partners {
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

func scenario2RetryPolicy() retry.RetryPolicy {
	return retry.New(retry.Config{
		MaxAttempts: 20,
		BaseDelay:   retry.DefaultBaseDelay,
		CapDelay:    15 * time.Second,
	})
}

func seedScenario2Customers(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO customers (id, weight) VALUES ")
	args := make([]any, 0, customerCount*2)
	for i := 0; i < customerCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "($%d, $%d)", i*2+1, i*2+2)
		args = append(args, scenario2CustomerID(i), 1)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert customers: %w", err)
	}
	return nil
}

func seedScenario2Webhooks(ctx context.Context, db *sql.DB, customerCount int) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")

	args := make([]any, 0, customerCount*len(scenario2Partners)*4)
	row := 0
	for i := 0; i < customerCount; i++ {
		customerID := scenario2CustomerID(i)
		for _, partner := range scenario2Partners {
			if row > 0 {
				sb.WriteString(", ")
			}
			base := row * 4
			fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", base+1, base+2, base+3, base+4)
			args = append(args,
				customerID,
				fmt.Sprintf("http://%s%s", partner.host, partner.path),
				"",
				fmt.Sprintf("{%s}", eventType),
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

func submitScenario2Events(ctx context.Context, store *ingest.Store, customerCount int, eventsPerCustomer int, interval time.Duration) error {
	for i := 0; i < customerCount; i++ {
		customerID := scenario2CustomerID(i)
		for range eventsPerCustomer {
			if _, err := store.Submit(ctx, ingest.Event{
				CustomerID: customerID,
				Type:       eventType,
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

type scenario2ReportParams struct {
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

func writeScenario2Report(ctx context.Context, db *sql.DB, path string, p scenario2ReportParams) error {
	totalDeliveries, err := queryTotalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query total deliveries: %w", err)
	}
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return fmt.Errorf("query terminal deliveries: %w", err)
	}
	partnerAStats, err := queryPartnerLatency(ctx, db, "partner-a.test")
	if err != nil {
		return fmt.Errorf("query partner a latency: %w", err)
	}
	recovery, err := queryPartnerRecovery(ctx, db, "partner-c.test", p.outageStart, p.outageEnd)
	if err != nil {
		return fmt.Errorf("query partner c recovery: %w", err)
	}
	jitter, err := queryPartnerRetryBuckets(ctx, db, "partner-c.test", p.outageEnd)
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

	_, _ = fmt.Fprintf(f, `# Scenario 2: Transient + Recovering Partners

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

| Criterion | Observed | Limit | Result |
|---|---:|---:|---|
| 100%% Deliveries terminal | %d / %d | %d / %d | %s |
| Partner C outage-window Deliveries succeeded | %d / %d | %d / %d | %s |
| Partner A lifecycle p99 unaffected | %.0f ms | < %.0f ms | %s |
| Partner C post-recovery retry spike | %d max/s of %d retries | <= %d max/s | %s |

## Per-Partner Lifecycle Latency

| Partner | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
`,
		p.profileName, p.customerCount, p.eventsPerCustomer, p.expectedDeliveries,
		p.outageStart.Format(time.RFC3339), p.outageEnd.Format(time.RFC3339), p.submitInterval,
		p.totalDuration.Round(time.Second), p.totalEvents, totalDeliveries, p.expectedDeliveries,
		p.workerCount, terminal, totalDeliveries,
		terminal, totalDeliveries, totalDeliveries, totalDeliveries, passFail(terminalOK),
		recovery.Succeeded, recovery.Total, recovery.Total, recovery.Total, passFail(recoveryOK),
		partnerAStats.P99Ms, float64(partnerAP99Limit/time.Millisecond), passFail(partnerAOK),
		jitter.MaxPerSecond, jitter.Total, jitterLimit, passFail(jitterOK),
	)

	for _, partner := range scenario2Partners {
		stats, err := queryPartnerLatency(ctx, db, partner.host)
		if err != nil {
			return fmt.Errorf("query %s latency: %w", partner.name, err)
		}
		_, _ = fmt.Fprintf(f, "| %s | %.0f ms | %.0f ms | %.0f ms | %.0f ms |\n",
			partner.name, stats.P50Ms, stats.P95Ms, stats.P99Ms, stats.MaxMs)
	}

	_, _ = fmt.Fprintf(f, "\n## Attempts Per Delivery\n\n")
	retries, err := queryRetryDistribution(ctx, db)
	if err != nil {
		return fmt.Errorf("query retry distribution: %w", err)
	}
	_, _ = fmt.Fprintf(f, "| Attempts | Deliveries |\n|---|---:|\n")
	for _, r := range retries {
		_, _ = fmt.Fprintf(f, "| %d | %d |\n", r.Attempts, r.Count)
	}

	_, _ = fmt.Fprintf(f, "\n## Delivery Outcomes\n\n")
	outcomes, err := queryDeliveryOutcomes(ctx, db)
	if err != nil {
		return fmt.Errorf("query outcomes: %w", err)
	}
	_, _ = fmt.Fprintf(f, "| Status | Count |\n|---|---:|\n")
	for _, o := range outcomes {
		_, _ = fmt.Fprintf(f, "| %s | %d |\n", o.Status, o.Count)
	}

	return f.Close()
}

type partnerLatencyStats struct {
	P50Ms float64
	P95Ms float64
	P99Ms float64
	MaxMs float64
}

func queryPartnerLatency(ctx context.Context, db *sql.DB, host string) (partnerLatencyStats, error) {
	var stats partnerLatencyStats
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
		return partnerLatencyStats{}, fmt.Errorf("query partner latency: %w", err)
	}
	return stats, nil
}

type partnerRecoveryStats struct {
	Total     int
	Succeeded int
}

func queryPartnerRecovery(ctx context.Context, db *sql.DB, host string, outageStart time.Time, outageEnd time.Time) (partnerRecoveryStats, error) {
	var stats partnerRecoveryStats
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
		return partnerRecoveryStats{}, fmt.Errorf("query partner recovery: %w", err)
	}
	return stats, nil
}

type retryBucketStats struct {
	Total        int
	MaxPerSecond int
}

func queryPartnerRetryBuckets(ctx context.Context, db *sql.DB, host string, recoveryAt time.Time) (retryBucketStats, error) {
	var stats retryBucketStats
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
		return retryBucketStats{}, fmt.Errorf("query partner retry buckets: %w", err)
	}
	return stats, nil
}

func queryTerminalDeliveries(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE status IN ('succeeded', 'dead')`).Scan(&n)
	return n, err
}
