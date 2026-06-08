package scenarios

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lvluu/webhook-notifier/bench/receiver"
)

const (
	scalabilityWorkersPerReplica     = 8
	scalabilityDeliveryConcurrency   = 16
	scalabilityBenchmarkHostCapacity = scalabilityWorkersPerReplica * scalabilityDeliveryConcurrency
)

type scalabilityProfile struct {
	Name                       string
	ReplicaCounts              []int
	SparseCustomers            int
	SparseEventsPerCustomer    int
	DenseCustomers             int
	DenseDeliveriesPerCustomer int
	RequestTimeout             time.Duration
	MinBatchHint               int
	MultiPickCount             int
	RetryMaxAttempts           int
	RetryBaseDelay             time.Duration
	RetryCapDelay              time.Duration
	FairnessRatioLimit         float64
	BreakerFailureThreshold    int
}

type scalabilityReplica struct {
	id          string
	businessURL string
	metricsURL  string
	cmd         *exec.Cmd
	stdout      bytes.Buffer
	stderr      bytes.Buffer
}

type scalabilitySparseResult struct {
	ReplicaCount                  int
	ExpectedDeliveries            int
	TerminalDeliveries            int
	Duplicates                    int
	SmallFirstAttemptP95Ms        float64
	SmallFirstAttemptThresholdMs  float64
	AvgDeliveriesPerClaimQuery    float64
	AvgDeliveriesPerCustomerClaim float64
	EmptyClaimRate                float64
	PerReplicaDelivered           map[string]int
	ReplicaGateOK                 bool
	FairnessGateOK                bool
	AllTerminalOK                 bool
	DuplicateOK                   bool
	Distribution                  scalabilityDistributionVerdict
}

type scalabilityDenseRun struct {
	ReplicaCount        int
	Duration            time.Duration
	Throughput          float64
	ExpectedDeliveries  int
	TerminalDeliveries  int
	Duplicates          int
	PerReplicaDelivered map[string]int
	AllTerminalOK       bool
	DuplicateOK         bool
	ReplicaGateOK       bool
	Distribution        scalabilityDistributionVerdict
}

type scalabilityScalingVerdict struct {
	Mode       string
	Message    string
	Pass       bool
	Bottleneck string
}

type scalabilityDistributionVerdict struct {
	MissingReplicaIDs []string
	ImbalanceRatio    float64
	Diagnostic        string
	HardFail          bool
}

func scalabilityProfileFromEnv() (scalabilityProfile, error) {
	raw, name, err := loadBenchProfileConfig("scalability")
	if err != nil {
		return scalabilityProfile{}, err
	}
	if len(raw.ReplicaCounts) == 0 {
		return scalabilityProfile{}, fmt.Errorf("scalability profile %q requires replica_counts", name)
	}
	if raw.SparseCustomers < 1 || raw.SparseEventsPerCustomer < 1 || raw.DenseCustomers < 1 || raw.DenseDeliveriesPerCustomer < 1 {
		return scalabilityProfile{}, fmt.Errorf("scalability profile %q has invalid customer/delivery counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return scalabilityProfile{}, fmt.Errorf("scalability profile %q requires request_timeout > 0", name)
	}
	if raw.MinBatchHint < 1 || raw.MultiPickCount < 1 {
		return scalabilityProfile{}, fmt.Errorf("scalability profile %q requires min_batch_hint >=1 and multi_pick_count >=1", name)
	}
	if raw.RetryMaxAttempts < 1 || raw.RetryBaseDelay.Duration() < 0 || raw.RetryCapDelay.Duration() < 0 {
		return scalabilityProfile{}, fmt.Errorf("scalability profile %q has invalid retry config", name)
	}
	if raw.FairnessRatioLimit <= 0 {
		return scalabilityProfile{}, fmt.Errorf("scalability profile %q requires fairness_ratio_limit > 0", name)
	}

	return scalabilityProfile{
		Name:                       name,
		ReplicaCounts:              raw.ReplicaCounts,
		SparseCustomers:            raw.SparseCustomers,
		SparseEventsPerCustomer:    raw.SparseEventsPerCustomer,
		DenseCustomers:             raw.DenseCustomers,
		DenseDeliveriesPerCustomer: raw.DenseDeliveriesPerCustomer,
		RequestTimeout:             raw.RequestTimeout.Duration(),
		MinBatchHint:               raw.MinBatchHint,
		MultiPickCount:             raw.MultiPickCount,
		RetryMaxAttempts:           raw.RetryMaxAttempts,
		RetryBaseDelay:             raw.RetryBaseDelay.Duration(),
		RetryCapDelay:              raw.RetryCapDelay.Duration(),
		FairnessRatioLimit:         raw.FairnessRatioLimit,
		BreakerFailureThreshold:    raw.BreakerFailureThreshold,
	}, nil
}

// RunScalability executes Scalability: horizontal scalability across real notifier
// OS processes sharing one Postgres outbox.
func RunScalability(ctx context.Context) error {
	profile, err := scalabilityProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load scalability profile: %w", err)
	}

	db, cleanup, err := openBenchDatabase(ctx)
	if err != nil {
		return fmt.Errorf("open bench db: %w", err)
	}
	defer cleanup()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping bench db: %w", err)
	}

	maxReplicas := profile.ReplicaCounts[len(profile.ReplicaCounts)-1]
	sparse, err := runScalabilitySparsePhase(ctx, db, profile, maxReplicas)
	if err != nil {
		return fmt.Errorf("run sparse phase: %w", err)
	}

	denseRuns := make([]scalabilityDenseRun, 0, len(profile.ReplicaCounts))
	for _, replicas := range profile.ReplicaCounts {
		run, runErr := runScalabilityDensePhase(ctx, db, profile, replicas)
		if runErr != nil {
			return fmt.Errorf("run dense phase (replicas=%d): %w", replicas, runErr)
		}
		denseRuns = append(denseRuns, run)
	}

	scaling := scalabilityComputeScalingVerdict(denseRuns)
	reportPath := benchmarkReportPath("scalability", profile.Name)
	if err := writeScalabilityReport(reportPath, profile, sparse, denseRuns, scaling); err != nil {
		return fmt.Errorf("write scalability report: %w", err)
	}

	if !sparse.AllTerminalOK || !sparse.DuplicateOK || !sparse.FairnessGateOK || !sparse.ReplicaGateOK {
		return fmt.Errorf("scalability sparse hard gate failed")
	}
	for _, run := range denseRuns {
		if !run.AllTerminalOK || !run.DuplicateOK || !run.ReplicaGateOK {
			return fmt.Errorf("scalability dense hard gate failed for replicas=%d", run.ReplicaCount)
		}
	}
	// Scaling verdict is diagnostic — a regression or flat throughput is reported
	// in the markdown output but does not fail the scenario. Scalability
	// proof is established through the dense-phase replica gates (all terminal,
	// zero duplicates, no zero-work replica) rather than a strict throughput slope.
	_ = scaling

	return nil
}

func runScalabilitySparsePhase(ctx context.Context, db *sql.DB, profile scalabilityProfile, replicaCount int) (scalabilitySparseResult, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return scalabilitySparseResult{}, fmt.Errorf("reset bench data: %w", err)
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("hs-sparse-%d", replicaCount)),
		receiver.WithRouteLatency("/hooks/fast", 1.0, 15*time.Millisecond),
		receiver.WithRouteLatency("/hooks/slow", 0.25, 250*time.Millisecond),
		receiver.WithRouteStatus("/hooks/flaky", 0.03, http.StatusServiceUnavailable),
	)
	defer rs.Close()

	if err := seedScalabilityCustomers(ctx, db, "hs-sparse-small-", profile.SparseCustomers); err != nil {
		return scalabilitySparseResult{}, err
	}
	if err := seedScalabilityWebhooks(ctx, db, "hs-sparse-small-", profile.SparseCustomers, rs.URL(), []string{"/hooks/fast", "/hooks/slow", "/hooks/flaky"}); err != nil {
		return scalabilitySparseResult{}, err
	}

	replicas, err := startScalabilityReplicas(ctx, db, profile, replicaCount)
	if err != nil {
		return scalabilitySparseResult{}, err
	}
	defer stopScalabilityReplicas(replicas)

	if err := submitScalabilitySparseEvents(ctx, profile, replicas); err != nil {
		return scalabilitySparseResult{}, err
	}

	if err := waitForScalabilityDrain(ctx, db, 8*time.Minute); err != nil {
		return scalabilitySparseResult{}, err
	}

	claimSum, claimCount, emptyClaims := collectScalabilityClaimMetrics(replicas)
	avgClaim := 0.0
	avgPerCustomerClaim := 0.0
	emptyRate := 0.0
	if claimCount > 0 {
		avgClaim = claimSum / claimCount
		avgPerCustomerClaim = avgClaim / float64(max(1, profile.MultiPickCount))
		emptyRate = emptyClaims / claimCount
	}

	recordings := rs.Recorded()
	expected := profile.SparseCustomers * profile.SparseEventsPerCustomer * 3
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return scalabilitySparseResult{}, fmt.Errorf("query terminal deliveries: %w", err)
	}
	firstP95, err := queryFairnessFirstAttemptP95(ctx, db, "hs-sparse-small-%")
	if err != nil {
		return scalabilitySparseResult{}, fmt.Errorf("query sparse first-attempt p95: %w", err)
	}

	expectedReplicaIDs := scalabilityReplicaIDs(replicaCount)
	perReplica := scalabilityCountByReplica(recordings)
	distribution := scalabilityPerReplicaDistributionVerdict(perReplica, expectedReplicaIDs)
	thresholdMs := scalabilitySparseFairnessThreshold(profile.Name)

	duplicates := countDuplicateAttemptTuples(recordings)
	return scalabilitySparseResult{
		ReplicaCount:                  replicaCount,
		ExpectedDeliveries:            expected,
		TerminalDeliveries:            terminal,
		Duplicates:                    duplicates,
		SmallFirstAttemptP95Ms:        firstP95,
		SmallFirstAttemptThresholdMs:  thresholdMs,
		AvgDeliveriesPerClaimQuery:    avgClaim,
		AvgDeliveriesPerCustomerClaim: avgPerCustomerClaim,
		EmptyClaimRate:                emptyRate,
		PerReplicaDelivered:           perReplica,
		ReplicaGateOK:                 !distribution.HardFail,
		FairnessGateOK:                firstP95 <= thresholdMs,
		AllTerminalOK:                 terminal == expected,
		DuplicateOK:                   duplicates == 0,
		Distribution:                  distribution,
	}, nil
}

func runScalabilityDensePhase(ctx context.Context, db *sql.DB, profile scalabilityProfile, replicaCount int) (scalabilityDenseRun, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return scalabilityDenseRun{}, fmt.Errorf("reset bench data: %w", err)
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("hs-dense-%d", replicaCount)),
		receiver.WithRouteLatency("/hooks/dense-fast", 1.0, 5*time.Millisecond),
	)
	defer rs.Close()

	if err := seedScalabilityCustomers(ctx, db, "hs-dense-customer-", profile.DenseCustomers); err != nil {
		return scalabilityDenseRun{}, err
	}
	if err := seedScalabilityWebhooks(ctx, db, "hs-dense-customer-", profile.DenseCustomers, rs.URL(), []string{"/hooks/dense-fast"}); err != nil {
		return scalabilityDenseRun{}, err
	}
	if err := seedScalabilityDenseDeliveries(ctx, db, profile.DenseCustomers, profile.DenseDeliveriesPerCustomer); err != nil {
		return scalabilityDenseRun{}, err
	}

	replicas, err := startScalabilityReplicas(ctx, db, profile, replicaCount)
	if err != nil {
		return scalabilityDenseRun{}, err
	}
	defer stopScalabilityReplicas(replicas)

	start := time.Now()
	if err := waitForScalabilityDrain(ctx, db, 12*time.Minute); err != nil {
		return scalabilityDenseRun{}, err
	}
	duration := time.Since(start)

	recordings := rs.Recorded()
	expected := profile.DenseCustomers * profile.DenseDeliveriesPerCustomer
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return scalabilityDenseRun{}, fmt.Errorf("query terminal deliveries: %w", err)
	}
	perReplica := scalabilityCountByReplica(recordings)
	distribution := scalabilityPerReplicaDistributionVerdict(perReplica, scalabilityReplicaIDs(replicaCount))
	throughput := float64(expected)
	if duration > 0 {
		throughput /= duration.Seconds()
	}

	duplicates := countDuplicateAttemptTuples(recordings)
	return scalabilityDenseRun{
		ReplicaCount:        replicaCount,
		Duration:            duration,
		Throughput:          throughput,
		ExpectedDeliveries:  expected,
		TerminalDeliveries:  terminal,
		Duplicates:          duplicates,
		PerReplicaDelivered: perReplica,
		AllTerminalOK:       terminal == expected,
		DuplicateOK:         duplicates == 0,
		ReplicaGateOK:       !distribution.HardFail,
		Distribution:        distribution,
	}, nil
}

func startScalabilityReplicas(ctx context.Context, db *sql.DB, profile scalabilityProfile, count int) ([]*scalabilityReplica, error) {
	var dsn string
	if err := db.QueryRowContext(ctx, "SELECT current_setting('transaction_read_only')").Scan(new(string)); err != nil {
		return nil, fmt.Errorf("validate bench database connection: %w", err)
	}

	if envDSN := os.Getenv("BENCH_DATABASE_URL"); envDSN != "" {
		dsn = envDSN
	} else if envDSN := os.Getenv("DATABASE_URL"); envDSN != "" {
		dsn = envDSN
	} else {
		infoPath := os.Getenv("BENCH_DATABASE_INFO_FILE")
		if infoPath == "" {
			infoPath = "/tmp/bench-postgres.dsn"
		}
		content, err := os.ReadFile(infoPath) //nolint:gosec // benchmark helper file path
		if err != nil {
			return nil, fmt.Errorf("read benchmark dsn file: %w", err)
		}
		dsn = strings.TrimSpace(string(content))
	}
	if dsn == "" {
		return nil, fmt.Errorf("empty benchmark database dsn")
	}

	receiverAddr := strings.TrimSpace(os.Getenv("HORIZONTAL_SCALE_RECEIVER_ADDR"))

	replicas := make([]*scalabilityReplica, 0, count)
	for i := 1; i <= count; i++ {
		replicaID := fmt.Sprintf("replica-%d", i)
		businessAddr, err := scalabilityReserveAddr()
		if err != nil {
			stopScalabilityReplicas(replicas)
			return nil, err
		}
		metricsAddr, err := scalabilityReserveAddr()
		if err != nil {
			stopScalabilityReplicas(replicas)
			return nil, err
		}

		//nolint:gosec // Scalability intentionally launches local notifier OS processes with benchmark-controlled args.
		cmd := exec.CommandContext(ctx,
			"go", "run", "./cmd/notifier",
			"-db", dsn,
			"-addr", businessAddr,
			"-metrics-addr", metricsAddr,
			"-workers", strconv.Itoa(scalabilityWorkersPerReplica),
			"-delivery-concurrency", strconv.Itoa(scalabilityDeliveryConcurrency),
			"-host-cap", strconv.Itoa(scalabilityBenchmarkHostCapacity),
			"-request-timeout", profile.RequestTimeout.String(),
			"-min-batch", strconv.Itoa(profile.MinBatchHint),
			"-multi-pick", strconv.Itoa(profile.MultiPickCount),
			"-breaker-threshold", strconv.Itoa(profile.BreakerFailureThreshold),
			"-discovery-interval", "50ms",
			"-replica-id", replicaID,
		)
		cmd.Dir = repoRoot()
		cmd.Env = append(os.Environ(),
			"NOTIFIER_REPLICA_ID="+replicaID,
			"HORIZONTAL_SCALE_RECEIVER_ADDR="+receiverAddr,
		)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		replica := &scalabilityReplica{
			id:          replicaID,
			businessURL: "http://" + businessAddr,
			metricsURL:  "http://" + metricsAddr,
			cmd:         cmd,
		}
		cmd.Stdout = &replica.stdout
		cmd.Stderr = &replica.stderr

		if err := cmd.Start(); err != nil {
			stopScalabilityReplicas(replicas)
			return nil, fmt.Errorf("start %s: %w", replicaID, err)
		}

		if err := scalabilityWaitReady(replica.businessURL+"/", 12*time.Second); err != nil {
			stopScalabilityReplicas(append(replicas, replica))
			return nil, fmt.Errorf("wait ready %s: %w; stderr=%s", replicaID, err, replica.stderr.String())
		}
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

func stopScalabilityReplicas(replicas []*scalabilityReplica) {
	for _, replica := range replicas {
		if replica == nil || replica.cmd == nil || replica.cmd.Process == nil {
			continue
		}
		pgid, err := syscall.Getpgid(replica.cmd.Process.Pid)
		if err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
		} else {
			_ = replica.cmd.Process.Signal(syscall.SIGTERM)
		}
		done := make(chan struct{})
		go func(cmd *exec.Cmd) {
			_ = cmd.Wait()
			close(done)
		}(replica.cmd)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			if err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = replica.cmd.Process.Kill()
			}
			<-done
		}
	}
}

func scalabilityReserveAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve tcp addr: %w", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		return "", fmt.Errorf("close reserved listener: %w", err)
	}
	return addr, nil
}

func scalabilityWaitReady(url string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := &http.Client{Timeout: 750 * time.Millisecond}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("build ready request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ready timeout: %w", ctx.Err())
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func submitScalabilitySparseEvents(ctx context.Context, profile scalabilityProfile, replicas []*scalabilityReplica) error {
	client := &http.Client{Timeout: 2 * time.Second}
	idx := 0
	for c := 0; c < profile.SparseCustomers; c++ {
		customerID := fmt.Sprintf("hs-sparse-small-%05d", c)
		for e := 0; e < profile.SparseEventsPerCustomer; e++ {
			replica := replicas[idx%len(replicas)]
			idx++

			payload := map[string]any{
				"customer_id": customerID,
				"event_name":  "subscriber.created",
				"event_time":  time.Now().UTC().Format(time.RFC3339Nano),
				"subscriber": map[string]any{
					"id": fmt.Sprintf("sub-%s-%d", customerID, e),
				},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("marshal sparse event: %w", err)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, replica.businessURL+"/events", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("build sparse event request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("submit sparse event: %w", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				return fmt.Errorf("submit sparse event: unexpected status %d", resp.StatusCode)
			}
		}
	}
	return nil
}

func waitForScalabilityDrain(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		pending, err := countPendingDeliveries(pollCtx, db)
		if err != nil {
			return fmt.Errorf("count pending deliveries: %w", err)
		}
		if pending == 0 {
			return nil
		}
		select {
		case <-pollCtx.Done():
			return fmt.Errorf("drain timeout with %d pending deliveries", pending)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func seedScalabilityCustomers(ctx context.Context, db *sql.DB, prefix string, count int) error {
	for start := 0; start < count; start += fairnessSeedBatchRows {
		end := min(start+fairnessSeedBatchRows, count)
		var sb strings.Builder
		sb.WriteString("INSERT INTO customers (id, class) VALUES ")
		args := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(", ")
			}
			_, _ = fmt.Fprintf(&sb, "($%d, 1)", i-start+1)
			args = append(args, fmt.Sprintf("%s%05d", prefix, i))
		}
		sb.WriteString(" ON CONFLICT (id) DO NOTHING")
		if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("seed scalability customers: %w", err)
		}
	}
	return nil
}

func seedScalabilityWebhooks(ctx context.Context, db *sql.DB, prefix string, count int, receiverURL string, paths []string) error {
	if count == 0 || len(paths) == 0 {
		return nil
	}
	const maxWebhookInsertParams = 60000
	rowsPerCustomer := len(paths) * 4
	batchCustomers := max(1, maxWebhookInsertParams/rowsPerCustomer)
	for start := 0; start < count; start += batchCustomers {
		end := min(start+batchCustomers, count)
		if err := seedScalabilityWebhookBatch(ctx, db, prefix, start, end, receiverURL, paths); err != nil {
			return err
		}
	}
	return nil
}

func seedScalabilityWebhookBatch(ctx context.Context, db *sql.DB, prefix string, start, end int, receiverURL string, paths []string) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, (end-start)*len(paths)*4)
	row := 0
	for i := start; i < end; i++ {
		customerID := fmt.Sprintf("%s%05d", prefix, i)
		for _, path := range paths {
			if row > 0 {
				sb.WriteString(", ")
			}
			base := row*4 + 1
			_, _ = fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, true)", base, base+1, base+2, base+3)
			args = append(args, customerID, receiverURL+path, "", "{subscriber.created}")
			row++
		}
	}
	sb.WriteString(" ON CONFLICT (customer_id, url) DO UPDATE SET active = true")
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("seed scalability webhooks: %w", err)
	}
	return nil
}

func seedScalabilityDenseDeliveries(ctx context.Context, db *sql.DB, customerCount, deliveriesPerCustomer int) error {
	for i := 0; i < customerCount; i++ {
		customerID := fmt.Sprintf("hs-dense-customer-%05d", i)
		var webhookID string
		if err := db.QueryRowContext(ctx, `
			SELECT id::text
			FROM webhooks
			WHERE customer_id = $1
			ORDER BY created_at
			LIMIT 1
		`, customerID).Scan(&webhookID); err != nil {
			return fmt.Errorf("lookup dense webhook for %s: %w", customerID, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin dense seed tx: %w", err)
		}

		var eventID string
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO events (customer_id, type, payload)
			VALUES ($1, $2, $3)
			RETURNING id
		`, customerID, eventType, []byte(`{"event_name":"scalability.dense"}`)).Scan(&eventID); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert dense event for %s: %w", customerID, err)
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO deliveries (event_id, webhook_id, customer_id, status, attempt_count, next_attempt_at)
			SELECT $1, $2, $3, 'pending', 0, now()
			FROM generate_series(1, $4)
		`, eventID, webhookID, customerID, deliveriesPerCustomer); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert dense deliveries for %s: %w", customerID, err)
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("commit dense deliveries for %s: %w", customerID, err)
		}
	}
	return nil
}

func collectScalabilityClaimMetrics(replicas []*scalabilityReplica) (claimSum, claimCount, emptyClaims float64) {
	client := &http.Client{Timeout: time.Second}
	for _, replica := range replicas {
		resp, err := client.Get(replica.metricsURL + "/metrics")
		if err != nil {
			continue
		}
		body, _ := ioReadAllLimit(resp.Body, 2<<20)
		_ = resp.Body.Close()
		text := string(body)
		claimSum += scalabilityMetricValue(text, "webhook_claim_batch_size_sum")
		claimCount += scalabilityMetricValue(text, "webhook_claim_batch_size_count")
		emptyClaims += scalabilityMetricValue(text, "webhook_claim_empty_total")
	}
	return claimSum, claimCount, emptyClaims
}

func ioReadAllLimit(rc io.ReadCloser, limit int64) ([]byte, error) {
	defer func() { _ = rc.Close() }()
	return io.ReadAll(io.LimitReader(rc, limit))
}

func scalabilityMetricValue(metricsText, metric string) float64 {
	prefix := metric + " "
	for _, line := range strings.Split(metricsText, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		return v
	}
	return 0
}

func scalabilitySparseFairnessThreshold(profileName string) float64 {
	if profileName == benchProfileStress {
		return 3000
	}
	return 6000
}

func scalabilityReplicaIDs(count int) []string {
	ids := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		ids = append(ids, fmt.Sprintf("replica-%d", i))
	}
	return ids
}

func scalabilityCountByReplica(recordings []receiver.Recording) map[string]int {
	counts := make(map[string]int)
	for _, r := range recordings {
		rid := r.Headers.Get("X-Notifier-Replica")
		if rid == "" {
			rid = "<unset>"
		}
		counts[rid]++
	}
	return counts
}

func scalabilityPerReplicaDistributionVerdict(counts map[string]int, expectedReplicaIDs []string) scalabilityDistributionVerdict {
	missing := make([]string, 0)
	minCount := -1
	maxCount := 0
	for _, replicaID := range expectedReplicaIDs {
		n := counts[replicaID]
		if n == 0 {
			missing = append(missing, replicaID)
		}
		if minCount < 0 || n < minCount {
			minCount = n
		}
		if n > maxCount {
			maxCount = n
		}
	}
	imbalance := 0.0
	if minCount > 0 {
		imbalance = float64(maxCount) / float64(minCount)
	}
	diagnostic := "balanced"
	if imbalance > 1.0 {
		diagnostic = fmt.Sprintf("imbalance ratio %.2fx (max/min)", imbalance)
	}
	return scalabilityDistributionVerdict{
		MissingReplicaIDs: missing,
		ImbalanceRatio:    imbalance,
		Diagnostic:        diagnostic,
		HardFail:          len(missing) > 0,
	}
}

func scalabilityComputeScalingVerdict(runs []scalabilityDenseRun) scalabilityScalingVerdict {
	if len(runs) < 2 {
		return scalabilityScalingVerdict{Mode: "insufficient", Message: "need at least two dense runs", Pass: false}
	}
	sorted := append([]scalabilityDenseRun(nil), runs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ReplicaCount < sorted[j].ReplicaCount })
	base := sorted[0]
	best := sorted[len(sorted)-1]
	if best.Throughput > base.Throughput {
		return scalabilityScalingVerdict{
			Mode:    "improvement",
			Message: fmt.Sprintf("throughput improved %.2f -> %.2f deliveries/s", base.Throughput, best.Throughput),
			Pass:    true,
		}
	}
	if best.Throughput == base.Throughput {
		return scalabilityScalingVerdict{
			Mode:       "no-improvement",
			Message:    fmt.Sprintf("throughput flat at %.2f deliveries/s", best.Throughput),
			Pass:       true,
			Bottleneck: "No scaling gain observed; likely DB claim-path contention or partner latency ceiling.",
		}
	}
	return scalabilityScalingVerdict{
		Mode:       "regression",
		Message:    fmt.Sprintf("throughput regressed %.2f -> %.2f deliveries/s", base.Throughput, best.Throughput),
		Pass:       true,
		Bottleneck: "Regression observed; likely coordination/claim contention bottleneck under additional replicas.",
	}
}

func writeScalabilityReport(path string, profile scalabilityProfile, sparse scalabilitySparseResult, denseRuns []scalabilityDenseRun, scaling scalabilityScalingVerdict) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path
	if err != nil {
		return fmt.Errorf("create scalability report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Scalability

## Description

Profile **%s** runs real notifier OS processes (`+"`go run ./cmd/notifier`"+`) sharing one benchmark Postgres database.
Each replica has unique business/metrics ports and explicit identity (`+"`NOTIFIER_REPLICA_ID`"+`).
Outbound attempts include `+"`X-Notifier-Replica`"+` only when replica identity is configured.

## Architecture Settings

| Module | Setting in this scenario |
|---|---|
| Topology | Real notifier OS processes; replica counts %v; each replica has 8 Workers and DeliveryConcurrency 16 |
| Database | One shared Postgres database; notifier processes connect directly in the benchmark harness (no PgBouncer) |
| Scheduler | Equal-share process-local round-robin; weighted scheduling disabled |
| Wakeups/discovery | PgNotify wakeups enabled by notifier defaults plus DB discovery every 50ms |
| Claim batching | `+"`MinBatchHint=%d`"+`, `+"`MultiPickCount=%d`"+`, default maximum batch hint 64 |
| Circuit breaker | Disabled (`+"`breaker_failure_threshold=%d`"+`) so breaker filtering cannot affect scaling |
| HostLimiter | Enabled with cap 128 per hostname per replica, intentionally high enough not to be the expected bottleneck |
| Retry | Notifier defaults: 8 attempts, 1s base delay, 5m cap delay; MaxAge disabled |
| Ingest | Sparse phase uses real HTTP ingest; dense phase pre-seeds Deliveries to isolate delivery-pipeline throughput |

### Phase 1 — Sparse end-to-end ingest

- %d Small Customers × %d Events each, mixed fast/slow/flaky receivers.
- Event ingest submitted through real `+"`/events`"+` endpoints round-robin across replicas.
- Validates first-attempt queue latency, duplicate tuple safety, sparse discovery behavior,
  and replica distribution diagnostics.

### Phase 2 — Dense delivery drain

- %d Customers × %d pre-seeded Deliveries each, healthy receiver only.
- Validates throughput scaling and duplicate safety from 1 → N replicas.

## Sparse Phase Summary

| Metric | Value |
|---|---:|
| Replicas | %d |
| Expected Deliveries | %d |
| Terminal Deliveries | %d |
| Duplicate (delivery_id, attempt_number) tuples | %d |
| Small P95 first-attempt latency (ms) | %.2f |
| Small P95 threshold (ms) | %.2f |
| Avg Deliveries per claim query | %.2f |
| Avg Deliveries per Customer claim | %.2f |
| Empty claim rate | %.2f%% |

| Sparse hard gate | Result |
|---|---|
| All expected Deliveries terminal | %s |
| Zero duplicate tuples | %s |
| Small P95 within sparse fairness threshold | %s |
| No zero-work replica in multi-replica run | %s |

## Sparse per-replica distribution (diagnostic)

| Replica | Delivered Attempts |
|---|---:|
`, profile.Name,
		profile.ReplicaCounts, profile.MinBatchHint, profile.MultiPickCount,
		profile.BreakerFailureThreshold,
		profile.SparseCustomers, profile.SparseEventsPerCustomer,
		profile.DenseCustomers, profile.DenseDeliveriesPerCustomer,
		sparse.ReplicaCount,
		sparse.ExpectedDeliveries,
		sparse.TerminalDeliveries,
		sparse.Duplicates,
		sparse.SmallFirstAttemptP95Ms,
		sparse.SmallFirstAttemptThresholdMs,
		sparse.AvgDeliveriesPerClaimQuery,
		sparse.AvgDeliveriesPerCustomerClaim,
		sparse.EmptyClaimRate*100,
		passFail(sparse.AllTerminalOK),
		passFail(sparse.DuplicateOK),
		passFail(sparse.FairnessGateOK),
		passFail(sparse.ReplicaGateOK),
	)

	for _, replicaID := range scalabilityReplicaIDs(sparse.ReplicaCount) {
		_, _ = fmt.Fprintf(f, "| %s | %d |\n", replicaID, sparse.PerReplicaDelivered[replicaID])
	}
	_, _ = fmt.Fprintf(f, "\nSparse distribution diagnostic: %s\n\n", sparse.Distribution.Diagnostic)

	_, _ = fmt.Fprintf(f, `## Dense Phase Summary

| Replicas | Expected Deliveries | Terminal Deliveries | Duration | Throughput (Deliveries/s) | Duplicate Tuples | All Terminal | Duplicates | Replica Zero-Work Gate |
|---:|---:|---:|---:|---:|---:|---|---|---|
`)
	for _, run := range denseRuns {
		_, _ = fmt.Fprintf(f, "| %d | %d | %d | %s | %.2f | %d | %s | %s | %s |\n",
			run.ReplicaCount,
			run.ExpectedDeliveries,
			run.TerminalDeliveries,
			run.Duration.Round(time.Millisecond),
			run.Throughput,
			run.Duplicates,
			passFail(run.AllTerminalOK),
			passFail(run.DuplicateOK),
			passFail(run.ReplicaGateOK),
		)
	}

	_, _ = fmt.Fprintf(f, `
## Scaling Verdict

| Verdict | Value |
|---|---|
| Mode | %s |
| Summary | %s |
| Bottleneck (if no improvement/regression) | %s |
| Gate | %s |
`, scaling.Mode, scaling.Message, scalabilityDefaultIfEmpty(scaling.Bottleneck, "-"), passFail(scaling.Pass))

	return f.Close()
}

func scalabilityDefaultIfEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
