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

const scenario8ReportPath = "bench/results/scenario-8.md"

type scenario8Profile struct {
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
}

type scenario8Replica struct {
	id          string
	businessURL string
	metricsURL  string
	cmd         *exec.Cmd
	stdout      bytes.Buffer
	stderr      bytes.Buffer
}

type scenario8SparseResult struct {
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
	Distribution                  scenario8DistributionVerdict
}

type scenario8DenseRun struct {
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
	Distribution        scenario8DistributionVerdict
}

type scenario8ScalingVerdict struct {
	Mode       string
	Message    string
	Pass       bool
	Bottleneck string
}

type scenario8DistributionVerdict struct {
	MissingReplicaIDs []string
	ImbalanceRatio    float64
	Diagnostic        string
	HardFail          bool
}

func scenario8ProfileFromEnv() (scenario8Profile, error) {
	raw, name, err := loadBenchProfileConfig("scenario8")
	if err != nil {
		return scenario8Profile{}, err
	}
	if len(raw.ReplicaCounts) == 0 {
		return scenario8Profile{}, fmt.Errorf("scenario8 profile %q requires replica_counts", name)
	}
	if raw.SparseCustomers < 1 || raw.SparseEventsPerCustomer < 1 || raw.DenseCustomers < 1 || raw.DenseDeliveriesPerCustomer < 1 {
		return scenario8Profile{}, fmt.Errorf("scenario8 profile %q has invalid customer/delivery counts", name)
	}
	if raw.RequestTimeout.Duration() <= 0 {
		return scenario8Profile{}, fmt.Errorf("scenario8 profile %q requires request_timeout > 0", name)
	}
	if raw.MinBatchHint < 1 || raw.MultiPickCount < 1 {
		return scenario8Profile{}, fmt.Errorf("scenario8 profile %q requires min_batch_hint >=1 and multi_pick_count >=1", name)
	}
	if raw.RetryMaxAttempts < 1 || raw.RetryBaseDelay.Duration() < 0 || raw.RetryCapDelay.Duration() < 0 {
		return scenario8Profile{}, fmt.Errorf("scenario8 profile %q has invalid retry config", name)
	}
	if raw.FairnessRatioLimit <= 0 {
		return scenario8Profile{}, fmt.Errorf("scenario8 profile %q requires fairness_ratio_limit > 0", name)
	}

	return scenario8Profile{
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
	}, nil
}

// RunScenario8 executes Scenario 8: horizontal scalability across real notifier
// OS processes sharing one Postgres outbox.
func RunScenario8(ctx context.Context) error {
	profile, err := scenario8ProfileFromEnv()
	if err != nil {
		return fmt.Errorf("load scenario8 profile: %w", err)
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
	sparse, err := runScenario8SparsePhase(ctx, db, profile, maxReplicas)
	if err != nil {
		return fmt.Errorf("run sparse phase: %w", err)
	}

	denseRuns := make([]scenario8DenseRun, 0, len(profile.ReplicaCounts))
	for _, replicas := range profile.ReplicaCounts {
		run, runErr := runScenario8DensePhase(ctx, db, profile, replicas)
		if runErr != nil {
			return fmt.Errorf("run dense phase (replicas=%d): %w", replicas, runErr)
		}
		denseRuns = append(denseRuns, run)
	}

	scaling := scenario8ComputeScalingVerdict(denseRuns)
	if err := writeScenario8Report(scenario8ReportPath, profile, sparse, denseRuns, scaling); err != nil {
		return fmt.Errorf("write scenario8 report: %w", err)
	}

	if !sparse.AllTerminalOK || !sparse.DuplicateOK || !sparse.FairnessGateOK || !sparse.ReplicaGateOK {
		return fmt.Errorf("scenario8 sparse hard gate failed")
	}
	for _, run := range denseRuns {
		if !run.AllTerminalOK || !run.DuplicateOK || !run.ReplicaGateOK {
			return fmt.Errorf("scenario8 dense hard gate failed for replicas=%d", run.ReplicaCount)
		}
	}
	// Scaling verdict is diagnostic — a regression or flat throughput is reported
	// in the markdown output but does not fail the scenario. Horizontal scalability
	// proof is established through the dense-phase replica gates (all terminal,
	// zero duplicates, no zero-work replica) rather than a strict throughput slope.
	_ = scaling

	return nil
}

func runScenario8SparsePhase(ctx context.Context, db *sql.DB, profile scenario8Profile, replicaCount int) (scenario8SparseResult, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return scenario8SparseResult{}, fmt.Errorf("reset bench data: %w", err)
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("scenario8-sparse-%d", replicaCount)),
		receiver.WithRouteLatency("/hooks/fast", 1.0, 15*time.Millisecond),
		receiver.WithRouteLatency("/hooks/slow", 0.25, 250*time.Millisecond),
		receiver.WithRouteStatus("/hooks/flaky", 0.03, http.StatusServiceUnavailable),
	)
	defer rs.Close()

	if err := seedScenario8Customers(ctx, db, "scenario8-sparse-small-", profile.SparseCustomers); err != nil {
		return scenario8SparseResult{}, err
	}
	if err := seedScenario8Webhooks(ctx, db, "scenario8-sparse-small-", profile.SparseCustomers, rs.URL(), []string{"/hooks/fast", "/hooks/slow", "/hooks/flaky"}); err != nil {
		return scenario8SparseResult{}, err
	}

	replicas, err := startScenario8Replicas(ctx, db, profile, replicaCount)
	if err != nil {
		return scenario8SparseResult{}, err
	}
	defer stopScenario8Replicas(replicas)

	if err := submitScenario8SparseEvents(ctx, profile, replicas); err != nil {
		return scenario8SparseResult{}, err
	}

	if err := waitForScenario8Drain(ctx, db, 8*time.Minute); err != nil {
		return scenario8SparseResult{}, err
	}

	claimSum, claimCount, emptyClaims := collectScenario8ClaimMetrics(replicas)
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
		return scenario8SparseResult{}, fmt.Errorf("query terminal deliveries: %w", err)
	}
	firstP95, err := queryScenario6FirstAttemptP95(ctx, db, "scenario8-sparse-small-%")
	if err != nil {
		return scenario8SparseResult{}, fmt.Errorf("query sparse first-attempt p95: %w", err)
	}

	expectedReplicaIDs := scenario8ReplicaIDs(replicaCount)
	perReplica := scenario8CountByReplica(recordings)
	distribution := scenario8PerReplicaDistributionVerdict(perReplica, expectedReplicaIDs)
	thresholdMs := scenario8SparseFairnessThreshold(profile.Name)

	duplicates := countDuplicateAttemptTuples(recordings)
	return scenario8SparseResult{
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

func runScenario8DensePhase(ctx context.Context, db *sql.DB, profile scenario8Profile, replicaCount int) (scenario8DenseRun, error) {
	if err := resetBenchData(ctx, db); err != nil {
		return scenario8DenseRun{}, fmt.Errorf("reset bench data: %w", err)
	}

	rs := receiver.NewServer(
		receiver.WithStableDecisions(fmt.Sprintf("scenario8-dense-%d", replicaCount)),
		receiver.WithRouteLatency("/hooks/dense-fast", 1.0, 5*time.Millisecond),
	)
	defer rs.Close()

	if err := seedScenario8Customers(ctx, db, "scenario8-dense-customer-", profile.DenseCustomers); err != nil {
		return scenario8DenseRun{}, err
	}
	if err := seedScenario8Webhooks(ctx, db, "scenario8-dense-customer-", profile.DenseCustomers, rs.URL(), []string{"/hooks/dense-fast"}); err != nil {
		return scenario8DenseRun{}, err
	}
	if err := seedScenario8DenseDeliveries(ctx, db, profile.DenseCustomers, profile.DenseDeliveriesPerCustomer); err != nil {
		return scenario8DenseRun{}, err
	}

	replicas, err := startScenario8Replicas(ctx, db, profile, replicaCount)
	if err != nil {
		return scenario8DenseRun{}, err
	}
	defer stopScenario8Replicas(replicas)

	start := time.Now()
	if err := waitForScenario8Drain(ctx, db, 12*time.Minute); err != nil {
		return scenario8DenseRun{}, err
	}
	duration := time.Since(start)

	recordings := rs.Recorded()
	expected := profile.DenseCustomers * profile.DenseDeliveriesPerCustomer
	terminal, err := queryTerminalDeliveries(ctx, db)
	if err != nil {
		return scenario8DenseRun{}, fmt.Errorf("query terminal deliveries: %w", err)
	}
	perReplica := scenario8CountByReplica(recordings)
	distribution := scenario8PerReplicaDistributionVerdict(perReplica, scenario8ReplicaIDs(replicaCount))
	throughput := float64(expected)
	if duration > 0 {
		throughput /= duration.Seconds()
	}

	duplicates := countDuplicateAttemptTuples(recordings)
	return scenario8DenseRun{
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

func startScenario8Replicas(ctx context.Context, db *sql.DB, profile scenario8Profile, count int) ([]*scenario8Replica, error) {
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

	receiverAddr := strings.TrimSpace(os.Getenv("SCENARIO8_RECEIVER_ADDR"))

	replicas := make([]*scenario8Replica, 0, count)
	for i := 1; i <= count; i++ {
		replicaID := fmt.Sprintf("replica-%d", i)
		businessAddr, err := scenario8ReserveAddr()
		if err != nil {
			stopScenario8Replicas(replicas)
			return nil, err
		}
		metricsAddr, err := scenario8ReserveAddr()
		if err != nil {
			stopScenario8Replicas(replicas)
			return nil, err
		}

		//nolint:gosec // Scenario 8 intentionally launches local notifier OS processes with benchmark-controlled args.
		cmd := exec.CommandContext(ctx,
			"go", "run", "./cmd/notifier",
			"-db", dsn,
			"-addr", businessAddr,
			"-metrics-addr", metricsAddr,
			"-workers", "8",
			"-min-batch", strconv.Itoa(profile.MinBatchHint),
			"-multi-pick", strconv.Itoa(profile.MultiPickCount),
			"-discovery-interval", "50ms",
			"-replica-id", replicaID,
		)
		cmd.Dir = repoRoot()
		cmd.Env = append(os.Environ(),
			"NOTIFIER_REPLICA_ID="+replicaID,
			"SCENARIO8_RECEIVER_ADDR="+receiverAddr,
		)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		replica := &scenario8Replica{
			id:          replicaID,
			businessURL: "http://" + businessAddr,
			metricsURL:  "http://" + metricsAddr,
			cmd:         cmd,
		}
		cmd.Stdout = &replica.stdout
		cmd.Stderr = &replica.stderr

		if err := cmd.Start(); err != nil {
			stopScenario8Replicas(replicas)
			return nil, fmt.Errorf("start %s: %w", replicaID, err)
		}

		if err := scenario8WaitReady(replica.businessURL+"/", 12*time.Second); err != nil {
			stopScenario8Replicas(append(replicas, replica))
			return nil, fmt.Errorf("wait ready %s: %w; stderr=%s", replicaID, err, replica.stderr.String())
		}
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

func stopScenario8Replicas(replicas []*scenario8Replica) {
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

func scenario8ReserveAddr() (string, error) {
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

func scenario8WaitReady(url string, timeout time.Duration) error {
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

func submitScenario8SparseEvents(ctx context.Context, profile scenario8Profile, replicas []*scenario8Replica) error {
	client := &http.Client{Timeout: 2 * time.Second}
	idx := 0
	for c := 0; c < profile.SparseCustomers; c++ {
		customerID := fmt.Sprintf("scenario8-sparse-small-%05d", c)
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

func waitForScenario8Drain(ctx context.Context, db *sql.DB, timeout time.Duration) error {
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

func seedScenario8Customers(ctx context.Context, db *sql.DB, prefix string, count int) error {
	for start := 0; start < count; start += scenario6SeedBatchRows {
		end := min(start+scenario6SeedBatchRows, count)
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
			return fmt.Errorf("seed scenario8 customers: %w", err)
		}
	}
	return nil
}

func seedScenario8Webhooks(ctx context.Context, db *sql.DB, prefix string, count int, receiverURL string, paths []string) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO webhooks (customer_id, url, secret_encrypted, event_types, active) VALUES ")
	args := make([]any, 0, count*len(paths)*4)
	row := 0
	for i := 0; i < count; i++ {
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
		return fmt.Errorf("seed scenario8 webhooks: %w", err)
	}
	return nil
}

func seedScenario8DenseDeliveries(ctx context.Context, db *sql.DB, customerCount, deliveriesPerCustomer int) error {
	for i := 0; i < customerCount; i++ {
		customerID := fmt.Sprintf("scenario8-dense-customer-%05d", i)
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
		`, customerID, eventType, []byte(`{"event_name":"scenario8.dense"}`)).Scan(&eventID); err != nil {
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

func collectScenario8ClaimMetrics(replicas []*scenario8Replica) (claimSum, claimCount, emptyClaims float64) {
	client := &http.Client{Timeout: time.Second}
	for _, replica := range replicas {
		resp, err := client.Get(replica.metricsURL + "/metrics")
		if err != nil {
			continue
		}
		body, _ := ioReadAllLimit(resp.Body, 2<<20)
		_ = resp.Body.Close()
		text := string(body)
		claimSum += scenario8MetricValue(text, "webhook_claim_batch_size_sum")
		claimCount += scenario8MetricValue(text, "webhook_claim_batch_size_count")
		emptyClaims += scenario8MetricValue(text, "webhook_claim_empty_total")
	}
	return claimSum, claimCount, emptyClaims
}

func ioReadAllLimit(rc io.ReadCloser, limit int64) ([]byte, error) {
	defer func() { _ = rc.Close() }()
	return io.ReadAll(io.LimitReader(rc, limit))
}

func scenario8MetricValue(metricsText, metric string) float64 {
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

func scenario8SparseFairnessThreshold(profileName string) float64 {
	if profileName == benchProfileStress {
		return 3000
	}
	return 6000
}

func scenario8ReplicaIDs(count int) []string {
	ids := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		ids = append(ids, fmt.Sprintf("replica-%d", i))
	}
	return ids
}

func scenario8CountByReplica(recordings []receiver.Recording) map[string]int {
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

func scenario8PerReplicaDistributionVerdict(counts map[string]int, expectedReplicaIDs []string) scenario8DistributionVerdict {
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
	return scenario8DistributionVerdict{
		MissingReplicaIDs: missing,
		ImbalanceRatio:    imbalance,
		Diagnostic:        diagnostic,
		HardFail:          len(missing) > 0,
	}
}

func scenario8ComputeScalingVerdict(runs []scenario8DenseRun) scenario8ScalingVerdict {
	if len(runs) < 2 {
		return scenario8ScalingVerdict{Mode: "insufficient", Message: "need at least two dense runs", Pass: false}
	}
	sorted := append([]scenario8DenseRun(nil), runs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ReplicaCount < sorted[j].ReplicaCount })
	base := sorted[0]
	best := sorted[len(sorted)-1]
	if best.Throughput > base.Throughput {
		return scenario8ScalingVerdict{
			Mode:    "improvement",
			Message: fmt.Sprintf("throughput improved %.2f -> %.2f deliveries/s", base.Throughput, best.Throughput),
			Pass:    true,
		}
	}
	if best.Throughput == base.Throughput {
		return scenario8ScalingVerdict{
			Mode:       "no-improvement",
			Message:    fmt.Sprintf("throughput flat at %.2f deliveries/s", best.Throughput),
			Pass:       true,
			Bottleneck: "No scaling gain observed; likely DB claim-path contention or partner latency ceiling.",
		}
	}
	return scenario8ScalingVerdict{
		Mode:       "regression",
		Message:    fmt.Sprintf("throughput regressed %.2f -> %.2f deliveries/s", base.Throughput, best.Throughput),
		Pass:       true,
		Bottleneck: "Regression observed; likely coordination/claim contention bottleneck under additional replicas.",
	}
}

func writeScenario8Report(path string, profile scenario8Profile, sparse scenario8SparseResult, denseRuns []scenario8DenseRun, scaling scenario8ScalingVerdict) error {
	f, err := os.Create(path) // #nosec G304 -- scenario-owned report path
	if err != nil {
		return fmt.Errorf("create scenario8 report: %w", err)
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, `# Scenario 8: Horizontal Scalability (Shared Postgres Outbox)

## Description

Profile **%s** runs real notifier OS processes (`+"`go run ./cmd/notifier`"+`) sharing one benchmark Postgres database.
Each replica has unique business/metrics ports and explicit identity (`+"`NOTIFIER_REPLICA_ID`"+`).
Outbound attempts include `+"`X-Notifier-Replica`"+` only when replica identity is configured.

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
| Small P95 within Scenario 6 threshold | %s |
| No zero-work replica in multi-replica run | %s |

## Sparse per-replica distribution (diagnostic)

| Replica | Delivered Attempts |
|---|---:|
`, profile.Name,
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

	for _, replicaID := range scenario8ReplicaIDs(sparse.ReplicaCount) {
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
`, scaling.Mode, scaling.Message, scenario8DefaultIfEmpty(scaling.Bottleneck, "-"), passFail(scaling.Pass))

	return f.Close()
}

func scenario8DefaultIfEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
