package main

import (
	"flag"
	"fmt"
	"time"
)

// cliConfig holds everything parsed from the notifier command line: the
// assembled worker runtime configuration plus the process-level settings main
// needs directly (DB DSN, listen addresses, replica identity, pool sizing).
type cliConfig struct {
	DBDSN       string
	Addr        string
	MetricsAddr string
	ReplicaID   string
	MaxConns    int
	Runtime     workerRuntimeConfig
}

// parseCLIConfig registers the notifier flags on fs, parses args, and assembles
// the runtime configuration. Extracted from main so the flag surface — names,
// defaults, and the mapping into workerRuntimeConfig — is unit testable. Pass a
// flag.FlagSet with ContinueOnError to observe parse errors instead of exiting.
func parseCLIConfig(fs *flag.FlagSet, args []string) (cliConfig, error) {
	db := fs.String("db", "", "DATABASE_URL")
	addr := fs.String("addr", ":8080", "business HTTP server address")
	metricsAddr := fs.String("metrics-addr", ":9090", "metrics HTTP server address")
	workerCount := fs.Int("workers", 64, "number of delivery workers")
	replicaID := fs.String("replica-id", "", "optional replica identity for diagnostics header")
	minBatch := fs.Int("min-batch", 32, "minimum claim batch size per customer")
	claimBatchHint := fs.Int("claim-batch-hint", 64, "maximum per-pick claim batch size; lower favours fairness, higher favours throughput. 0 = unbounded")
	multiPick := fs.Int("multi-pick", 1, "customers to batch per claim query (1 = single-customer ClaimNextN, >1 = ClaimNextBatch)")
	hostCap := fs.Int("host-cap", 20, "maximum concurrent in-flight requests per Integration Partner hostname")
	requestTimeout := fs.Duration("request-timeout", 30*time.Second, "per-Attempt HTTP request timeout; a stalled partner holds a delivery slot for this long")
	deliveryConcurrency := fs.Int("delivery-concurrency", 16, "maximum concurrent in-flight delivery attempts per worker")
	discoveryInterval := fs.Duration("discovery-interval", time.Second, "claimable customer discovery interval")
	emptyClaimBackoff := fs.Duration("empty-claim-backoff", 20*time.Millisecond, "pause after an empty claim cycle")
	backlogSampleInterval := fs.Duration("pending-sample-interval", 5*time.Second, "sample interval for pending-delivery backlog gauge (0 disables)")
	enablePgNotifyWakeups := fs.Bool("enable-pg-notify-wakeups", true, "use LISTEN/NOTIFY wakeups in addition to polling")
	pgNotifyDSN := fs.String("pg-notify-db", "", "optional direct DATABASE_URL for LISTEN/NOTIFY connections")
	pgNotifyChannel := fs.String("pg-notify-channel", "webhook_customer_ready", "LISTEN/NOTIFY channel for customer wakeups")
	breakerFailureThreshold := fs.Int("breaker-failure-threshold", 20, "failures before opening a Webhook circuit breaker; 0 disables breaker filtering")
	breakerInitialCooldown := fs.Duration("breaker-initial-cooldown", 5*time.Minute, "initial circuit breaker open cooldown")
	breakerMaxCooldown := fs.Duration("breaker-max-cooldown", 15*time.Minute, "maximum circuit breaker open cooldown")
	maxConns := fs.Int("max-conns", 0, "max DB connections (0 = workerCount + 10)")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}
	if *breakerFailureThreshold < 0 {
		return cliConfig{}, fmt.Errorf("breaker-failure-threshold must be >= 0")
	}
	if *breakerInitialCooldown <= 0 {
		return cliConfig{}, fmt.Errorf("breaker-initial-cooldown must be > 0")
	}
	if *breakerMaxCooldown <= 0 {
		return cliConfig{}, fmt.Errorf("breaker-max-cooldown must be > 0")
	}
	if *breakerMaxCooldown < *breakerInitialCooldown {
		return cliConfig{}, fmt.Errorf("breaker-max-cooldown must be >= breaker-initial-cooldown")
	}

	runtimeCfg := loadWorkerRuntimeConfig()
	runtimeCfg.WorkerCount = *workerCount
	runtimeCfg.Worker.MinBatchHint = *minBatch
	runtimeCfg.Worker.MaxBatchHint = *claimBatchHint
	runtimeCfg.Worker.MultiPickCount = *multiPick
	runtimeCfg.HostConcurrencyCap = *hostCap
	runtimeCfg.Worker.EmptyClaimBackoff = *emptyClaimBackoff
	runtimeCfg.Worker.RequestTimeout = *requestTimeout
	runtimeCfg.Worker.DeliveryConcurrency = *deliveryConcurrency
	runtimeCfg.BacklogSampleInterval = *backlogSampleInterval
	runtimeCfg.EnablePgNotifyWakeups = *enablePgNotifyWakeups
	runtimeCfg.PgNotifyDSN = *pgNotifyDSN
	runtimeCfg.PgNotifyChannel = *pgNotifyChannel
	runtimeCfg.Breaker.FailureThreshold = *breakerFailureThreshold
	runtimeCfg.Breaker.InitialCooldown = *breakerInitialCooldown
	runtimeCfg.Breaker.MaxCooldown = *breakerMaxCooldown
	runtimeCfg.Discovery.Interval = *discoveryInterval

	return cliConfig{
		DBDSN:       *db,
		Addr:        *addr,
		MetricsAddr: *metricsAddr,
		ReplicaID:   *replicaID,
		MaxConns:    *maxConns,
		Runtime:     runtimeCfg,
	}, nil
}
