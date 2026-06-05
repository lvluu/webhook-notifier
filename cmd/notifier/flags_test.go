package main

import (
	"flag"
	"testing"
	"time"
)

func parseForTest(t *testing.T, args []string) cliConfig {
	t.Helper()
	fs := flag.NewFlagSet("notifier-test", flag.ContinueOnError)
	cfg, err := parseCLIConfig(fs, args)
	if err != nil {
		t.Fatalf("parseCLIConfig(%v): %v", args, err)
	}
	return cfg
}

func TestParseCLIConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := parseForTest(t, nil)

	if cfg.Addr != ":8080" {
		t.Errorf("Addr: got %q, want :8080", cfg.Addr)
	}
	if cfg.MetricsAddr != ":9090" {
		t.Errorf("MetricsAddr: got %q, want :9090", cfg.MetricsAddr)
	}
	if cfg.MaxConns != 0 {
		t.Errorf("MaxConns: got %d, want 0", cfg.MaxConns)
	}
	if cfg.Runtime.WorkerCount != 64 {
		t.Errorf("WorkerCount: got %d, want 64", cfg.Runtime.WorkerCount)
	}
	if cfg.Runtime.HostConcurrencyCap != 20 {
		t.Errorf("HostConcurrencyCap: got %d, want 20", cfg.Runtime.HostConcurrencyCap)
	}
	// The two flags this change adds — defaults must match worker.DefaultConfig
	// so out-of-the-box behavior is unchanged.
	if cfg.Runtime.Worker.RequestTimeout != 30*time.Second {
		t.Errorf("RequestTimeout: got %s, want 30s", cfg.Runtime.Worker.RequestTimeout)
	}
	if cfg.Runtime.Worker.DeliveryConcurrency != 16 {
		t.Errorf("DeliveryConcurrency: got %d, want 16", cfg.Runtime.Worker.DeliveryConcurrency)
	}
}

func TestParseCLIConfig_OverridesNewDeliveryFlags(t *testing.T) {
	t.Parallel()

	cfg := parseForTest(t, []string{
		"--request-timeout=5s",
		"--delivery-concurrency=4",
		"--workers=8",
		"--host-cap=2",
	})

	if got := cfg.Runtime.Worker.RequestTimeout; got != 5*time.Second {
		t.Errorf("RequestTimeout: got %s, want 5s", got)
	}
	if got := cfg.Runtime.Worker.DeliveryConcurrency; got != 4 {
		t.Errorf("DeliveryConcurrency: got %d, want 4", got)
	}
	if got := cfg.Runtime.WorkerCount; got != 8 {
		t.Errorf("WorkerCount: got %d, want 8", got)
	}
	if got := cfg.Runtime.HostConcurrencyCap; got != 2 {
		t.Errorf("HostConcurrencyCap: got %d, want 2", got)
	}
}

func TestParseCLIConfig_ParsedWorkerConfigValidates(t *testing.T) {
	t.Parallel()

	// The assembled worker config must satisfy Validate at defaults and at
	// representative overrides — guards against a flag mapping that produces an
	// internally inconsistent Config.
	for _, args := range [][]string{
		nil,
		{"--request-timeout=1s", "--delivery-concurrency=1"},
	} {
		cfg := parseForTest(t, args)
		if err := cfg.Runtime.Worker.Validate(); err != nil {
			t.Errorf("Worker.Validate() for args %v: %v", args, err)
		}
	}
}

func TestParseCLIConfig_RejectsUnknownFlag(t *testing.T) {
	t.Parallel()

	fs := flag.NewFlagSet("notifier-test", flag.ContinueOnError)
	fs.SetOutput(discardWriter{})
	if _, err := parseCLIConfig(fs, []string{"--nope=1"}); err == nil {
		t.Fatal("expected error for unknown flag, got nil")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
