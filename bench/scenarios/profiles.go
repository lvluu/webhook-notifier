package scenarios

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	benchProfileDev    = "dev"
	benchProfileStress = "stress"
)

type benchProfilesFile struct {
	Scenarios map[string]map[string]benchProfileConfig `yaml:"scenarios"`
}

type benchProfileConfig struct {
	WorkerCount                int           `yaml:"worker_count"`
	WorkerCounts               []int         `yaml:"worker_counts"`
	ReplicaCounts              []int         `yaml:"replica_counts"`
	WhaleDeliveries            int           `yaml:"whale_deliveries"`
	SmallCount                 int           `yaml:"small_count"`
	SmallDeliveries            int           `yaml:"small_deliveries"`
	SparseCustomers            int           `yaml:"sparse_customers"`
	SparseEventsPerCustomer    int           `yaml:"sparse_events_per_customer"`
	DenseCustomers             int           `yaml:"dense_customers"`
	DenseDeliveriesPerCustomer int           `yaml:"dense_deliveries_per_customer"`
	LateDeliveries             int           `yaml:"late_deliveries"`
	SmallDripInterval          durationValue `yaml:"small_drip_interval"`
	LateNotifyDelay            durationValue `yaml:"late_notify_delay"`
	CustomerCount              int           `yaml:"customer_count"`
	EventsPerCustomer          int           `yaml:"events_per_customer"`
	SubmitInterval             durationValue `yaml:"submit_interval"`
	OutageDelay                durationValue `yaml:"outage_delay"`
	OutageDuration             durationValue `yaml:"outage_duration"`
	EventCount                 int           `yaml:"event_count"`
	ThroughputDeliveries       int           `yaml:"throughput_deliveries"`
	RetryDeliveries            int           `yaml:"retry_deliveries"`
	DeliveriesPerCustomer      int           `yaml:"deliveries_per_customer"`
	RequestTimeout             durationValue `yaml:"request_timeout"`
	DiscoveryInterval          durationValue `yaml:"discovery_interval"`
	DiscoveryEmptyBackoff      durationValue `yaml:"discovery_empty_backoff"`
	DiscoveryErrorBackoff      durationValue `yaml:"discovery_error_backoff"`
	DiscoveryMaxBackoff        durationValue `yaml:"discovery_max_backoff"`
	DiscoveryLimit             int           `yaml:"discovery_limit"`
	BreakerFailureThreshold    int           `yaml:"breaker_failure_threshold"`
	BreakerInitialCooldown     durationValue `yaml:"breaker_initial_cooldown"`
	BreakerMaxCooldown         durationValue `yaml:"breaker_max_cooldown"`
	MinBatchHint               int           `yaml:"min_batch_hint"`
	MultiPickCount             int           `yaml:"multi_pick_count"`
	RetryMaxAttempts           int           `yaml:"retry_max_attempts"`
	RetryBaseDelay             durationValue `yaml:"retry_base_delay"`
	RetryCapDelay              durationValue `yaml:"retry_cap_delay"`
	FairnessRatioLimit         float64       `yaml:"fairness_ratio_limit"`
}

type durationValue time.Duration

func (d *durationValue) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		*d = 0
		return nil
	}

	var raw string
	if err := node.Decode(&raw); err != nil {
		return err
	}
	if raw == "" {
		*d = 0
		return nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", raw, err)
	}
	*d = durationValue(parsed)
	return nil
}

func (d durationValue) Duration() time.Duration {
	return time.Duration(d)
}

var (
	benchProfilesOnce sync.Once
	benchProfilesData benchProfilesFile
	benchProfilesErr  error
)

func loadBenchProfileConfig(scenario string) (benchProfileConfig, string, error) {
	profiles, err := readBenchProfiles()
	if err != nil {
		return benchProfileConfig{}, "", err
	}

	profilesForScenario, ok := profiles.Scenarios[scenario]
	if !ok {
		return benchProfileConfig{}, "", fmt.Errorf("unknown bench scenario %q in profiles", scenario)
	}

	name, err := benchProfileNameFromEnv()
	if err != nil {
		return benchProfileConfig{}, "", err
	}

	profile, ok := profilesForScenario[name]
	if !ok {
		return benchProfileConfig{}, "", fmt.Errorf("missing %q profile for %s", name, scenario)
	}

	return profile, name, nil
}

func benchProfileNameFromEnv() (string, error) {
	name := os.Getenv("BENCH_PROFILE")
	if name == "" {
		return benchProfileDev, nil
	}
	switch name {
	case benchProfileDev, benchProfileStress:
		return name, nil
	default:
		return "", fmt.Errorf("unsupported BENCH_PROFILE %q: want %q or %q", name, benchProfileDev, benchProfileStress)
	}
}

func readBenchProfiles() (benchProfilesFile, error) {
	benchProfilesOnce.Do(func() {
		path, err := benchProfilesPath()
		if err != nil {
			benchProfilesErr = err
			return
		}

		data, err := os.ReadFile(path) //nolint:gosec // path is resolved from the repo-local bench profiles file
		if err != nil {
			benchProfilesErr = fmt.Errorf("read bench profiles: %w", err)
			return
		}

		if err := yaml.Unmarshal(data, &benchProfilesData); err != nil {
			benchProfilesErr = fmt.Errorf("parse bench profiles: %w", err)
			return
		}
	})

	return benchProfilesData, benchProfilesErr
}

func benchProfilesPath() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve bench profiles path: runtime caller unavailable")
	}
	return filepath.Join(filepath.Dir(filepath.Dir(file)), "profiles.yaml"), nil
}
