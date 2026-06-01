package main

import (
	"time"

	"github.com/lvluu/webhook-notifier/internal/worker"
)

type workerRuntimeConfig struct {
	WorkerCount        int
	MinBatchHint       int
	HostConcurrencyCap int
	RequestTimeout     time.Duration
	Discovery          worker.DiscoveryConfig
}

func loadWorkerRuntimeConfig() workerRuntimeConfig {
	return workerRuntimeConfig{
		WorkerCount:        64,
		MinBatchHint:       32,
		HostConcurrencyCap: 16,
		RequestTimeout:     30 * time.Second,
		Discovery: worker.DiscoveryConfig{
			Interval:     time.Second,
			EmptyBackoff: 5 * time.Second,
			ErrorBackoff: time.Second,
			MaxBackoff:   30 * time.Second,
			Limit:        1000,
		},
	}
}
