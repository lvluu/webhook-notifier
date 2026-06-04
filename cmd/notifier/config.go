package main

import (
	"time"

	"github.com/lvluu/webhook-notifier/internal/worker"
)

type workerRuntimeConfig struct {
	WorkerCount             int
	MinBatchHint            int
	ClaimBatchHint          uint32
	MultiPickCount          int
	HostConcurrencyCap      int
	RequestTimeout          time.Duration
	EmptyClaimBackoff       time.Duration
	BacklogSampleInterval   time.Duration
	EnablePgNotifyWakeups   bool
	PgNotifyDSN             string
	PgNotifyChannel         string
	ClaimQueryMode          string
	EnableWeightedScheduler bool
	Discovery               worker.DiscoveryConfig
}

func loadWorkerRuntimeConfig() workerRuntimeConfig {
	return workerRuntimeConfig{
		WorkerCount:           64,
		MinBatchHint:          32,
		ClaimBatchHint:        64,
		MultiPickCount:        1,
		HostConcurrencyCap:    16,
		RequestTimeout:        30 * time.Second,
		EmptyClaimBackoff:     20 * time.Millisecond,
		BacklogSampleInterval: 5 * time.Second,
		EnablePgNotifyWakeups: true,
		PgNotifyChannel:       "webhook_customer_ready",
		ClaimQueryMode:        "split",
		Discovery: worker.DiscoveryConfig{
			Interval:     time.Second,
			EmptyBackoff: 5 * time.Second,
			ErrorBackoff: time.Second,
			MaxBackoff:   30 * time.Second,
			Limit:        1000,
		},
	}
}
