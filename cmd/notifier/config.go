package main

import (
	"time"

	"github.com/lvluu/webhook-notifier/internal/worker"
)

type workerRuntimeConfig struct {
	WorkerCount             int
	Worker                  worker.Config
	HostConcurrencyCap      int
	BacklogSampleInterval   time.Duration
	EnablePgNotifyWakeups   bool
	PgNotifyDSN             string
	PgNotifyChannel         string
	EnableWeightedScheduler bool
	Discovery               worker.DiscoveryConfig
}

func loadWorkerRuntimeConfig() workerRuntimeConfig {
	workerConfig := worker.DefaultConfig()
	return workerRuntimeConfig{
		WorkerCount:           64,
		Worker:                workerConfig,
		HostConcurrencyCap:    16,
		BacklogSampleInterval: 5 * time.Second,
		EnablePgNotifyWakeups: true,
		PgNotifyChannel:       "webhook_customer_ready",
		Discovery: worker.DiscoveryConfig{
			Interval:     time.Second,
			EmptyBackoff: 5 * time.Second,
			ErrorBackoff: time.Second,
			MaxBackoff:   30 * time.Second,
			Limit:        1000,
		},
	}
}
