package observe

import (
	"log/slog"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Package-level metric variables. Initialised by RegisterMetrics. Tests that
// need a clean registry should call ResetMetricsForTest between test cases.
var (
	WebhookDeliveriesTotal              *prometheus.CounterVec
	WebhookDeliveryLatencySeconds       *prometheus.HistogramVec
	WebhookQueueWaitSeconds             *prometheus.HistogramVec
	WebhookAttemptTotal                 *prometheus.CounterVec
	WebhookPostDurationSeconds          *prometheus.HistogramVec
	WebhookDeliveriesInFlight           prometheus.Gauge
	WebhookAttemptsPerDelivery          prometheus.Histogram
	WebhookDlqTotal                     *prometheus.CounterVec
	WebhookSchedulerPickDurationSeconds prometheus.Histogram
	WebhookHostSemaphoreWaitSeconds     *prometheus.HistogramVec
	WebhookClaimDurationSeconds         prometheus.Histogram
	WebhookClaimBatchSize               prometheus.Histogram
	WebhookClaimEmptyTotal              prometheus.Counter
	WebhookPendingDeliveries            *prometheus.GaugeVec
	WebhookPendingOldestAgeSeconds      prometheus.Gauge
	WebhookActiveCustomers              prometheus.Gauge
	WebhookWorkersConfigured            prometheus.Gauge
	WebhookIngestFanoutPerEvent         prometheus.Histogram
	WebhookBreakerState                 *prometheus.GaugeVec
	WebhookBreakerTripsTotal            *prometheus.CounterVec
	WebhookPgNotifyBroadcastTotal       *prometheus.CounterVec
	metricsRegistrationWarned           atomic.Bool
)

// ResetMetricsForTest reinitialises all metric variables to nil so the next
// RegisterMetrics call supplies them to a (possibly different) registry. Only
// safe in sequential tests — not safe for concurrent access.
func ResetMetricsForTest() {
	WebhookDeliveriesTotal = nil
	WebhookDeliveryLatencySeconds = nil
	WebhookQueueWaitSeconds = nil
	WebhookAttemptTotal = nil
	WebhookPostDurationSeconds = nil
	WebhookDeliveriesInFlight = nil
	WebhookAttemptsPerDelivery = nil
	WebhookDlqTotal = nil
	WebhookSchedulerPickDurationSeconds = nil
	WebhookHostSemaphoreWaitSeconds = nil
	WebhookClaimDurationSeconds = nil
	WebhookClaimBatchSize = nil
	WebhookClaimEmptyTotal = nil
	WebhookPendingDeliveries = nil
	WebhookPendingOldestAgeSeconds = nil
	WebhookActiveCustomers = nil
	WebhookWorkersConfigured = nil
	WebhookIngestFanoutPerEvent = nil
	WebhookBreakerState = nil
	WebhookBreakerTripsTotal = nil
	WebhookPgNotifyBroadcastTotal = nil
	metricsRegistrationWarned.Store(false)
}

func metricsRegistered() bool {
	if WebhookDeliveriesTotal != nil {
		return true
	}
	if metricsRegistrationWarned.CompareAndSwap(false, true) {
		slog.Default().Warn("observe metrics called before RegisterMetrics; dropping metric update")
	}
	return false
}

// RegisterMetrics creates all observe package metrics and registers them with
// the provided registerer. Must be called before any metric increment/observe
// call and should be called exactly once in production.
func RegisterMetrics(reg prometheus.Registerer) {
	if WebhookDeliveriesTotal != nil {
		return
	}
	initMetrics(reg)
}

func initMetrics(reg prometheus.Registerer) {
	WebhookDeliveriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_deliveries_total",
		Help: "Total number of webhook deliveries processed, by terminal status.",
	}, []string{"status", "customer_class"})

	WebhookDeliveryLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_delivery_latency_seconds",
		Help:    "Delivery lifetime in seconds, by customer class.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30, 60, 300, 1800},
	}, []string{"customer_class"})

	WebhookQueueWaitSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_queue_wait_seconds",
		Help:    "Time from delivery enqueue to first Attempt, by customer class.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30, 60},
	}, []string{"customer_class"})

	WebhookAttemptTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_attempt_total",
		Help: "Total number of delivery Attempts, by classified outcome.",
	}, []string{"outcome"})

	WebhookPostDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_post_duration_seconds",
		Help:    "Duration of HTTP POST Attempts, by customer class.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	}, []string{"customer_class"})

	WebhookDeliveriesInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_deliveries_in_flight",
		Help: "Current number of deliveries being processed by workers.",
	})

	WebhookAttemptsPerDelivery = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_attempts_per_delivery",
		Help:    "Number of attempts per delivery at terminal state.",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})

	WebhookDlqTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_dlq_total",
		Help: "Total number of deliveries sent to the DLQ, by reason.",
	}, []string{"reason"})

	WebhookSchedulerPickDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_scheduler_pick_duration_seconds",
		Help:    "Duration of Scheduler.Pick() calls.",
		Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1},
	})

	WebhookHostSemaphoreWaitSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_host_semaphore_wait_seconds",
		Help:    "Time spent waiting for a per-host semaphore slot.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	}, []string{"host"})

	WebhookClaimDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_claim_duration_seconds",
		Help:    "Duration of outbox claim transactions.",
		Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	WebhookClaimBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_claim_batch_size",
		Help:    "Number of deliveries claimed per batch.",
		Buckets: []float64{0, 1, 5, 10, 25, 50, 100},
	})

	WebhookClaimEmptyTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "webhook_claim_empty_total",
		Help: "Total number of claim queries that returned zero rows.",
	})

	WebhookPendingDeliveries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "webhook_pending_deliveries",
		Help: "Current number of pending or retryable deliveries awaiting worker processing, by customer class.",
	}, []string{"customer_class"})

	WebhookPendingOldestAgeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_pending_oldest_age_seconds",
		Help: "Age in seconds of the oldest pending or retryable delivery.",
	})

	WebhookActiveCustomers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_active_customers",
		Help: "Current number of customers with pending deliveries in the scheduler.",
	})
	WebhookWorkersConfigured = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_workers_configured",
		Help: "Configured number of notifier worker goroutines.",
	})

	WebhookIngestFanoutPerEvent = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_ingest_fanout_per_event",
		Help:    "Number of webhooks fanned out per ingested event.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100},
	})

	WebhookBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "webhook_breaker_state",
		Help: "Current persisted circuit breaker state for a webhook; value is 1 for the active state.",
	}, []string{"webhook_id", "state"})

	WebhookBreakerTripsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_breaker_trips_total",
		Help: "Total number of webhook circuit breaker trips, by reason.",
	}, []string{"reason"})

	WebhookPgNotifyBroadcastTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_pg_notify_broadcast_total",
		Help: "Total number of remote Customer wake hints handled by the pg_notify broadcaster.",
	}, []string{"result"})

	reg.MustRegister(
		WebhookDeliveriesTotal,
		WebhookDeliveryLatencySeconds,
		WebhookQueueWaitSeconds,
		WebhookAttemptTotal,
		WebhookPostDurationSeconds,
		WebhookDeliveriesInFlight,
		WebhookAttemptsPerDelivery,
		WebhookDlqTotal,
		WebhookSchedulerPickDurationSeconds,
		WebhookHostSemaphoreWaitSeconds,
		WebhookClaimDurationSeconds,
		WebhookClaimBatchSize,
		WebhookClaimEmptyTotal,
		WebhookPendingDeliveries,
		WebhookPendingOldestAgeSeconds,
		WebhookActiveCustomers,
		WebhookWorkersConfigured,
		WebhookIngestFanoutPerEvent,
		WebhookBreakerState,
		WebhookBreakerTripsTotal,
		WebhookPgNotifyBroadcastTotal,
	)
}
