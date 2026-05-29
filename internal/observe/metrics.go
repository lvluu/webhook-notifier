package observe

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

// Package-level metric variables. Initialised by RegisterMetrics. Tests that
// need a clean registry should call ResetMetricsForTest between test cases.
var (
	WebhookDeliveriesTotal              *prometheus.CounterVec
	WebhookDeliveryLatencySeconds       *prometheus.HistogramVec
	WebhookAttemptsPerDelivery          prometheus.Histogram
	WebhookDlqTotal                     *prometheus.CounterVec
	WebhookSchedulerPickDurationSeconds prometheus.Histogram
	WebhookHostSemaphoreWaitSeconds     *prometheus.HistogramVec
	WebhookClaimDurationSeconds         prometheus.Histogram
	WebhookClaimBatchSize               prometheus.Histogram
	WebhookClaimEmptyTotal              prometheus.Counter
	WebhookActiveCustomers              prometheus.Gauge
	WebhookIngestFanoutPerEvent         prometheus.Histogram
)

// ResetMetricsForTest reinitialises all metric variables to nil so the next
// RegisterMetrics call supplies them to a (possibly different) registry. Only
// safe in sequential tests — not safe for concurrent access.
func ResetMetricsForTest() {
	WebhookDeliveriesTotal = nil
	WebhookDeliveryLatencySeconds = nil
	WebhookAttemptsPerDelivery = nil
	WebhookDlqTotal = nil
	WebhookSchedulerPickDurationSeconds = nil
	WebhookHostSemaphoreWaitSeconds = nil
	WebhookClaimDurationSeconds = nil
	WebhookClaimBatchSize = nil
	WebhookClaimEmptyTotal = nil
	WebhookActiveCustomers = nil
	WebhookIngestFanoutPerEvent = nil
}

// RegisterPoolGauges registers six GaugeFunc metrics backed by sql.DB.Stats().
// Must be called after the DB is opened and metrics are registered.
func RegisterPoolGauges(reg prometheus.Registerer, db *sql.DB) {
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "webhook_pg_pool_max_open",
			Help: "Maximum number of open connections to the database.",
		}, func() float64 { return float64(db.Stats().MaxOpenConnections) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "webhook_pg_pool_open",
			Help: "Current number of open connections.",
		}, func() float64 { return float64(db.Stats().OpenConnections) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "webhook_pg_pool_in_use",
			Help: "Current number of open connections in use.",
		}, func() float64 { return float64(db.Stats().InUse) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "webhook_pg_pool_idle",
			Help: "Current number of idle connections.",
		}, func() float64 { return float64(db.Stats().Idle) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "webhook_pg_pool_wait_count",
			Help: "Total number of connections waited for.",
		}, func() float64 { return float64(db.Stats().WaitCount) }),

		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "webhook_pg_pool_wait_duration_seconds",
			Help: "Total time blocked waiting for a new connection, in seconds.",
		}, func() float64 { return db.Stats().WaitDuration.Seconds() }),
	)
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
	}, []string{"status"})

	WebhookDeliveryLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_delivery_latency_seconds",
		Help:    "Delivery lifetime in seconds, by customer class.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30, 60, 300, 1800},
	}, []string{"customer_class"})

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

	WebhookActiveCustomers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_active_customers",
		Help: "Current number of customers with pending deliveries in the scheduler.",
	})

	WebhookIngestFanoutPerEvent = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_ingest_fanout_per_event",
		Help:    "Number of webhooks fanned out per ingested event.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100},
	})

	reg.MustRegister(
		WebhookDeliveriesTotal,
		WebhookDeliveryLatencySeconds,
		WebhookAttemptsPerDelivery,
		WebhookDlqTotal,
		WebhookSchedulerPickDurationSeconds,
		WebhookHostSemaphoreWaitSeconds,
		WebhookClaimDurationSeconds,
		WebhookClaimBatchSize,
		WebhookClaimEmptyTotal,
		WebhookActiveCustomers,
		WebhookIngestFanoutPerEvent,
	)
}

// CustomerClassFromWeight derives the customer_class label from a weight value.
func CustomerClassFromWeight(weight uint32) string {
	switch {
	case weight >= 3:
		return "whale"
	case weight == 0:
		return "paused"
	default:
		return "small"
	}
}

// IncDelivery safely increments WebhookDeliveriesTotal, skipping if nil.
func IncDelivery(status string) {
	if WebhookDeliveriesTotal == nil {
		return
	}
	WebhookDeliveriesTotal.WithLabelValues(status).Add(1)
}

// ObserveDeliveryLatency safely observes WebhookDeliveryLatencySeconds, skipping if nil.
func ObserveDeliveryLatency(customerClass string, seconds float64) {
	if WebhookDeliveryLatencySeconds == nil {
		return
	}
	WebhookDeliveryLatencySeconds.WithLabelValues(customerClass).Observe(seconds)
}

// IncDlq safely increments WebhookDlqTotal, skipping if nil.
func IncDlq(reason string) {
	if WebhookDlqTotal == nil {
		return
	}
	WebhookDlqTotal.WithLabelValues(reason).Add(1)
}

// ObserveSchedulerPick safely observes WebhookSchedulerPickDurationSeconds, skipping if nil.
func ObserveSchedulerPick(seconds float64) {
	if WebhookSchedulerPickDurationSeconds == nil {
		return
	}
	WebhookSchedulerPickDurationSeconds.Observe(seconds)
}

// SetActiveCustomers safely sets WebhookActiveCustomers, skipping if nil.
func SetActiveCustomers(n float64) {
	if WebhookActiveCustomers == nil {
		return
	}
	WebhookActiveCustomers.Set(n)
}

// ObserveSemaphoreWait safely observes WebhookHostSemaphoreWaitSeconds, skipping if nil.
func ObserveSemaphoreWait(host string, seconds float64) {
	if WebhookHostSemaphoreWaitSeconds == nil {
		return
	}
	WebhookHostSemaphoreWaitSeconds.WithLabelValues(host).Observe(seconds)
}

// ObserveClaimDuration safely observes WebhookClaimDurationSeconds, skipping if nil.
func ObserveClaimDuration(seconds float64) {
	if WebhookClaimDurationSeconds == nil {
		return
	}
	WebhookClaimDurationSeconds.Observe(seconds)
}

// ObserveClaimBatchSize safely observes WebhookClaimBatchSize, skipping if nil.
func ObserveClaimBatchSize(n float64) {
	if WebhookClaimBatchSize == nil {
		return
	}
	WebhookClaimBatchSize.Observe(n)
}

// IncClaimEmpty safely increments WebhookClaimEmptyTotal, skipping if nil.
func IncClaimEmpty() {
	if WebhookClaimEmptyTotal == nil {
		return
	}
	WebhookClaimEmptyTotal.Add(1)
}

// ObserveIngestFanout safely observes WebhookIngestFanoutPerEvent, skipping if nil.
func ObserveIngestFanout(n float64) {
	if WebhookIngestFanoutPerEvent == nil {
		return
	}
	WebhookIngestFanoutPerEvent.Observe(n)
}

// ObserveAttempts safely observes WebhookAttemptsPerDelivery, skipping if nil.
func ObserveAttempts(attempts float64) {
	if WebhookAttemptsPerDelivery == nil {
		return
	}
	WebhookAttemptsPerDelivery.Observe(attempts)
}
