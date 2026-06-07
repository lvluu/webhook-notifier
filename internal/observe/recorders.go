package observe

const (
	customerClassSmall  = "small"
	customerClassWhale  = "whale"
	customerClassPaused = "paused"
)

var customerClasses = []string{
	customerClassSmall,
	customerClassWhale,
	customerClassPaused,
}

// CustomerClassFromValue derives the customer_class label from a class value.
func CustomerClassFromValue(class uint32) string {
	switch {
	case class >= 3:
		return customerClassWhale
	case class == 0:
		return customerClassPaused
	default:
		return customerClassSmall
	}
}

// IncDelivery safely increments WebhookDeliveriesTotal, skipping if nil.
func IncDelivery(status string, customerClass string) {
	if !metricsRegistered() {
		return
	}
	WebhookDeliveriesTotal.WithLabelValues(status, customerClass).Add(1)
}

// ObserveDeliveryLatency safely observes WebhookDeliveryLatencySeconds, skipping if nil.
func ObserveDeliveryLatency(customerClass string, seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookDeliveryLatencySeconds.WithLabelValues(customerClass).Observe(seconds)
}

// ObserveQueueWait safely observes WebhookQueueWaitSeconds, skipping if nil.
func ObserveQueueWait(customerClass string, seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookQueueWaitSeconds.WithLabelValues(customerClass).Observe(seconds)
}

// IncAttempt safely increments WebhookAttemptTotal, skipping if nil.
func IncAttempt(outcome string) {
	if !metricsRegistered() {
		return
	}
	WebhookAttemptTotal.WithLabelValues(outcome).Add(1)
}

// ObservePostDuration safely observes WebhookPostDurationSeconds, skipping if nil.
func ObservePostDuration(customerClass string, seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookPostDurationSeconds.WithLabelValues(customerClass).Observe(seconds)
}

// IncDeliveriesInFlight safely increments WebhookDeliveriesInFlight, skipping if nil.
func IncDeliveriesInFlight() {
	if !metricsRegistered() {
		return
	}
	WebhookDeliveriesInFlight.Inc()
}

// DecDeliveriesInFlight safely decrements WebhookDeliveriesInFlight, skipping if nil.
func DecDeliveriesInFlight() {
	if !metricsRegistered() {
		return
	}
	WebhookDeliveriesInFlight.Dec()
}

// IncDlq safely increments WebhookDlqTotal, skipping if nil.
func IncDlq(reason string) {
	if !metricsRegistered() {
		return
	}
	WebhookDlqTotal.WithLabelValues(reason).Add(1)
}

// ObserveSchedulerPick safely observes WebhookSchedulerPickDurationSeconds, skipping if nil.
func ObserveSchedulerPick(seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookSchedulerPickDurationSeconds.Observe(seconds)
}

// SetConfiguredWorkers safely sets WebhookWorkersConfigured, skipping if nil.
func SetConfiguredWorkers(n int) {
	if !metricsRegistered() {
		return
	}
	WebhookWorkersConfigured.Set(float64(n))
}

// SetActiveCustomers safely sets WebhookActiveCustomers, skipping if nil.
func SetActiveCustomers(n float64) {
	if !metricsRegistered() {
		return
	}
	WebhookActiveCustomers.Set(n)
}

// ObserveSemaphoreWait safely observes WebhookHostSemaphoreWaitSeconds, skipping if nil.
func ObserveSemaphoreWait(host string, seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookHostSemaphoreWaitSeconds.WithLabelValues(host).Observe(seconds)
}

// ObserveClaimDuration safely observes WebhookClaimDurationSeconds, skipping if nil.
func ObserveClaimDuration(seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookClaimDurationSeconds.Observe(seconds)
}

// ObserveClaimBatchSize safely observes WebhookClaimBatchSize, skipping if nil.
func ObserveClaimBatchSize(n float64) {
	if !metricsRegistered() {
		return
	}
	WebhookClaimBatchSize.Observe(n)
}

// IncClaimEmpty safely increments WebhookClaimEmptyTotal, skipping if nil.
func IncClaimEmpty() {
	if !metricsRegistered() {
		return
	}
	WebhookClaimEmptyTotal.Add(1)
}

// SetPendingDeliveries safely sets the current queue backlog gauge.
func SetPendingDeliveries(customerClass string, n float64) {
	if !metricsRegistered() {
		return
	}
	WebhookPendingDeliveries.WithLabelValues(customerClass).Set(n)
}

// SetPendingOldestAge safely sets the oldest pending delivery age gauge.
func SetPendingOldestAge(seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookPendingOldestAgeSeconds.Set(seconds)
}

// ObserveIngestFanout safely observes WebhookIngestFanoutPerEvent, skipping if nil.
func ObserveIngestFanout(n float64) {
	if !metricsRegistered() {
		return
	}
	WebhookIngestFanoutPerEvent.Observe(n)
}

// ObserveAttempts safely observes WebhookAttemptsPerDelivery, skipping if nil.
func ObserveAttempts(attempts float64) {
	if !metricsRegistered() {
		return
	}
	WebhookAttemptsPerDelivery.Observe(attempts)
}

// SetBreakerState safely records the active breaker state for a Webhook.
func SetBreakerState(webhookID string, state string) {
	if !metricsRegistered() {
		return
	}
	for _, candidate := range []string{"closed", "open", "half_open"} {
		value := 0.0
		if candidate == state {
			value = 1
		}
		WebhookBreakerState.WithLabelValues(webhookID, candidate).Set(value)
	}
}

// IncBreakerTrip safely increments WebhookBreakerTripsTotal, skipping if nil.
func IncBreakerTrip(reason string) {
	if !metricsRegistered() {
		return
	}
	WebhookBreakerTripsTotal.WithLabelValues(reason).Add(1)
}

// IncWorkerTransientDBError safely increments
// WebhookWorkerTransientDBErrorsTotal for the given loop phase.
func IncWorkerTransientDBError(phase string) {
	if !metricsRegistered() {
		return
	}
	WebhookWorkerTransientDBErrorsTotal.WithLabelValues(phase).Add(1)
}

// IncPgNotifyBroadcast safely increments the remote wake broadcaster outcome.
func IncPgNotifyBroadcast(result string) {
	if !metricsRegistered() {
		return
	}
	WebhookPgNotifyBroadcastTotal.WithLabelValues(result).Add(1)
}
