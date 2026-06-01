package observe

// CustomerClassFromValue derives the customer_class label from a class value.
func CustomerClassFromValue(class uint32) string {
	switch {
	case class >= 3:
		return "whale"
	case class == 0:
		return "paused"
	default:
		return "small"
	}
}

// IncDelivery safely increments WebhookDeliveriesTotal, skipping if nil.
func IncDelivery(status string) {
	if !metricsRegistered() {
		return
	}
	WebhookDeliveriesTotal.WithLabelValues(status).Add(1)
}

// ObserveDeliveryLatency safely observes WebhookDeliveryLatencySeconds, skipping if nil.
func ObserveDeliveryLatency(customerClass string, seconds float64) {
	if !metricsRegistered() {
		return
	}
	WebhookDeliveryLatencySeconds.WithLabelValues(customerClass).Observe(seconds)
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
