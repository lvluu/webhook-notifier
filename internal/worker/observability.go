package worker

import "github.com/lvluu/webhook-notifier/internal/observe"

func recordSucceededMetrics(customerClass string, lifetimeSeconds float64, attempts int) {
	observe.IncDelivery("succeeded", customerClass)
	observe.ObserveDeliveryLatency(customerClass, lifetimeSeconds)
	observe.ObserveAttempts(float64(attempts))
}

func recordDeadMetrics(customerClass string, lifetimeSeconds float64, attempts int, reason string) {
	observe.IncDelivery("dead", customerClass)
	observe.ObserveDeliveryLatency(customerClass, lifetimeSeconds)
	observe.ObserveAttempts(float64(attempts))
	observe.IncDlq(reason)
}
