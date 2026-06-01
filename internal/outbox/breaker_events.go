package outbox

import (
	"log/slog"

	"github.com/lvluu/webhook-notifier/internal/observe"
)

func emitBreakerOpened(webhookID, reason string) {
	slog.Default().Warn("webhook circuit breaker opened", "webhook_id", webhookID, "reason", reason)
	observe.SetBreakerState(webhookID, BreakerStateOpen)
	observe.IncBreakerTrip(reason)
}

func emitBreakerClosed(webhookID string) {
	slog.Default().Info("webhook circuit breaker closed", "webhook_id", webhookID)
	observe.SetBreakerState(webhookID, BreakerStateClosed)
}
