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
