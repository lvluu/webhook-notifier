package scenarios

import (
	"database/sql"

	"github.com/lvluu/webhook-notifier/internal/outbox"
)

func baselineOutboxStore(db *sql.DB) *outbox.Store {
	return outbox.New(db, outbox.WithBreakerConfig(outbox.BreakerConfig{
		FailureThreshold: 1_000_000,
	}))
}
