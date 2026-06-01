package observe

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

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
