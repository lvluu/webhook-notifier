package observe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/alexliesenfeld/health"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// InfraServer bundles the /metrics, /health, /ready, and optionally /debug/pprof
// HTTP handlers into a single server. Intended to run on a separate port from
// the business HTTP server.
type InfraServer struct {
	srv *http.Server
}

// InfraServerConfig configures the infra HTTP server.
type InfraServerConfig struct {
	Addr      string
	DB        *sql.DB
	Pprof     bool
	Collector prometheus.Gatherer
}

// NewInfraServer creates an infra HTTP server exposing the configured endpoints.
func NewInfraServer(cfg InfraServerConfig) *InfraServer {
	gatherer := cfg.Collector
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))

	if cfg.DB != nil {
		dbCheck := health.Check{
			Name: "database",
			Check: func(ctx context.Context) error {
				return cfg.DB.PingContext(ctx)
			},
		}
		schemaCheck := health.Check{
			Name: "schema",
			Check: func(ctx context.Context) error {
				var ok bool
				err := cfg.DB.QueryRowContext(ctx,
					"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'deliveries')",
				).Scan(&ok)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("deliveries table not found")
				}
				return nil
			},
			Timeout: 5 * time.Second,
		}
		mux.Handle("GET /health", health.NewHandler(
			health.NewChecker(
				health.WithCacheDuration(5*time.Second),
				health.WithCheck(dbCheck),
			),
		))
		mux.Handle("GET /ready", health.NewHandler(
			health.NewChecker(
				health.WithCacheDuration(5*time.Second),
				health.WithCheck(dbCheck),
				health.WithCheck(schemaCheck),
			),
		))
	} else {
		mux.Handle("GET /health", health.NewHandler(health.NewChecker()))
		mux.Handle("GET /ready", health.NewHandler(health.NewChecker()))
	}

	if cfg.Pprof {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	return &InfraServer{
		srv: &http.Server{
			Addr:    cfg.Addr,
			Handler: mux,
		},
	}
}

// Start runs the server in a goroutine. Returns a channel that receives
// a non-nil error when the server shuts down unexpectedly.
func (s *InfraServer) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("infra server: %w", err)
		}
		close(errCh)
	}()
	return errCh
}

// Shutdown gracefully stops the server. Returns the result of the underlying
// http.Server.Shutdown call.
func (s *InfraServer) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
