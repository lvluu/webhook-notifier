package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/lvluu/webhook-notifier/internal/observe"
	"github.com/lvluu/webhook-notifier/internal/outbox"
	"github.com/lvluu/webhook-notifier/internal/retry"
	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// Delivery contains the outbound HTTP request data for one webhook delivery.
type Delivery struct {
	EventID       string
	DeliveryID    string
	CustomerID    string
	URL           string
	Body          []byte
	EventType     string
	Secret        string
	AttemptNumber int
}

// ProcessStatus describes the outcome of processing one claimed Delivery.
type ProcessStatus int

const (
	// StatusSucceeded means the Delivery reached a terminal success/dead state.
	StatusSucceeded ProcessStatus = iota
	// StatusRequeued means host-level backpressure deferred this Delivery.
	StatusRequeued
)

// ProcessResult is the outcome returned after processing a Delivery.
type ProcessResult struct {
	Status ProcessStatus
	Err    error
}

// DeliveryStore mutates and claims Delivery rows. Satisfied by *outbox.Store.
type DeliveryStore interface {
	ClaimNextN(ctx context.Context, customerID string, limit int) ([]outbox.Delivery, error)
	ClaimNextBatch(ctx context.Context, customerIDs []string, limit int) ([]outbox.Delivery, error)
	HasClaimableWork(ctx context.Context, customerID string) (bool, error)
	HasClaimableWorkBatch(ctx context.Context, customerIDs []string) (map[string]bool, error)
	MarkSucceeded(ctx context.Context, deliveryID string, statusCode int, duration time.Duration) error
	MarkRetry(ctx context.Context, deliveryID string, statusCode int, errorClass string, delay time.Duration, duration time.Duration) error
	MarkDead(ctx context.Context, deliveryID string, statusCode int, errorClass string, reason string, duration time.Duration) error
	Requeue(ctx context.Context, deliveryID string, delay time.Duration) error
	RequeueBatch(ctx context.Context, deliveryIDs []string, delay time.Duration) error
	MarkSucceededBatch(ctx context.Context, items []outbox.BatchMarkItem) error
	MarkRetryBatch(ctx context.Context, items []outbox.BatchMarkItem) error
	MarkDeadBatch(ctx context.Context, items []outbox.BatchMarkItem) error
}

// Worker sends webhook Deliveries and records retry outcomes.
type Worker struct {
	HTTP        *http.Client
	RetryPolicy retry.RetryPolicy
	Scheduler   scheduler.Scheduler
	HostLimiter HostLimiter
	Logger      *slog.Logger
	Config      Config
}

// Config contains the worker settings whose values interact at runtime.
type Config struct {
	RequestTimeout      time.Duration
	MinBatchHint        int
	MaxBatchHint        int
	DeliveryConcurrency int
	EmptyClaimBackoff   time.Duration
	MultiPickCount      int
}

// DefaultConfig returns the worker runtime defaults.
func DefaultConfig() Config {
	return Config{
		RequestTimeout:      30 * time.Second,
		MinBatchHint:        32,
		MaxBatchHint:        64,
		DeliveryConcurrency: 16,
		EmptyClaimBackoff:   20 * time.Millisecond,
		MultiPickCount:      1,
	}
}

// Validate rejects internally inconsistent worker settings.
func (c Config) Validate() error {
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be > 0")
	}
	if c.MinBatchHint < 1 {
		return fmt.Errorf("min batch hint must be >= 1")
	}
	if c.MaxBatchHint < 0 {
		return fmt.Errorf("max batch hint must be >= 0")
	}
	if c.MaxBatchHint > 0 && c.MaxBatchHint < c.MinBatchHint {
		return fmt.Errorf("max batch hint must be 0 or >= min batch hint")
	}
	if c.DeliveryConcurrency < 1 {
		return fmt.Errorf("delivery concurrency must be >= 1")
	}
	if c.EmptyClaimBackoff <= 0 {
		return fmt.Errorf("empty claim backoff must be > 0")
	}
	if c.MultiPickCount < 1 {
		return fmt.Errorf("multi-pick count must be >= 1")
	}
	return nil
}

// ClaimLimit returns the effective claim limit for one scheduler pick.
func (c Config) ClaimLimit(batchHint uint32) int {
	limit := int(batchHint)
	if limit < c.MinBatchHint {
		limit = c.MinBatchHint
	}
	if c.MaxBatchHint >= c.MinBatchHint && limit > c.MaxBatchHint {
		limit = c.MaxBatchHint
	}
	return limit
}

// HTTPConfig configures the shared HTTP client used for webhook Attempts.
type HTTPConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
}

// Option configures a Worker.
type Option func(*Worker)

// WithHTTPClient sets the HTTP client used for webhook requests.
func WithHTTPClient(c *http.Client) Option {
	return func(w *Worker) { w.HTTP = c }
}

// WithHTTPConfig configures the worker's shared HTTP client transport.
func WithHTTPConfig(config HTTPConfig) Option {
	return func(w *Worker) { w.HTTP = NewHTTPClient(config) }
}

// WithRequestTimeout sets the per-Attempt request timeout.
func WithRequestTimeout(timeout time.Duration) Option {
	return func(w *Worker) {
		if timeout > 0 {
			w.Config.RequestTimeout = timeout
		}
	}
}

// WithRetryPolicy sets the retry policy used to classify delivery attempts.
func WithRetryPolicy(rp retry.RetryPolicy) Option {
	return func(w *Worker) { w.RetryPolicy = rp }
}

// WithHostLimiter sets the optional per-host concurrency limiter.
func WithHostLimiter(hl HostLimiter) Option {
	return func(w *Worker) { w.HostLimiter = hl }
}

// WithLogger sets the base structured logger used by the Worker.
func WithLogger(logger *slog.Logger) Option {
	return func(w *Worker) { w.Logger = logger }
}

// WithMinBatchHint sets the minimum batch size used during ClaimNextN.
func WithMinBatchHint(minBatchHint int) Option {
	return func(w *Worker) {
		if minBatchHint > 0 {
			w.Config.MinBatchHint = minBatchHint
		}
	}
}

// WithMaxBatchHint sets the maximum claim batch size. The scheduler batchHint
// (fairness weight) is clamped between MinBatchHint and MaxBatchHint before
// the claim query. Default 64 (roundrobin ClaimBatchSize). A value of 0 makes
// the upper bound unbounded. Lower favours fairness; higher favours throughput.
// Tunable via --claim-batch-hint flag.
func WithMaxBatchHint(maxBatchHint int) Option {
	return func(w *Worker) {
		if maxBatchHint >= 0 {
			w.Config.MaxBatchHint = maxBatchHint
		}
	}
}

// WithDeliveryConcurrency sets max concurrent HTTP delivery classifications
// per worker run-loop batch.
func WithDeliveryConcurrency(n int) Option {
	return func(w *Worker) {
		if n > 0 {
			w.Config.DeliveryConcurrency = n
		}
	}
}

// WithEmptyClaimBackoff sets the pause after an empty claim cycle.
func WithEmptyClaimBackoff(delay time.Duration) Option {
	return func(w *Worker) {
		if delay > 0 {
			w.Config.EmptyClaimBackoff = delay
		}
	}
}

// WithMultiPickCount sets how many customers the worker collects from the
// Scheduler before issuing a single ClaimNextBatch call. When > 1 the worker
// amortises the DB claim round-trip across many sparse Customers at the cost
// of a short fairness-delay window for the batch tail.
func WithMultiPickCount(n int) Option {
	return func(w *Worker) {
		if n > 0 {
			w.Config.MultiPickCount = n
		}
	}
}

// NewWithConfig returns a Worker using a validated worker Config plus optional
// dependency overrides.
func NewWithConfig(s scheduler.Scheduler, config Config, opts ...Option) (*Worker, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	w := newWithConfig(s, config)
	for _, opt := range opts {
		opt(w)
	}
	if err := w.Config.Validate(); err != nil {
		return nil, err
	}
	return w, nil
}

// New returns a Worker using scheduler s and optional overrides.
func New(s scheduler.Scheduler, opts ...Option) *Worker {
	w := newWithConfig(s, DefaultConfig())
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func newWithConfig(s scheduler.Scheduler, config Config) *Worker {
	w := &Worker{
		HTTP:        NewHTTPClient(DefaultHTTPConfig()),
		RetryPolicy: retry.New(retry.Config{}),
		Scheduler:   s,
		Logger:      observe.NewLogger(nil),
		Config:      config,
	}
	return w
}
