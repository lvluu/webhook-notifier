package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type eventRequest struct {
	CustomerID string          `json:"customer_id"`
	Type       string          `json:"event_name"`
	EventTime  string          `json:"event_time"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// Config tunes the chaos Emitter.
type Config struct {
	NotifierURL  string
	Rate         float64
	MaxInFlight  int
	Duration     time.Duration
	CustomerIDs  []string
	CustomerBase string
	HTTPClient   *http.Client
}

// Emitter emits synthetic webhook events at a smooth rate while capping
// concurrent in-flight requests.
type Emitter struct {
	cfg         Config
	limiter     *rate.Limiter
	sem         chan struct{}
	sent        atomic.Int64
	failed      atomic.Int64
	shed        atomic.Int64
	customerMu  sync.Mutex
	customerIdx int
	startTime   time.Time
}

// New returns an Emitter configured for the given parameters.
func New(cfg Config) *Emitter {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 10
	}
	if cfg.Rate <= 0 {
		cfg.Rate = 1
	}
	// Allow a burst of 1 so the first event fires immediately; the steady
	// rate governs the remainder.
	return &Emitter{
		cfg:     cfg,
		limiter: rate.NewLimiter(rate.Limit(cfg.Rate), 1),
		sem:     make(chan struct{}, cfg.MaxInFlight),
	}
}

// Sent returns the number of events that received an HTTP 2xx response.
func (e *Emitter) Sent() int64 { return e.sent.Load() }

// Failed returns the number of events that received a network error or a
// non-2xx, non-429, non-503 HTTP response.
func (e *Emitter) Failed() int64 { return e.failed.Load() }

// Shed returns the number of events that received HTTP 429 or 503 (backpressure signals).
func (e *Emitter) Shed() int64 { return e.shed.Load() }

// Run blocks until ctx is cancelled, emitting events at the configured rate.
func (e *Emitter) Run(ctx context.Context) {
	e.startTime = time.Now()
	logger := slog.Default().With("service", "chaos-emitter")

	for {
		if err := e.limiter.Wait(ctx); err != nil {
			return
		}

		if e.cfg.Duration > 0 && time.Since(e.startTime) >= e.cfg.Duration {
			return
		}

		cid := e.nextCustomer()

		data, _ := json.Marshal(map[string]interface{}{
			"subscriber_id": fmt.Sprintf("sub-%d", time.Now().UnixNano()),
			"source":        e.cfg.CustomerBase,
		})

		body, _ := json.Marshal(eventRequest{
			CustomerID: cid,
			Type:       "subscriber.created",
			EventTime:  time.Now().UTC().Format(time.RFC3339),
			Data:       data,
		})

		e.sem <- struct{}{}
		go func() {
			defer func() { <-e.sem }()
			e.post(logger, body)
		}()
	}
}

func (e *Emitter) nextCustomer() string {
	e.customerMu.Lock()
	defer e.customerMu.Unlock()
	cid := e.cfg.CustomerIDs[e.customerIdx%len(e.cfg.CustomerIDs)]
	e.customerIdx++
	return cid
}

func (e *Emitter) post(logger *slog.Logger, body []byte) {
	resp, err := e.cfg.HTTPClient.Post(
		e.cfg.NotifierURL+"/api/events",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		e.failed.Add(1)
		return
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.Warn("close event response body", "error", cerr)
		}
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		e.sent.Add(1)
	case resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests:
		e.shed.Add(1)
	default:
		e.failed.Add(1)
	}
}
