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
}

// New returns an Emitter configured for the given parameters.
func New(cfg Config) (*Emitter, error) {
	if cfg.NotifierURL == "" {
		return nil, fmt.Errorf("notifier URL is required")
	}
	if len(cfg.CustomerIDs) == 0 {
		return nil, fmt.Errorf("at least one customer ID is required")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
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
	}, nil
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
	logger := slog.Default().With("service", "chaos-emitter")
	var wg sync.WaitGroup

	admitCtx := ctx
	if e.cfg.Duration > 0 {
		var cancel context.CancelFunc
		admitCtx, cancel = context.WithTimeout(ctx, e.cfg.Duration)
		defer cancel()
	}

	for {
		if err := e.limiter.Wait(admitCtx); err != nil {
			wg.Wait()
			return
		}

		cid := e.nextCustomer()

		body, err := e.eventBody(cid)
		if err != nil {
			e.failed.Add(1)
			logger.Error("build event request", "customer", cid, "error", err)
			continue
		}

		select {
		case e.sem <- struct{}{}:
		case <-admitCtx.Done():
			wg.Wait()
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-e.sem }()
			e.post(ctx, logger, body)
		}()
	}
}

func (e *Emitter) eventBody(cid string) ([]byte, error) {
	data, err := json.Marshal(map[string]string{
		"subscriber_id": fmt.Sprintf("sub-%d", time.Now().UnixNano()),
		"source":        e.cfg.CustomerBase,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal event data: %w", err)
	}

	body, err := json.Marshal(eventRequest{
		CustomerID: cid,
		Type:       "subscriber.created",
		EventTime:  time.Now().UTC().Format(time.RFC3339),
		Data:       data,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal event request: %w", err)
	}
	return body, nil
}

func (e *Emitter) nextCustomer() string {
	e.customerMu.Lock()
	defer e.customerMu.Unlock()
	cid := e.cfg.CustomerIDs[e.customerIdx%len(e.cfg.CustomerIDs)]
	e.customerIdx++
	return cid
}

func (e *Emitter) post(ctx context.Context, logger *slog.Logger, body []byte) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		e.cfg.NotifierURL+"/api/events",
		bytes.NewReader(body),
	)
	if err != nil {
		e.failed.Add(1)
		logger.Error("build event POST request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.cfg.HTTPClient.Do(req)
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
