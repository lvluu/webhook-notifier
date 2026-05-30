package receiver

import (
	"hash/fnv"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// Option configures a ReceiverServer.
type Option interface {
	apply(*ReceiverServer)
}

type optionFunc func(*ReceiverServer)

func (f optionFunc) apply(rs *ReceiverServer) { f(rs) }

// WithStatus configures server-level status-code injection.
// Each request independently has prob chance of returning statusCode.
// Panics if prob is outside [0, 1].
func WithStatus(prob float64, statusCode int) Option {
	if prob < 0 || prob > 1 {
		panic("receiver: WithStatus prob must be in [0, 1]")
	}
	return optionFunc(func(rs *ReceiverServer) {
		rs.statusProb = prob
		rs.statusCode = statusCode
	})
}

// WithLatency configures server-level latency injection.
// Each request independently has prob chance of being delayed by d.
func WithLatency(prob float64, d time.Duration) Option {
	if prob < 0 || prob > 1 {
		panic("receiver: WithLatency prob must be in [0, 1]")
	}
	return optionFunc(func(rs *ReceiverServer) {
		rs.latencyProb = prob
		rs.latencyDur = d
	})
}

// WithDowntime configures server-level downtime injection.
// After afterN requests, the server returns 503 for duration.
func WithDowntime(afterN int, duration time.Duration) Option {
	if afterN < 0 {
		panic("receiver: WithDowntime afterN must be >= 0")
	}
	return optionFunc(func(rs *ReceiverServer) {
		rs.srvDowntime.afterN = afterN
		rs.srvDowntime.dur = duration
	})
}

// WithNowFunc overrides the time function used for downtime checks.
func WithNowFunc(fn func() time.Time) Option {
	return optionFunc(func(rs *ReceiverServer) {
		rs.nowFunc = fn
	})
}

// WithRandSource sets the random source used for injection decisions.
func WithRandSource(src rand.Source) Option {
	return optionFunc(func(rs *ReceiverServer) {
		rs.rng = rand.New(src)
	})
}

// WithStableDecisions makes probability checks deterministic per request
// identity instead of dependent on concurrent handler ordering.
func WithStableDecisions(seed string) Option {
	return optionFunc(func(rs *ReceiverServer) {
		rs.stableSeed = seed
	})
}

type downtimeState struct {
	afterN       int
	dur          time.Duration
	reqCount     int
	activeUntil  time.Time
	hasTriggered bool
}

type routeConfig struct {
	latencyProb       float64
	latencyDur        time.Duration
	latencyConfigured bool
	statusProb        float64
	statusCode        int
	statusConfigured  bool
	statusFunc        func(*http.Request, time.Time) int
}

// Recording holds the request data recorded by the ReceiverServer.
type Recording struct {
	ReceivedAt time.Time
	Method     string
	Path       string
	Headers    http.Header
	Body       []byte
	StatusCode int
}

// ReceiverServer is a configurable HTTP test server for benchmark scenarios.
type ReceiverServer struct {
	mu          sync.RWMutex
	srv         *httptest.Server
	rng         *rand.Rand
	statusProb  float64
	statusCode  int
	latencyProb float64
	latencyDur  time.Duration
	stableSeed  string
	nowFunc     func() time.Time
	srvDowntime downtimeState
	routes      map[string]*routeConfig
	recordings  []Recording
}

// WithRouteStatus configures route-level status-code injection.
// Requests to route independently have prob chance of returning statusCode,
// overriding any server-level WithStatus.
func WithRouteStatus(route string, prob float64, statusCode int) Option {
	if prob < 0 || prob > 1 {
		panic("receiver: WithRouteStatus prob must be in [0, 1]")
	}
	return optionFunc(func(rs *ReceiverServer) {
		rc := rs.routeConfig(route)
		rc.statusProb = prob
		rc.statusCode = statusCode
		rc.statusConfigured = true
	})
}

// WithRouteStatusFunc configures route-level status-code behavior from the
// current request and server clock. Return 0 to continue normal status handling.
func WithRouteStatusFunc(route string, fn func(*http.Request, time.Time) int) Option {
	if fn == nil {
		panic("receiver: WithRouteStatusFunc fn must not be nil")
	}
	return optionFunc(func(rs *ReceiverServer) {
		rc := rs.routeConfig(route)
		rc.statusFunc = fn
	})
}

// WithRouteLatency configures route-level latency injection.
// Requests to route independently have prob chance of being delayed by d,
// overriding any server-level WithLatency.
func WithRouteLatency(route string, prob float64, d time.Duration) Option {
	if prob < 0 || prob > 1 {
		panic("receiver: WithRouteLatency prob must be in [0, 1]")
	}
	return optionFunc(func(rs *ReceiverServer) {
		rc := rs.routeConfig(route)
		rc.latencyProb = prob
		rc.latencyDur = d
		rc.latencyConfigured = true
	})
}

func (rs *ReceiverServer) routeConfig(route string) *routeConfig {
	if rs.routes == nil {
		rs.routes = make(map[string]*routeConfig)
	}
	rc := rs.routes[route]
	if rc == nil {
		rc = &routeConfig{}
		rs.routes[route] = rc
	}
	return rc
}

// NewServer starts and returns a ReceiverServer configured with opts.
func NewServer(opts ...Option) *ReceiverServer {
	rs := &ReceiverServer{
		rng:     rand.New(rand.NewSource(0)),
		nowFunc: time.Now,
	}
	for _, opt := range opts {
		opt.apply(rs)
	}
	rs.srv = httptest.NewServer(http.HandlerFunc(rs.handle))
	return rs
}

// Close shuts down the underlying test server.
func (rs *ReceiverServer) Close() {
	rs.srv.Close()
}

// URL returns the base URL of the test server.
func (rs *ReceiverServer) URL() string {
	return rs.srv.URL
}

// Recorded returns a snapshot of all request recordings.
func (rs *ReceiverServer) Recorded() []Recording {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	out := make([]Recording, len(rs.recordings))
	copy(out, rs.recordings)
	return out
}

// Reset clears all recordings and resets downtime state.
func (rs *ReceiverServer) Reset() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.recordings = nil
	rs.srvDowntime = downtimeState{
		afterN: rs.srvDowntime.afterN,
		dur:    rs.srvDowntime.dur,
	}
}

func (rs *ReceiverServer) handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		body = nil
	}

	code := rs.serveCode(r)

	rs.mu.Lock()
	rs.recordings = append(rs.recordings, Recording{
		ReceivedAt: rs.nowFunc(),
		Method:     r.Method,
		Path:       r.URL.Path,
		Headers:    r.Header.Clone(),
		Body:       body,
		StatusCode: code,
	})
	rs.mu.Unlock()

	w.WriteHeader(code)
}

func (rs *ReceiverServer) serveCode(r *http.Request) int {
	if rs.inDowntime() {
		return http.StatusServiceUnavailable
	}

	rc := rs.routes[r.URL.Path]

	latencyProb := rs.latencyProb
	latencyDur := rs.latencyDur
	statusProb := rs.statusProb
	statusCode := rs.statusCode
	if rc != nil {
		if rc.statusConfigured {
			statusProb = rc.statusProb
			statusCode = rc.statusCode
		}
		if rc.latencyConfigured {
			latencyProb = rc.latencyProb
			latencyDur = rc.latencyDur
		}
	}

	if rc != nil && rc.statusFunc != nil {
		if code := rc.statusFunc(r, rs.nowFunc()); code != 0 {
			return code
		}
	}

	if latencyProb > 0 && rs.probabilityHit(r, "latency", latencyProb) {
		time.Sleep(latencyDur)
	}

	if statusProb > 0 && rs.probabilityHit(r, "status", statusProb) {
		return statusCode
	}
	return http.StatusOK
}

func (rs *ReceiverServer) probabilityHit(r *http.Request, kind string, prob float64) bool {
	if rs.stableSeed == "" {
		return rs.randFloat64() < prob
	}
	return stableProbability(rs.stableSeed, r, kind) < prob
}

func stableProbability(seed string, r *http.Request, kind string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(kind))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(r.URL.Path))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(r.Header.Get("X-Delivery-Id")))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(r.Header.Get("X-Attempt-Number")))
	return float64(h.Sum64()) / float64(^uint64(0))
}

func (rs *ReceiverServer) randFloat64() float64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.rng.Float64()
}

func (rs *ReceiverServer) inDowntime() bool {
	now := rs.nowFunc()

	rs.mu.Lock()
	defer rs.mu.Unlock()

	dt := &rs.srvDowntime
	dt.reqCount++

	if dt.hasTriggered {
		return now.Before(dt.activeUntil)
	}

	if dt.reqCount >= dt.afterN && dt.afterN > 0 {
		dt.activeUntil = now.Add(dt.dur)
		dt.hasTriggered = true
		return true
	}

	return false
}
