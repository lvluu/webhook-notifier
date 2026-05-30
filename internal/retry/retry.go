package retry

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultMaxAttempts is the ADR-0003 retry budget for a Delivery.
	DefaultMaxAttempts = 8
	// DefaultBaseDelay is the first retry delay before exponential backoff.
	DefaultBaseDelay = time.Second
	// DefaultCapDelay is the maximum retry delay after backoff and jitter.
	DefaultCapDelay = 5 * time.Minute

	// ReasonSucceeded records a successful terminal delivery.
	ReasonSucceeded = "succeeded"
	// ReasonExhausted records a delivery that consumed its retry budget.
	ReasonExhausted = "exhausted"
	// ReasonRetryAfter records a retry scheduled from a Retry-After header.
	ReasonRetryAfter = "retry_after"
	// ReasonRetryable408 records an HTTP 408 retryable response.
	ReasonRetryable408 = "retryable_408"
	// ReasonRetryable429 records an HTTP 429 retryable response.
	ReasonRetryable429 = "retryable_429"
	// ReasonRetryable5xx records an HTTP 5xx retryable response.
	ReasonRetryable5xx = "retryable_5xx"
	// ReasonNetworkError records a retryable network error.
	ReasonNetworkError = "network_error"
	// ReasonTimeout records a retryable timeout.
	ReasonTimeout = "timeout"
	// ReasonTerminal4xx records a non-retryable HTTP 4xx response.
	ReasonTerminal4xx = "terminal_4xx"
)

// Config tunes RetryPolicy while preserving ADR-0003 defaults.
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	CapDelay    time.Duration
}

// RetryPolicy classifies delivery Attempts and computes retry delay.
type RetryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	capDelay    time.Duration
}

// New returns a RetryPolicy using ADR-0003 defaults for zero-valued config.
func New(config Config) RetryPolicy {
	policy := RetryPolicy{
		maxAttempts: config.MaxAttempts,
		baseDelay:   config.BaseDelay,
		capDelay:    config.CapDelay,
	}
	if policy.maxAttempts <= 0 {
		policy.maxAttempts = DefaultMaxAttempts
	}
	if policy.baseDelay <= 0 {
		policy.baseDelay = DefaultBaseDelay
	}
	if policy.capDelay <= 0 {
		policy.capDelay = DefaultCapDelay
	}
	if policy.capDelay < policy.baseDelay {
		policy.capDelay = policy.baseDelay
	}

	return policy
}

// Next returns the next retry delay, whether the outcome is terminal, and the
// reason that should be recorded for audit/DLQ classification.
func (p RetryPolicy) Next(attempt int, prevDelay time.Duration, resp *http.Response, err error) (time.Duration, bool, string) {
	return p.next(attempt, prevDelay, resp, err, "")
}

// NextWithKey classifies an Attempt like Next, using key as deterministic
// entropy for jitter so simultaneous failures do not retry in lockstep.
func (p RetryPolicy) NextWithKey(attempt int, prevDelay time.Duration, resp *http.Response, err error, key string) (time.Duration, bool, string) {
	return p.next(attempt, prevDelay, resp, err, key)
}

func (p RetryPolicy) next(attempt int, prevDelay time.Duration, resp *http.Response, err error, key string) (time.Duration, bool, string) {
	if p.maxAttempts <= 0 || p.baseDelay <= 0 || p.capDelay <= 0 {
		p = New(Config{
			MaxAttempts: p.maxAttempts,
			BaseDelay:   p.baseDelay,
			CapDelay:    p.capDelay,
		})
	}

	if resp != nil && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return 0, true, ReasonSucceeded
	}

	reason, retryable := p.classify(resp, err)
	if !retryable {
		return 0, true, reason
	}
	if attempt >= p.maxAttempts {
		return 0, true, ReasonExhausted
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if delay, ok := retryAfterDelay(resp.Header.Get("Retry-After")); ok {
			return delay, false, ReasonRetryAfter
		}
	}

	return p.decorrelatedJitter(attempt, prevDelay, reason, key), false, reason
}

func (p RetryPolicy) classify(resp *http.Response, err error) (string, bool) {
	if err != nil {
		if isTimeout(err) {
			return ReasonTimeout, true
		}
		return ReasonNetworkError, true
	}
	if resp == nil {
		return ReasonNetworkError, true
	}

	switch {
	case resp.StatusCode == http.StatusRequestTimeout:
		return ReasonRetryable408, true
	case resp.StatusCode == http.StatusTooManyRequests:
		return ReasonRetryable429, true
	case resp.StatusCode >= http.StatusInternalServerError && resp.StatusCode <= 599:
		return ReasonRetryable5xx, true
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode <= 499:
		return ReasonTerminal4xx, false
	default:
		return ReasonTerminal4xx, false
	}
}

func (p RetryPolicy) decorrelatedJitter(attempt int, prevDelay time.Duration, reason string, key string) time.Duration {
	high := p.baseDelay
	if prevDelay > 0 {
		high = prevDelay * 3
	}
	if high < p.baseDelay {
		high = p.baseDelay
	}
	if high > p.capDelay {
		high = p.capDelay
	}
	if high == p.baseDelay {
		return p.baseDelay
	}

	span := high - p.baseDelay
	fraction := deterministicFraction(attempt, prevDelay, reason, key)
	return p.baseDelay + time.Duration(math.Round(float64(span)*fraction))
}

func deterministicFraction(attempt int, prevDelay time.Duration, reason string, key string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strconv.Itoa(attempt)))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(prevDelay.String()))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(reason))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(key))
	return float64(h.Sum64()) / float64(math.MaxUint64)
}

func retryAfterDelay(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 0 {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
