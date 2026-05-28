package retry_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lvluu/webhook-notifier/internal/retry"
)

func TestRetryPolicyNextTruthTable(t *testing.T) {
	t.Parallel()

	policy := retry.New(retry.Config{})
	prevDelay := 10 * time.Second
	tests := []struct {
		name         string
		attempt      int
		prevDelay    time.Duration
		statusCode   int
		retryAfter   string
		err          error
		wantTerminal bool
		wantReason   string
		wantDelay    time.Duration
		wantRange    bool
	}{
		{
			name:         "2xx succeeds",
			attempt:      1,
			statusCode:   http.StatusNoContent,
			wantTerminal: true,
			wantReason:   retry.ReasonSucceeded,
		},
		{
			name:       "408 retries",
			attempt:    1,
			prevDelay:  prevDelay,
			statusCode: http.StatusRequestTimeout,
			wantReason: retry.ReasonRetryable408,
			wantRange:  true,
		},
		{
			name:       "429 without Retry-After retries with backoff",
			attempt:    1,
			prevDelay:  prevDelay,
			statusCode: http.StatusTooManyRequests,
			wantReason: retry.ReasonRetryable429,
			wantRange:  true,
		},
		{
			name:       "429 with Retry-After seconds is honoured",
			attempt:    1,
			prevDelay:  prevDelay,
			statusCode: http.StatusTooManyRequests,
			retryAfter: "17",
			wantReason: retry.ReasonRetryAfter,
			wantDelay:  17 * time.Second,
		},
		{
			name:       "429 with Retry-After date falls back to pure backoff",
			attempt:    1,
			prevDelay:  prevDelay,
			statusCode: http.StatusTooManyRequests,
			retryAfter: time.Now().Add(time.Hour).UTC().Format(http.TimeFormat),
			wantReason: retry.ReasonRetryable429,
			wantRange:  true,
		},
		{
			name:       "500 retries",
			attempt:    1,
			prevDelay:  prevDelay,
			statusCode: http.StatusInternalServerError,
			wantReason: retry.ReasonRetryable5xx,
			wantRange:  true,
		},
		{
			name:       "599 retries",
			attempt:    1,
			prevDelay:  prevDelay,
			statusCode: 599,
			wantReason: retry.ReasonRetryable5xx,
			wantRange:  true,
		},
		{
			name:       "network error retries",
			attempt:    1,
			prevDelay:  prevDelay,
			err:        errors.New("connection reset"),
			wantReason: retry.ReasonNetworkError,
			wantRange:  true,
		},
		{
			name:       "timeout retries",
			attempt:    1,
			prevDelay:  prevDelay,
			err:        context.DeadlineExceeded,
			wantReason: retry.ReasonTimeout,
			wantRange:  true,
		},
		{
			name:         "400 terminal",
			attempt:      1,
			statusCode:   http.StatusBadRequest,
			wantTerminal: true,
			wantReason:   retry.ReasonTerminal4xx,
		},
		{
			name:         "404 terminal",
			attempt:      1,
			statusCode:   http.StatusNotFound,
			wantTerminal: true,
			wantReason:   retry.ReasonTerminal4xx,
		},
		{
			name:         "attempt 8 exhaustion",
			attempt:      8,
			prevDelay:    prevDelay,
			statusCode:   http.StatusInternalServerError,
			wantTerminal: true,
			wantReason:   retry.ReasonExhausted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var resp *http.Response
			if tt.statusCode != 0 {
				resp = response(tt.statusCode, tt.retryAfter)
				defer resp.Body.Close()
			}
			delay, terminal, reason := policy.Next(tt.attempt, tt.prevDelay, resp, tt.err)
			if terminal != tt.wantTerminal {
				t.Fatalf("terminal: got %v, want %v", terminal, tt.wantTerminal)
			}
			if reason != tt.wantReason {
				t.Fatalf("reason: got %q, want %q", reason, tt.wantReason)
			}
			if tt.wantRange {
				if delay < retry.DefaultBaseDelay || delay > 30*time.Second {
					t.Fatalf("delay: got %s, want within [1s, 30s]", delay)
				}
				return
			}
			if delay != tt.wantDelay {
				t.Fatalf("delay: got %s, want %s", delay, tt.wantDelay)
			}
		})
	}
}

func TestRetryPolicyNextAll5xxAreRetryable(t *testing.T) {
	t.Parallel()

	policy := retry.New(retry.Config{})
	for statusCode := http.StatusInternalServerError; statusCode <= 599; statusCode++ {
		statusCode := statusCode
		t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
			t.Parallel()

			resp := response(statusCode, "")
			defer resp.Body.Close()

			delay, terminal, reason := policy.Next(1, time.Second, resp, nil)
			if terminal {
				t.Fatalf("terminal: got true, want false")
			}
			if reason != retry.ReasonRetryable5xx {
				t.Fatalf("reason: got %q, want %q", reason, retry.ReasonRetryable5xx)
			}
			if delay <= 0 {
				t.Fatalf("delay: got %s, want positive", delay)
			}
		})
	}
}

func TestRetryPolicyNextTerminal4xxExcept408And429(t *testing.T) {
	t.Parallel()

	policy := retry.New(retry.Config{})
	for statusCode := http.StatusBadRequest; statusCode <= 499; statusCode++ {
		if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests {
			continue
		}
		statusCode := statusCode
		t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
			t.Parallel()

			resp := response(statusCode, "")
			defer resp.Body.Close()

			delay, terminal, reason := policy.Next(1, time.Second, resp, nil)
			if !terminal {
				t.Fatalf("terminal: got false, want true")
			}
			if reason != retry.ReasonTerminal4xx {
				t.Fatalf("reason: got %q, want %q", reason, retry.ReasonTerminal4xx)
			}
			if delay != 0 {
				t.Fatalf("delay: got %s, want 0", delay)
			}
		})
	}
}

func response(statusCode int, retryAfter string) *http.Response {
	resp := &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
	}
	if retryAfter != "" {
		resp.Header.Set("Retry-After", retryAfter)
	}
	return resp
}
