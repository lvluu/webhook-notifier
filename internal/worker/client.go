package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/lvluu/webhook-notifier/internal/signer"
)

// DefaultHTTPConfig returns the worker's production-oriented HTTP transport
// defaults.
func DefaultHTTPConfig() HTTPConfig {
	return HTTPConfig{
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   500,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// NewHTTPClient returns a shared HTTP client configured for webhook delivery.
func NewHTTPClient(config HTTPConfig) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          config.MaxIdleConns,
			MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
			MaxConnsPerHost:       config.MaxConnsPerHost,
			IdleConnTimeout:       config.IdleConnTimeout,
			TLSHandshakeTimeout:   config.TLSHandshakeTimeout,
			ResponseHeaderTimeout: config.ResponseHeaderTimeout,
			ExpectContinueTimeout: config.ExpectContinueTimeout,
		},
	}
}

// Deliver sends d and returns the HTTP status code.
func (w *Worker) Deliver(ctx context.Context, d Delivery) (int, error) {
	resp, err := w.Send(ctx, d)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if err := resp.Body.Close(); err != nil {
		return 0, fmt.Errorf("close response body: %w", err)
	}

	return resp.StatusCode, nil
}

// Send builds and executes the signed webhook HTTP request for d.
func (w *Worker) Send(ctx context.Context, d Delivery) (*http.Response, error) {
	if w.HTTP == nil {
		w.HTTP = NewHTTPClient(DefaultHTTPConfig())
	}
	var cancel context.CancelFunc
	if w.RequestTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, w.RequestTimeout)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Id", d.EventID)
	req.Header.Set("X-Delivery-Id", d.DeliveryID)
	req.Header.Set("X-Event-Type", d.EventType)
	if d.AttemptNumber > 0 {
		req.Header.Set("X-Attempt-Number", fmt.Sprintf("%d", d.AttemptNumber))
	}
	if d.Secret != "" {
		ts := time.Now()
		req.Header.Set("X-Signature-Timestamp", strconv.FormatInt(ts.Unix(), 10))
		req.Header.Set("X-Signature-256", signer.Sign([]byte(d.Secret), d.Body, ts))
	}

	resp, err := w.HTTP.Do(req)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, fmt.Errorf("post %s: %w", d.URL, err)
	}
	if cancel != nil && resp != nil && resp.Body != nil {
		resp.Body = cancelOnCloseReadCloser{
			ReadCloser: resp.Body,
			cancel:     cancel,
		}
	}

	return resp, nil
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnCloseReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
