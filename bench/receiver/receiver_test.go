package receiver

import (
	"math"
	"math/rand"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDowntime(t *testing.T) {
	t.Parallel()

	fakeNow := time.Now()
	nowFunc := func() time.Time { return fakeNow }

	srv := NewServer(
		WithDowntime(5, 2*time.Second),
		WithNowFunc(nowFunc),
	)
	defer srv.Close()

	url := srv.URL()

	t.Run("before threshold", func(t *testing.T) {
		for i := range 4 {
			resp, err := http.Post(url+"/dt", "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("req %d: got %d, want 200", i, resp.StatusCode)
			}
		}
	})

	t.Run("trigger downtime", func(t *testing.T) {
		fakeNow = fakeNow.Add(time.Second)
		resp, err := http.Post(url+"/dt", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("got %d, want 503", resp.StatusCode)
		}
	})

	t.Run("during window", func(t *testing.T) {
		fakeNow = fakeNow.Add(500 * time.Millisecond)
		resp, err := http.Post(url+"/dt", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("got %d, want 503", resp.StatusCode)
		}
	})

	t.Run("after recovery", func(t *testing.T) {
		fakeNow = fakeNow.Add(2 * time.Second)
		resp, err := http.Post(url+"/dt", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("got %d, want 200", resp.StatusCode)
		}
	})
}

func TestRandIsolation(t *testing.T) {
	t.Parallel()

	collect := func(srv *ReceiverServer) []int {
		var seq []int
		for range 20 {
			resp, err := http.Get(srv.URL())
			if err != nil {
				t.Fatal(err)
			}
			seq = append(seq, resp.StatusCode)
			resp.Body.Close()
		}
		return seq
	}

	srvSame1 := NewServer(
		WithStatus(0.5, http.StatusTeapot),
		WithRandSource(rand.NewSource(42)),
	)
	srvSame2 := NewServer(
		WithStatus(0.5, http.StatusTeapot),
		WithRandSource(rand.NewSource(42)),
	)
	srvDiff := NewServer(
		WithStatus(0.5, http.StatusTeapot),
		WithRandSource(rand.NewSource(99)),
	)
	defer srvSame1.Close()
	defer srvSame2.Close()
	defer srvDiff.Close()

	seq1 := collect(srvSame1)
	seq2 := collect(srvSame2)
	seq3 := collect(srvDiff)

	if !slices.Equal(seq1, seq2) {
		t.Error("same seed produced different sequences — sources are not isolated")
	}
	if slices.Equal(seq1, seq3) {
		t.Error("different seeds produced identical sequences")
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	srv := NewServer()
	defer srv.Close()

	body := `{"hello": "world"}`
	resp, err := http.Post(srv.URL()+"/reset-me", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if len(srv.Recorded()) != 1 {
		t.Fatal("expected 1 recording before reset")
	}

	srv.Reset()

	if len(srv.Recorded()) != 0 {
		t.Errorf("got %d recordings after reset, want 0", len(srv.Recorded()))
	}
}

func TestRecorded(t *testing.T) {
	t.Parallel()

	srv := NewServer()
	defer srv.Close()

	body := `{"test": true}`
	resp, err := http.Post(srv.URL()+"/record-me", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	got := srv.Recorded()
	if len(got) != 1 {
		t.Fatalf("got %d recordings, want 1", len(got))
	}

	r := got[0]
	if r.Method != http.MethodPost {
		t.Errorf("got method %s, want POST", r.Method)
	}
	if r.Path != "/record-me" {
		t.Errorf("got path %s, want /record-me", r.Path)
	}
	if r.StatusCode != http.StatusOK {
		t.Errorf("got status %d, want 200", r.StatusCode)
	}
	if string(r.Body) != body {
		t.Errorf("got body %s, want %s", string(r.Body), body)
	}
	if r.ReceivedAt.IsZero() {
		t.Error("ReceivedAt should not be zero")
	}
}

func TestDowntimePrecedence(t *testing.T) {
	t.Parallel()

	fakeNow := time.Now()
	nowFunc := func() time.Time { return fakeNow }

	srv := NewServer(
		WithDowntime(2, 5*time.Second),
		WithStatus(1.0, http.StatusOK),
		WithNowFunc(nowFunc),
	)
	defer srv.Close()

	url := srv.URL()

	t.Run("before downtime", func(t *testing.T) {
		resp, err := http.Post(url+"/prec", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("got %d, want 200", resp.StatusCode)
		}
	})

	t.Run("downtime overrides status", func(t *testing.T) {
		fakeNow = fakeNow.Add(time.Second)
		resp, err := http.Post(url+"/prec", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("got %d, want 503 (downtime should override status injection)", resp.StatusCode)
		}
	})
}

func TestLatencyInjection(t *testing.T) {
	t.Parallel()

	srv := NewServer(
		WithLatency(1.0, 50*time.Millisecond),
	)
	defer srv.Close()

	url := srv.URL()
	for i := range 10 {
		start := time.Now()
		resp, err := http.Post(url+"/latency-test", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		elapsed := time.Since(start)
		if elapsed < 50*time.Millisecond {
			t.Errorf("request %d: elapsed %v, want ≥50ms", i, elapsed)
		}
	}
}

func TestRouteStatusOverridesServer(t *testing.T) {
	t.Parallel()

	srv := NewServer(
		WithRouteStatus("/bad", 1.0, http.StatusNotFound),
	)
	defer srv.Close()

	base := srv.URL()

	tests := []struct {
		name string
		path string
		want int
	}{
		{"route with override", "/bad", http.StatusNotFound},
		{"route without override", "/good", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(base+tt.path, "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("got status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestRouteStatusZeroProbOverridesServer(t *testing.T) {
	t.Parallel()

	srv := NewServer(
		WithStatus(1.0, http.StatusInternalServerError),
		WithRouteStatus("/opt-out", 0, http.StatusTeapot),
	)
	defer srv.Close()

	resp, err := http.Post(srv.URL()+"/opt-out", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("got status %d, want 200", resp.StatusCode)
	}
}

func TestRouteLatency(t *testing.T) {
	t.Parallel()

	srv := NewServer(
		WithRouteLatency("/slow", 1.0, 50*time.Millisecond),
	)
	defer srv.Close()

	start := time.Now()
	resp, err := http.Post(srv.URL()+"/slow", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Errorf("elapsed %v, want ≥50ms", elapsed)
	}
}

func TestConcurrentInjection(t *testing.T) {
	t.Parallel()

	const requests = 64

	srv := NewServer(
		WithStatus(0.5, http.StatusInternalServerError),
		WithLatency(0.5, time.Microsecond),
		WithDowntime(requests+1, time.Second),
		WithRandSource(rand.NewSource(42)),
	)
	defer srv.Close()

	var wg sync.WaitGroup
	errs := make(chan error, requests)

	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := http.Post(srv.URL()+"/concurrent", "application/json", strings.NewReader(`{}`))
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := len(srv.Recorded()); got != requests {
		t.Errorf("got %d recordings, want %d", got, requests)
	}
}

func TestStatusInjection(t *testing.T) {
	t.Parallel()

	prob := 0.3
	n := 1000
	σ := math.Sqrt(float64(n) * prob * (1 - prob))
	expected := float64(n) * prob
	threshold := 3 * σ

	srv := NewServer(
		WithStatus(prob, http.StatusInternalServerError),
		WithRandSource(rand.NewSource(42)),
	)
	defer srv.Close()

	url := srv.URL()
	var got500 int
	for range n {
		resp, err := http.Post(url+"/status-test", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusInternalServerError {
			got500++
		}
	}

	got := float64(got500)
	if math.Abs(got-expected) > threshold {
		t.Errorf("got %d 500s, expected %.0f ± %.0f (%.1fσ off)",
			got500, expected, threshold, math.Abs(got-expected)/σ)
	}
}
