package selfmetrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestCounterIncAndValue(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("things_total", "count of things")
	if c.Value() != 0 {
		t.Fatalf("new counter value = %d, want 0", c.Value())
	}
	c.Inc()
	c.Inc()
	c.Inc()
	if c.Value() != 3 {
		t.Fatalf("value after 3 Inc() = %d, want 3", c.Value())
	}
}

func TestCounterIncIsConcurrencySafe(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("things_total", "count of things")

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Inc()
		}()
	}
	wg.Wait()

	if c.Value() != 100 {
		t.Fatalf("value after 100 concurrent Inc() = %d, want 100", c.Value())
	}
}

func TestHandlerServesPrometheusTextFormat(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("keda_inference_scaler_stream_errors_total", "Total StreamIsActive query failures.")
	c.Inc()
	c.Inc()

	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain prefix", ct)
	}

	var body strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		body.Write(buf[:n])
		if err != nil {
			break
		}
	}

	got := body.String()
	if !strings.Contains(got, "# HELP keda_inference_scaler_stream_errors_total Total StreamIsActive query failures.") {
		t.Errorf("missing HELP line, got:\n%s", got)
	}
	if !strings.Contains(got, "# TYPE keda_inference_scaler_stream_errors_total counter") {
		t.Errorf("missing TYPE line, got:\n%s", got)
	}
	if !strings.Contains(got, "keda_inference_scaler_stream_errors_total 2") {
		t.Errorf("missing value line, got:\n%s", got)
	}
}

func TestHandlerServesMultipleCounters(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("a_total", "a")
	r.NewCounter("b_total", "b")

	var body strings.Builder
	r.writeTo(&body)

	got := body.String()
	if !strings.Contains(got, "a_total 0") || !strings.Contains(got, "b_total 0") {
		t.Errorf("expected both counters present, got:\n%s", got)
	}
}
