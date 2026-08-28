package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeProm returns a Prometheus /query response with the given scalar value,
// sampled at ts (a Unix timestamp in seconds, as Prometheus encodes it).
func fakeProm(t *testing.T, value string, ts float64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body := fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[%v,"%s"]}]}}`, ts, value)
		_, _ = w.Write([]byte(body))
	}))
}

func TestInstantParsesScalar(t *testing.T) {
	sampleTime := time.Now().Add(-9500 * time.Millisecond)
	srv := fakeProm(t, "6", float64(sampleTime.UnixNano())/1e9)
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	got, err := p.Instant(context.Background(), srv.URL, "q")
	if err != nil {
		t.Fatalf("Instant: %v", err)
	}
	if got.Value != 6 {
		t.Fatalf("got %.2f, want 6", got.Value)
	}
	// Prometheus's own sample timestamp should come through, not the time
	// the query happened to run -- within a generous tolerance for the
	// float64<->time.Time round trip.
	if diff := got.Time.Sub(sampleTime); diff > 5*time.Millisecond || diff < -5*time.Millisecond {
		t.Fatalf("Time = %v, want close to %v (diff %v)", got.Time, sampleTime, diff)
	}
}

func TestInstantEmptyResultIsErrMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	v, err := p.Instant(context.Background(), srv.URL, "q")
	if !errors.Is(err, ErrMissing) {
		t.Fatalf("expected ErrMissing, got value=%.2f err=%v", v.Value, err)
	}
}

func TestInstantNonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	if _, err := p.Instant(context.Background(), srv.URL, "q"); err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

func TestInstantMalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	if _, err := p.Instant(context.Background(), srv.URL, "q"); err == nil {
		t.Fatal("expected error on malformed JSON")
	}
}

func TestInstantUndecodableValueIsErrMissing(t *testing.T) {
	// Prometheus always encodes the sample value as a JSON string, but
	// Instant only type-asserts — a well-formed response whose value[1]
	// decodes as a number (not a string) is treated the same as an absent
	// series: ErrMissing, not a silent fallback to 0. Pin that behavior.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,42]}]}}`))
	}))
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	v, err := p.Instant(context.Background(), srv.URL, "q")
	if !errors.Is(err, ErrMissing) {
		t.Fatalf("expected ErrMissing for non-string value, got value=%.2f err=%v", v.Value, err)
	}
}

func TestInstantMalformedTimestampFallsBackToNow(t *testing.T) {
	// The timestamp half of the [timestamp, value] pair is a JSON string
	// here instead of a number -- malformed, but only the ancillary
	// staleness field. The sample's value should still parse rather than
	// the whole query failing over it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":["not-a-number","6"]}]}}`))
	}))
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	before := time.Now()
	got, err := p.Instant(context.Background(), srv.URL, "q")
	if err != nil {
		t.Fatalf("Instant: %v", err)
	}
	if got.Value != 6 {
		t.Fatalf("got %.2f, want 6", got.Value)
	}
	if got.Time.Before(before) {
		t.Fatalf("expected Time to fall back to roughly now, got %v (before request was %v)", got.Time, before)
	}
}

func TestInstantRequestConstructionError(t *testing.T) {
	p := &Prometheus{HTTP: http.DefaultClient}
	if _, err := p.Instant(context.Background(), "://bad-url", "q"); err == nil {
		t.Fatal("expected error building the request for an invalid address")
	}
}
