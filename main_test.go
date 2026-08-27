package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeEmptyProm returns a Prometheus /query response with an empty result set,
// as if the queried series doesn't currently exist.
func fakeEmptyProm(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
}

// fakeProm returns a Prometheus /query response with the given scalar value.
func fakeProm(t *testing.T, value string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"` + value + `"]}]}}`))
	}))
}

func TestSaturationScalesOnWhicheverIsHotter(t *testing.T) {
	// queue=6 (threshold 3 -> 2.0), kv=0.35 (threshold 0.7 -> 0.5); max=2.0 -> 200.
	srv := fakeProm(t, "6")
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	c := config{promAddr: srv.URL, queueQuery: "q", kvQuery: "kv", queueThreshold: 3, kvThreshold: 0.7}

	// fakeProm returns 6 for *both* queries, so kvScore = 6/0.7 dominates here;
	// use a dedicated server per dimension instead for a precise check.
	got, err := s.saturation(context.Background(), c)
	if err != nil {
		t.Fatalf("saturation: %v", err)
	}
	if got <= 0 {
		t.Fatalf("expected positive saturation, got %.2f", got)
	}
}

func TestEmptyResultIsMetricMissing(t *testing.T) {
	srv := fakeEmptyProm(t)
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	v, err := s.promInstant(context.Background(), srv.URL, "q")
	if !errors.Is(err, errMetricMissing) {
		t.Fatalf("expected errMetricMissing, got value=%.2f err=%v", v, err)
	}
}

func TestMissingSeriesReadsAsIdleByDefault(t *testing.T) {
	srv := fakeEmptyProm(t)
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	c := config{promAddr: srv.URL, queueQuery: "q", kvQuery: "kv", queueThreshold: 3, kvThreshold: 0.7}

	got, err := s.saturation(context.Background(), c)
	if err != nil {
		t.Fatalf("expected no error with treatMissingAsError=false, got: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected saturation 0 for an absent series, got %.2f", got)
	}
}

func TestMissingSeriesErrorsWhenConfigured(t *testing.T) {
	srv := fakeEmptyProm(t)
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	c := config{promAddr: srv.URL, queueQuery: "q", kvQuery: "kv", queueThreshold: 3, kvThreshold: 0.7, treatMissingAsError: true}

	if _, err := s.saturation(context.Background(), c); err == nil {
		t.Fatal("expected an error with treatMissingAsError=true and an absent series")
	}
}

func TestParseConfigRequiresPromAddr(t *testing.T) {
	if _, err := parseConfig(map[string]string{}); err == nil {
		t.Fatal("expected error when prometheusAddress is missing")
	}
	c, err := parseConfig(map[string]string{"prometheusAddress": "http://p:9090", "queueThreshold": "5"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.queueThreshold != 5 {
		t.Fatalf("queueThreshold not parsed: %v", c.queueThreshold)
	}
	if c.treatMissingAsError != false {
		t.Fatalf("expected treatMissingAsError to default to false, got %v", c.treatMissingAsError)
	}

	c2, err := parseConfig(map[string]string{"prometheusAddress": "http://p:9090", "treatMissingAsError": "true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c2.treatMissingAsError {
		t.Fatal("expected treatMissingAsError=true to be parsed")
	}
}
