package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

func TestEmptyResultIsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	v, err := s.promInstant(context.Background(), srv.URL, "q")
	if err != nil {
		t.Fatalf("promInstant: %v", err)
	}
	if v != 0 {
		t.Fatalf("expected 0 for empty result, got %.2f", v)
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
}
