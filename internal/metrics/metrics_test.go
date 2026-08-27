package metrics

import (
	"context"
	"errors"
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

func TestInstantParsesScalar(t *testing.T) {
	srv := fakeProm(t, "6")
	defer srv.Close()
	p := &Prometheus{HTTP: srv.Client()}
	got, err := p.Instant(context.Background(), srv.URL, "q")
	if err != nil {
		t.Fatalf("Instant: %v", err)
	}
	if got != 6 {
		t.Fatalf("got %.2f, want 6", got)
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
		t.Fatalf("expected ErrMissing, got value=%.2f err=%v", v, err)
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
		t.Fatalf("expected ErrMissing for non-string value, got value=%.2f err=%v", v, err)
	}
}

func TestInstantRequestConstructionError(t *testing.T) {
	p := &Prometheus{HTTP: http.DefaultClient}
	if _, err := p.Instant(context.Background(), "://bad-url", "q"); err == nil {
		t.Fatal("expected error building the request for an invalid address")
	}
}
