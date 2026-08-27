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
