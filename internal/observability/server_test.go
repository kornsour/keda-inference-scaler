package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServerHealthzAlwaysOK(t *testing.T) {
	srv := NewServer(":0", NewMetrics(), NewHealth(time.Minute))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", rec.Code)
	}
}

func TestServerReadyzReflectsHealth(t *testing.T) {
	h := NewHealth(10 * time.Millisecond)
	srv := NewServer(":0", NewMetrics(), h)

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz status during startup grace = %d, want 200", rec.Code)
	}

	time.Sleep(30 * time.Millisecond)
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz status after grace expired with no success = %d, want 503", rec.Code)
	}

	h.RecordSuccess()
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz status after a recorded success = %d, want 200", rec.Code)
	}
}

func TestServerMetricsExposesRegisteredCollectors(t *testing.T) {
	m := NewMetrics()
	m.IncGRPCRequest("IsActive")
	srv := NewServer(":0", m, NewHealth(time.Minute))

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "scaler_grpc_requests_total") {
		t.Fatalf("/metrics body missing scaler_grpc_requests_total:\n%s", body)
	}
}
