package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRecordAndExpose(t *testing.T) {
	m := NewMetrics()

	m.ObserveQueryDuration("queue", 25*time.Millisecond)
	m.IncQueryError("kv")
	m.SetSaturation("inference", "vllm", 142.5)
	m.SetSignalAge("inference", "vllm", "queue", 8*time.Second)
	m.IncGRPCRequest("IsActive")
	m.IncGRPCRequest("IsActive")
	m.IncStreamError()
	m.IncStreamError()
	m.IncStreamError()

	if got := testutil.CollectAndCount(m.QueryDuration); got != 1 {
		t.Fatalf("QueryDuration series count = %d, want 1", got)
	}
	if got := testutil.ToFloat64(m.QueryErrors.WithLabelValues("kv")); got != 1 {
		t.Fatalf("QueryErrors{kv} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Saturation.WithLabelValues("inference", "vllm")); got != 142.5 {
		t.Fatalf("Saturation{inference,vllm} = %v, want 142.5", got)
	}
	if got := testutil.ToFloat64(m.SignalAge.WithLabelValues("inference", "vllm", "queue")); got != 8 {
		t.Fatalf("SignalAge{inference,vllm,queue} = %v, want 8", got)
	}
	if got := testutil.ToFloat64(m.GRPCRequests.WithLabelValues("IsActive")); got != 2 {
		t.Fatalf("GRPCRequests{IsActive} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.StreamErrors); got != 3 {
		t.Fatalf("StreamErrors = %v, want 3", got)
	}
}

func TestMetricsNilIsSafe(t *testing.T) {
	var m *Metrics
	m.ObserveQueryDuration("queue", time.Second) // must not panic
	m.IncQueryError("kv")
	m.SetSaturation("ns", "obj", 1)
	m.SetSignalAge("ns", "obj", "queue", time.Second)
	m.IncGRPCRequest("IsActive")
	m.IncStreamError()
	if m.Registry() != nil {
		t.Fatal("expected a nil *Metrics to return a nil registry")
	}
}

func TestNewMetricsRegistersDistinctCollectorsPerInstance(t *testing.T) {
	// Two independent Metrics must not collide on "already registered" --
	// each owns its own registry.
	a := NewMetrics()
	b := NewMetrics()
	a.IncGRPCRequest("IsActive")
	b.IncGRPCRequest("IsActive")
	if got := testutil.ToFloat64(a.GRPCRequests.WithLabelValues("IsActive")); got != 1 {
		t.Fatalf("a.GRPCRequests{IsActive} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(b.GRPCRequests.WithLabelValues("IsActive")); got != 1 {
		t.Fatalf("b.GRPCRequests{IsActive} = %v, want 1", got)
	}
}
