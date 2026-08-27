package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"google.golang.org/grpc/metadata"
)

// fakeProm returns a Prometheus /query response with the given scalar value
// for every query.
func fakeProm(t *testing.T, value string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"` + value + `"]}]}}`))
	}))
}

// fakePromByQuery dispatches on the `query` URL parameter so distinct
// queue/kv queries can return distinct values, letting tests pin exactly
// which dimension of the composite signal is driving the result.
func fakePromByQuery(t *testing.T, values map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		v, ok := values[q]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"` + v + `"]}]}}`))
	}))
}

func TestSaturationCompositeSignal(t *testing.T) {
	tests := []struct {
		name           string
		queue          string
		kv             string
		queueThreshold float64
		kvThreshold    float64
		want           float64
	}{
		{
			name:           "queue-dominant",
			queue:          "6",
			kv:             "0.1",
			queueThreshold: 3,
			kvThreshold:    0.7,
			want:           200, // 6/3 = 2.0 -> 200, vs kv 0.1/0.7 ~= 14.3
		},
		{
			name:           "kv-dominant",
			queue:          "0",
			kv:             "0.63",
			queueThreshold: 3,
			kvThreshold:    0.7,
			want:           90, // kv 0.63/0.7 = 0.9 -> 90, vs queue 0
		},
		{
			name:           "exactly at threshold",
			queue:          "3",
			kv:             "0",
			queueThreshold: 3,
			kvThreshold:    0.7,
			want:           100,
		},
		{
			name:           "both zero",
			queue:          "0",
			kv:             "0",
			queueThreshold: 3,
			kvThreshold:    0.7,
			want:           0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := fakePromByQuery(t, map[string]string{
				"queue_query": tt.queue,
				"kv_query":    tt.kv,
			})
			defer srv.Close()
			s := &scaler{http: srv.Client()}
			c := config{
				promAddr:       srv.URL,
				queueQuery:     "queue_query",
				kvQuery:        "kv_query",
				queueThreshold: tt.queueThreshold,
				kvThreshold:    tt.kvThreshold,
			}
			got, err := s.saturation(context.Background(), c)
			if err != nil {
				t.Fatalf("saturation: %v", err)
			}
			if got != tt.want {
				t.Fatalf("saturation = %.4f, want %.4f", got, tt.want)
			}
		})
	}
}

func TestSaturationZeroThresholdGuards(t *testing.T) {
	// Both thresholds zero must not divide by zero (and must not panic);
	// both scores are skipped, so saturation is 0 regardless of the raw
	// queue/kv values.
	srv := fakePromByQuery(t, map[string]string{
		"queue_query": "50",
		"kv_query":    "0.9",
	})
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	c := config{promAddr: srv.URL, queueQuery: "queue_query", kvQuery: "kv_query", queueThreshold: 0, kvThreshold: 0}
	got, err := s.saturation(context.Background(), c)
	if err != nil {
		t.Fatalf("saturation: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0 with zero thresholds, got %.2f", got)
	}

	// Only the queue threshold is zero: the queue score is skipped (not
	// NaN/Inf) and the kv score alone determines saturation.
	c2 := config{promAddr: srv.URL, queueQuery: "queue_query", kvQuery: "kv_query", queueThreshold: 0, kvThreshold: 0.9}
	got2, err := s.saturation(context.Background(), c2)
	if err != nil {
		t.Fatalf("saturation: %v", err)
	}
	if got2 != 100 {
		t.Fatalf("expected 100 (kv 0.9/0.9), got %.2f", got2)
	}
}

func TestSaturationQueryErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	c := config{promAddr: srv.URL, queueQuery: "q", kvQuery: "kv", queueThreshold: 3, kvThreshold: 0.7}
	if _, err := s.saturation(context.Background(), c); err == nil {
		t.Fatal("expected error when the queue query fails")
	}
}

func TestPromInstantErrorPaths(t *testing.T) {
	t.Run("http 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		if _, err := s.promInstant(context.Background(), srv.URL, "q"); err == nil {
			t.Fatal("expected error on HTTP 500")
		}
	})

	t.Run("malformed json", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{not valid json`))
		}))
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		if _, err := s.promInstant(context.Background(), srv.URL, "q"); err == nil {
			t.Fatal("expected error on malformed JSON")
		}
	})

	t.Run("numeric value[1] falls back to zero", func(t *testing.T) {
		// Prometheus always encodes the sample value as a JSON string, but
		// promInstant only type-asserts — a well-formed response whose
		// value[1] decodes as a number (not a string) currently falls back
		// to 0 rather than erroring. Pin that behavior.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,42]}]}}`))
		}))
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		v, err := s.promInstant(context.Background(), srv.URL, "q")
		if err != nil {
			t.Fatalf("promInstant: %v", err)
		}
		if v != 0 {
			t.Fatalf("expected fallback to 0 for non-string value, got %.2f", v)
		}
	})

	t.Run("request construction error", func(t *testing.T) {
		s := &scaler{http: http.DefaultClient}
		if _, err := s.promInstant(context.Background(), "://bad-url", "q"); err == nil {
			t.Fatal("expected error building the request for an invalid address")
		}
	})
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

func TestParseConfigDefaults(t *testing.T) {
	c, err := parseConfig(map[string]string{"prometheusAddress": "http://p:9090"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.promAddr != "http://p:9090" {
		t.Fatalf("promAddr = %q", c.promAddr)
	}
	if c.queueQuery != defaultQueueQuery {
		t.Fatalf("queueQuery = %q, want default %q", c.queueQuery, defaultQueueQuery)
	}
	if c.kvQuery != defaultKVCacheQuery {
		t.Fatalf("kvQuery = %q, want default %q", c.kvQuery, defaultKVCacheQuery)
	}
	if c.queueThreshold != defaultQueueThreshold {
		t.Fatalf("queueThreshold = %v, want default %v", c.queueThreshold, defaultQueueThreshold)
	}
	if c.kvThreshold != defaultKVThreshold {
		t.Fatalf("kvThreshold = %v, want default %v", c.kvThreshold, defaultKVThreshold)
	}
	if c.activation != defaultActivation {
		t.Fatalf("activation = %v, want default %v", c.activation, defaultActivation)
	}
}

func TestParseConfigRequiresPromAddr(t *testing.T) {
	if _, err := parseConfig(map[string]string{}); err == nil {
		t.Fatal("expected error when prometheusAddress is missing")
	}
}

func TestParseConfigAllSixMetadataKeys(t *testing.T) {
	m := map[string]string{
		"prometheusAddress":   "http://p:9090",
		"queueQuery":          "custom_queue_query",
		"kvCacheQuery":        "custom_kv_query",
		"queueThreshold":      "5",
		"kvCacheThreshold":    "0.42",
		"activationThreshold": "10",
	}
	c, err := parseConfig(m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.promAddr != "http://p:9090" {
		t.Fatalf("promAddr = %q", c.promAddr)
	}
	if c.queueQuery != "custom_queue_query" {
		t.Fatalf("queueQuery = %q", c.queueQuery)
	}
	if c.kvQuery != "custom_kv_query" {
		t.Fatalf("kvQuery = %q", c.kvQuery)
	}
	if c.queueThreshold != 5 {
		t.Fatalf("queueThreshold = %v", c.queueThreshold)
	}
	if c.kvThreshold != 0.42 {
		t.Fatalf("kvThreshold = %v", c.kvThreshold)
	}
	if c.activation != 10 {
		t.Fatalf("activation = %v", c.activation)
	}
}

func TestFloatOr(t *testing.T) {
	if got := floatOr("", 3); got != 3 {
		t.Fatalf("floatOr empty string = %v, want default 3", got)
	}
	if got := floatOr("2.5", 3); got != 2.5 {
		t.Fatalf("floatOr valid input = %v, want 2.5", got)
	}
	// Unparseable input silently falls back to the default rather than
	// erroring or propagating; pin the current behavior explicitly since
	// it's easy to accidentally "fix" without noticing it's user-facing.
	if got := floatOr("abc", 3); got != 3 {
		t.Fatalf("floatOr unparseable input = %v, want fallback default 3", got)
	}
}

func TestParseConfigUnparseableThresholdFallsBackToDefault(t *testing.T) {
	c, err := parseConfig(map[string]string{
		"prometheusAddress": "http://p:9090",
		"queueThreshold":    "abc",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.queueThreshold != defaultQueueThreshold {
		t.Fatalf("queueThreshold = %v, want silent fallback to default %v", c.queueThreshold, defaultQueueThreshold)
	}
}

func TestIsActive(t *testing.T) {
	t.Run("below activation threshold", func(t *testing.T) {
		srv := fakeProm(t, "0")
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		ref := &pb.ScaledObjectRef{
			Namespace: "ns",
			Name:      "obj",
			ScalerMetadata: map[string]string{
				"prometheusAddress":   srv.URL,
				"activationThreshold": "1",
			},
		}
		resp, err := s.IsActive(context.Background(), ref)
		if err != nil {
			t.Fatalf("IsActive: %v", err)
		}
		if resp.Result {
			t.Fatal("expected inactive when saturation is 0")
		}
	})

	t.Run("above activation threshold", func(t *testing.T) {
		// queue=6, threshold=3 -> saturation 200, well above the default
		// activation threshold of 1.
		srv := fakeProm(t, "6")
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		ref := &pb.ScaledObjectRef{
			Namespace: "ns",
			Name:      "obj",
			ScalerMetadata: map[string]string{
				"prometheusAddress": srv.URL,
			},
		}
		resp, err := s.IsActive(context.Background(), ref)
		if err != nil {
			t.Fatalf("IsActive: %v", err)
		}
		if !resp.Result {
			t.Fatal("expected active when saturation is well above threshold")
		}
	})

	t.Run("invalid config", func(t *testing.T) {
		s := &scaler{http: http.DefaultClient}
		if _, err := s.IsActive(context.Background(), &pb.ScaledObjectRef{ScalerMetadata: map[string]string{}}); err == nil {
			t.Fatal("expected error when prometheusAddress is missing")
		}
	})

	t.Run("query error propagates", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{"prometheusAddress": srv.URL}}
		if _, err := s.IsActive(context.Background(), ref); err == nil {
			t.Fatal("expected error to propagate from a failing query")
		}
	})
}

func TestGetMetricSpec(t *testing.T) {
	s := &scaler{}
	resp, err := s.GetMetricSpec(context.Background(), &pb.ScaledObjectRef{})
	if err != nil {
		t.Fatalf("GetMetricSpec: %v", err)
	}
	if len(resp.MetricSpecs) != 1 {
		t.Fatalf("expected exactly one metric spec, got %d", len(resp.MetricSpecs))
	}
	spec := resp.MetricSpecs[0]
	if spec.MetricName != metricName {
		t.Fatalf("MetricName = %q, want %q", spec.MetricName, metricName)
	}
	if spec.TargetSize != int64(targetValue) {
		t.Fatalf("TargetSize = %d, want %d", spec.TargetSize, int64(targetValue))
	}
	if spec.TargetSizeFloat != targetValue {
		t.Fatalf("TargetSizeFloat = %v, want %v", spec.TargetSizeFloat, targetValue)
	}
}

func TestGetMetrics(t *testing.T) {
	// queue=2.72, threshold=3 -> qScore=0.90666..., saturation=90.666...
	// (kv is 0, so it never dominates). math.Round should push the integer
	// MetricValue to 91 while the float field keeps the unrounded value.
	srv := fakePromByQuery(t, map[string]string{
		defaultQueueQuery:   "2.72",
		defaultKVCacheQuery: "0",
	})
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	req := &pb.GetMetricsRequest{
		ScaledObjectRef: &pb.ScaledObjectRef{
			Namespace:      "ns",
			ScalerMetadata: map[string]string{"prometheusAddress": srv.URL, "queueThreshold": "3"},
		},
	}
	resp, err := s.GetMetrics(context.Background(), req)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if len(resp.MetricValues) != 1 {
		t.Fatalf("expected exactly one metric value, got %d", len(resp.MetricValues))
	}
	mv := resp.MetricValues[0]
	if mv.MetricName != metricName {
		t.Fatalf("MetricName = %q, want %q", mv.MetricName, metricName)
	}
	wantFloat := (2.72 / 3.0) * targetValue
	if diff := mv.MetricValueFloat - wantFloat; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("MetricValueFloat = %v, want %v", mv.MetricValueFloat, wantFloat)
	}
	if mv.MetricValue != 91 {
		t.Fatalf("MetricValue = %d, want rounded 91", mv.MetricValue)
	}

	t.Run("invalid config", func(t *testing.T) {
		s := &scaler{http: http.DefaultClient}
		req := &pb.GetMetricsRequest{ScaledObjectRef: &pb.ScaledObjectRef{ScalerMetadata: map[string]string{}}}
		if _, err := s.GetMetrics(context.Background(), req); err == nil {
			t.Fatal("expected error when prometheusAddress is missing")
		}
	})

	t.Run("query error propagates", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		s := &scaler{http: srv.Client()}
		req := &pb.GetMetricsRequest{ScaledObjectRef: &pb.ScaledObjectRef{ScalerMetadata: map[string]string{"prometheusAddress": srv.URL}}}
		if _, err := s.GetMetrics(context.Background(), req); err == nil {
			t.Fatal("expected error to propagate from a failing query")
		}
	})
}

// fakeStreamServer is a minimal grpc.ServerStreamingServer[IsActiveResponse]
// used to drive StreamIsActive without a real gRPC connection.
type fakeStreamServer struct {
	ctx     context.Context
	sent    chan *pb.IsActiveResponse
	sendErr error
}

func (f *fakeStreamServer) Send(resp *pb.IsActiveResponse) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent <- resp
	return nil
}
func (f *fakeStreamServer) Context() context.Context     { return f.ctx }
func (f *fakeStreamServer) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStreamServer) SendHeader(metadata.MD) error { return nil }
func (f *fakeStreamServer) SetTrailer(metadata.MD)       {}
func (f *fakeStreamServer) SendMsg(m any) error          { return nil }
func (f *fakeStreamServer) RecvMsg(m any) error          { return nil }

func TestStreamIsActiveSendsOnEachTick(t *testing.T) {
	orig := streamIsActiveInterval
	streamIsActiveInterval = 5 * time.Millisecond
	defer func() { streamIsActiveInterval = orig }()

	srv := fakeProm(t, "6") // saturation 200, active
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{"prometheusAddress": srv.URL}}

	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 1)}

	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	select {
	case resp := <-fs.sent:
		if !resp.Result {
			t.Fatal("expected active result on stream tick")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a tick to be sent")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StreamIsActive returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamIsActive to return after context cancel")
	}
}

func TestStreamIsActiveReturnsImmediatelyWhenContextDone(t *testing.T) {
	orig := streamIsActiveInterval
	streamIsActiveInterval = time.Hour // long enough that only ctx.Done() can end the test
	defer func() { streamIsActiveInterval = orig }()

	s := &scaler{http: http.DefaultClient}
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{"prometheusAddress": "http://unused"}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 1)}

	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StreamIsActive returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamIsActive to return on an already-cancelled context")
	}
}

func TestStreamIsActiveSkipsSendOnQueryError(t *testing.T) {
	orig := streamIsActiveInterval
	streamIsActiveInterval = 5 * time.Millisecond
	defer func() { streamIsActiveInterval = orig }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := &scaler{http: srv.Client()}
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{"prometheusAddress": srv.URL}}

	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 1)}

	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	// A failing IsActive should be skipped (no send, no returned error) --
	// give it a couple of tick intervals to prove nothing arrives, then
	// cancel and confirm a clean return.
	select {
	case resp := <-fs.sent:
		t.Fatalf("expected no send on query error, got %+v", resp)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StreamIsActive returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamIsActive to return after context cancel")
	}
}
