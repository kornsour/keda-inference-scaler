package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kornsour/keda-inference-scaler/internal/config"
	"github.com/kornsour/keda-inference-scaler/internal/metrics"
	"github.com/kornsour/keda-inference-scaler/internal/saturation"

	pb "github.com/kornsour/keda-inference-scaler/externalscaler"
	"google.golang.org/grpc/metadata"
)

// fakeSource is a metrics.Source that returns canned values per query, with no
// HTTP involved — exactly the point of depending on the Source interface. A
// query listed in missing returns metrics.ErrMissing; a query listed in errs
// returns that error, for tests that need a non-ErrMissing failure to
// propagate.
type fakeSource struct {
	values  map[string]float64
	missing map[string]bool
	errs    map[string]error
}

func (f *fakeSource) Instant(_ context.Context, _, query string) (float64, error) {
	if err, ok := f.errs[query]; ok {
		return 0, err
	}
	if f.missing[query] {
		return 0, metrics.ErrMissing
	}
	return f.values[query], nil
}

func TestScalerSaturationForUsesConfiguredQueries(t *testing.T) {
	s := &scaler{source: &fakeSource{values: map[string]float64{
		"queue_q": 6,
		"kv_q":    0.35,
	}}}
	c := config.Config{
		PromAddr:       "unused",
		QueueQuery:     "queue_q",
		KVQuery:        "kv_q",
		QueueThreshold: 3,
		KVThreshold:    0.7,
	}

	// queue=6/3=2.0, kv=0.35/0.7=0.5 -> max is queue -> 200.
	got, err := s.saturationFor(context.Background(), c)
	if err != nil {
		t.Fatalf("saturationFor: %v", err)
	}
	if got != 200 {
		t.Fatalf("got %.2f, want 200", got)
	}
}

func TestMissingSeriesReadsAsIdleByDefault(t *testing.T) {
	s := &scaler{source: &fakeSource{missing: map[string]bool{"queue_q": true, "kv_q": true}}}
	c := config.Config{PromAddr: "unused", QueueQuery: "queue_q", KVQuery: "kv_q", QueueThreshold: 3, KVThreshold: 0.7}

	got, err := s.saturationFor(context.Background(), c)
	if err != nil {
		t.Fatalf("expected no error with TreatMissingAsError=false, got: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected saturation 0 for an absent series, got %.2f", got)
	}
}

func TestMissingSeriesErrorsWhenConfigured(t *testing.T) {
	s := &scaler{source: &fakeSource{missing: map[string]bool{"queue_q": true, "kv_q": true}}}
	c := config.Config{PromAddr: "unused", QueueQuery: "queue_q", KVQuery: "kv_q", QueueThreshold: 3, KVThreshold: 0.7, TreatMissingAsError: true}

	if _, err := s.saturationFor(context.Background(), c); err == nil {
		t.Fatal("expected an error with TreatMissingAsError=true and an absent series")
	}
}

func TestIsActive(t *testing.T) {
	t.Run("below activation threshold", func(t *testing.T) {
		s := &scaler{source: &fakeSource{values: map[string]float64{"queue_q": 0, "kv_q": 0}}}
		ref := &pb.ScaledObjectRef{
			Namespace: "ns",
			Name:      "obj",
			ScalerMetadata: map[string]string{
				"prometheusAddress":   "unused",
				"queueQuery":          "queue_q",
				"kvCacheQuery":        "kv_q",
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
		s := &scaler{source: &fakeSource{values: map[string]float64{"queue_q": 6, "kv_q": 0}}}
		ref := &pb.ScaledObjectRef{
			Namespace: "ns",
			Name:      "obj",
			ScalerMetadata: map[string]string{
				"prometheusAddress": "unused",
				"queueQuery":        "queue_q",
				"kvCacheQuery":      "kv_q",
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
		s := &scaler{}
		if _, err := s.IsActive(context.Background(), &pb.ScaledObjectRef{ScalerMetadata: map[string]string{}}); err == nil {
			t.Fatal("expected error when prometheusAddress is missing")
		}
	})

	t.Run("query error propagates", func(t *testing.T) {
		s := &scaler{source: &fakeSource{errs: map[string]error{"queue_q": errors.New("boom")}}}
		ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
			"prometheusAddress": "unused",
			"queueQuery":        "queue_q",
			"kvCacheQuery":      "kv_q",
		}}
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
	if spec.TargetSize != int64(saturation.TargetValue) {
		t.Fatalf("TargetSize = %d, want %d", spec.TargetSize, int64(saturation.TargetValue))
	}
	if spec.TargetSizeFloat != saturation.TargetValue {
		t.Fatalf("TargetSizeFloat = %v, want %v", spec.TargetSizeFloat, saturation.TargetValue)
	}
}

func TestGetMetrics(t *testing.T) {
	// queue=2.72, threshold=3 -> qScore=0.90666..., saturation=90.666...
	// (kv is 0, so it never dominates). math.Round should push the integer
	// MetricValue to 91 while the float field keeps the unrounded value.
	s := &scaler{source: &fakeSource{values: map[string]float64{
		"queue_q": 2.72,
		"kv_q":    0,
	}}}
	req := &pb.GetMetricsRequest{
		ScaledObjectRef: &pb.ScaledObjectRef{
			Namespace: "ns",
			ScalerMetadata: map[string]string{
				"prometheusAddress": "unused",
				"queueQuery":        "queue_q",
				"kvCacheQuery":      "kv_q",
				"queueThreshold":    "3",
			},
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
	wantFloat := (2.72 / 3.0) * saturation.TargetValue
	if diff := mv.MetricValueFloat - wantFloat; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("MetricValueFloat = %v, want %v", mv.MetricValueFloat, wantFloat)
	}
	if mv.MetricValue != 91 {
		t.Fatalf("MetricValue = %d, want rounded 91", mv.MetricValue)
	}

	t.Run("invalid config", func(t *testing.T) {
		s := &scaler{}
		req := &pb.GetMetricsRequest{ScaledObjectRef: &pb.ScaledObjectRef{ScalerMetadata: map[string]string{}}}
		if _, err := s.GetMetrics(context.Background(), req); err == nil {
			t.Fatal("expected error when prometheusAddress is missing")
		}
	})

	t.Run("query error propagates", func(t *testing.T) {
		s := &scaler{source: &fakeSource{errs: map[string]error{"queue_q": errors.New("boom")}}}
		req := &pb.GetMetricsRequest{ScaledObjectRef: &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
			"prometheusAddress": "unused",
			"queueQuery":        "queue_q",
			"kvCacheQuery":      "kv_q",
		}}}
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
	s := &scaler{source: &fakeSource{values: map[string]float64{"queue_q": 6, "kv_q": 0}}} // saturation 200, active
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
		"prometheusAddress":         "unused",
		"queueQuery":                "queue_q",
		"kvCacheQuery":              "kv_q",
		"streamPollIntervalSeconds": "0.005",
	}}

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
	s := &scaler{}
	// A poll interval long enough that only ctx.Done() can end the test.
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
		"prometheusAddress":         "unused",
		"streamPollIntervalSeconds": "3600",
	}}

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

func TestStreamIsActiveSkipsSendOnTransientQueryError(t *testing.T) {
	s := &scaler{source: &fakeSource{errs: map[string]error{"queue_q": errors.New("boom")}}}
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
		"prometheusAddress":            "unused",
		"queueQuery":                   "queue_q",
		"kvCacheQuery":                 "kv_q",
		"streamPollIntervalSeconds":    "0.005",
		"streamMaxConsecutiveFailures": "100", // high enough that this test's failures don't exhaust it
	}}

	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 1)}

	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	// A failing IsActive should be skipped (no send), and the stream kept
	// open, while consecutive failures stay under the configured limit --
	// give it a few tick intervals to prove nothing arrives, then cancel and
	// confirm a clean return.
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

func TestStreamIsActiveEndsStreamAfterMaxConsecutiveFailures(t *testing.T) {
	before := streamErrorsTotal.Value()

	s := &scaler{source: &fakeSource{errs: map[string]error{"queue_q": errors.New("boom")}}}
	ref := &pb.ScaledObjectRef{
		Namespace: "ns",
		Name:      "obj",
		ScalerMetadata: map[string]string{
			"prometheusAddress":            "unused",
			"queueQuery":                   "queue_q",
			"kvCacheQuery":                 "kv_q",
			"streamPollIntervalSeconds":    "0.002",
			"streamMaxConsecutiveFailures": "3",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 1)}

	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected StreamIsActive to return an error after repeated query failures")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamIsActive to give up after repeated failures")
	}

	if got := streamErrorsTotal.Value() - before; got < 3 {
		t.Fatalf("streamErrorsTotal increased by %d, want at least 3", got)
	}
}

func TestStreamIsActiveBacksOffBetweenFailures(t *testing.T) {
	s := &scaler{source: &fakeSource{errs: map[string]error{"queue_q": errors.New("boom")}}}
	interval := 2 * time.Millisecond
	maxFailures := 5
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
		"prometheusAddress":            "unused",
		"queueQuery":                   "queue_q",
		"kvCacheQuery":                 "kv_q",
		"streamPollIntervalSeconds":    "0.002",
		"streamMaxConsecutiveFailures": "5",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 1)}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error once consecutive failures reach the configured max")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamIsActive to give up")
	}
	elapsed := time.Since(start)

	// With no backoff, maxFailures failures at a flat `interval` would take
	// roughly maxFailures*interval. With doubling backoff (interval, 2x, 4x,
	// ...) it should take noticeably longer -- assert it's well above the
	// flat-rate figure as a sanity check that backoff is actually happening.
	flatRate := time.Duration(maxFailures) * interval
	if elapsed < flatRate*2 {
		t.Fatalf("elapsed %s did not noticeably exceed flat-rate retry time %s -- backoff may not be applied", elapsed, flatRate)
	}
}

func TestStreamIsActiveResetsFailureCountOnSuccess(t *testing.T) {
	// Fails on the first call, then always succeeds -- if the failure count
	// weren't reset after a success, unrelated later failures in a long-lived
	// stream could accumulate toward the limit even with successes in
	// between. Here there's only one failure total, so the stream must not
	// give up.
	calls := 0
	src := &countingSource{
		fn: func(n int) (float64, error) {
			calls++
			if n == 1 {
				return 0, errors.New("boom")
			}
			return 0, nil
		},
	}
	s := &scaler{source: src}
	ref := &pb.ScaledObjectRef{ScalerMetadata: map[string]string{
		"prometheusAddress":            "unused",
		"queueQuery":                   "queue_q",
		"kvCacheQuery":                 "kv_q",
		"streamPollIntervalSeconds":    "0.002",
		"streamMaxConsecutiveFailures": "2",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeStreamServer{ctx: ctx, sent: make(chan *pb.IsActiveResponse, 4)}

	done := make(chan error, 1)
	go func() { done <- s.StreamIsActive(ref, fs) }()

	// Wait for a couple of successful sends -- if the lone early failure
	// weren't reset, the stream would still be alive at this point too
	// (2 max failures > 1 actual), so this alone wouldn't prove much; the
	// real assertion is that the stream is still running well past what one
	// failure plus the max would allow if failures didn't reset.
	received := 0
	timeout := time.After(200 * time.Millisecond)
loop:
	for {
		select {
		case <-fs.sent:
			received++
			if received >= 3 {
				break loop
			}
		case err := <-done:
			t.Fatalf("StreamIsActive ended unexpectedly (err=%v) after %d sends", err, received)
		case <-timeout:
			t.Fatalf("timed out waiting for sends, got %d", received)
		}
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

// countingSource is a metrics.Source whose behavior is driven by fn, called
// with a 1-based count of Instant calls so far.
type countingSource struct {
	mu sync.Mutex
	n  int
	fn func(call int) (float64, error)
}

func (c *countingSource) Instant(_ context.Context, _, _ string) (float64, error) {
	c.mu.Lock()
	c.n++
	n := c.n
	c.mu.Unlock()
	return c.fn(n)
}
