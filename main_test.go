package main

import (
	"context"
	"testing"

	"github.com/kornsour/keda-inference-scaler/internal/config"
	"github.com/kornsour/keda-inference-scaler/internal/metrics"
)

// fakeSource is a metrics.Source that returns canned values per query, with no
// HTTP involved — exactly the point of depending on the Source interface. A
// query listed in missing returns metrics.ErrMissing instead of a value.
type fakeSource struct {
	values  map[string]float64
	missing map[string]bool
}

func (f *fakeSource) Instant(_ context.Context, _, query string) (float64, error) {
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

func TestScalerIsActive(t *testing.T) {
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
		Activation:     1,
	}
	sat, err := s.saturationFor(context.Background(), c)
	if err != nil {
		t.Fatalf("saturationFor: %v", err)
	}
	if active := sat > c.Activation; !active {
		t.Fatalf("expected saturation %.2f to exceed activation %.2f", sat, c.Activation)
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
