package main

import (
	"context"
	"testing"

	"github.com/kornsour/keda-inference-scaler/internal/config"
)

// fakeSource is a metrics.Source that returns canned values per query, with no
// HTTP involved — exactly the point of depending on the Source interface.
type fakeSource struct {
	values map[string]float64
}

func (f *fakeSource) Instant(_ context.Context, _, query string) (float64, error) {
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
