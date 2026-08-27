// Package selfmetrics exposes the scaler's own operational counters — things
// like "how many StreamIsActive query failures has this process seen" — as a
// tiny Prometheus text-exposition endpoint. This is deliberately separate
// from internal/metrics, which is the scaler's client for querying an
// external Prometheus about inference saturation; selfmetrics is instead
// what an external Prometheus (or any scraper) can query about the scaler
// itself, so a failure mode such as "the query backend has been unreachable
// for the last hour" is visible in metrics and alertable, not just sitting in
// logs.
package selfmetrics

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// Counter is a threadsafe, monotonically increasing counter.
type Counter struct {
	name string
	help string
	v    atomic.Int64
}

// Inc increments the counter by one.
func (c *Counter) Inc() { c.v.Add(1) }

// Value returns the counter's current value.
func (c *Counter) Value() int64 { return c.v.Load() }

// Registry collects Counters and serves them in Prometheus text exposition
// format from a single HTTP handler.
type Registry struct {
	mu       sync.Mutex
	counters []*Counter
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// NewCounter creates a Counter, registers it, and returns it. name should be
// a valid Prometheus metric name (e.g. "keda_inference_scaler_foo_total");
// help is the one-line description shown alongside it.
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
	return c
}

// Handler returns an http.Handler that serves every counter registered on r
// in Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.writeTo(w)
	})
}

// writeTo writes every counter registered on r to w in Prometheus text
// exposition format.
func (r *Registry) writeTo(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.counters {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", c.name, c.help, c.name, c.name, c.Value())
	}
}
