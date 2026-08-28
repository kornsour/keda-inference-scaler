// Package observability provides the scaler's self-observability surface:
// Prometheus collectors for its own behavior (as opposed to the
// vLLM/saturation metrics it queries), a readiness tracker for whether it
// can currently reach the configured Prometheus, and the HTTP server that
// exposes /healthz, /readyz, and /metrics alongside the gRPC listener.
package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the scaler's self-observability instruments, registered
// against a dedicated registry rather than the global default so that
// multiple instances (e.g. in tests) never collide on "already registered".
//
// All methods are nil-safe (a nil *Metrics silently no-ops) so callers that
// don't care about metrics -- unit tests constructing a scaler directly,
// for instance -- don't need to wire up a registry just to avoid a panic.
type Metrics struct {
	QueryDuration *prometheus.HistogramVec
	QueryErrors   *prometheus.CounterVec
	Saturation    *prometheus.GaugeVec
	SignalAge     *prometheus.GaugeVec
	GRPCRequests  *prometheus.CounterVec
	StreamErrors  prometheus.Counter

	registry *prometheus.Registry
}

// NewMetrics creates and registers the scaler's collectors on a fresh
// registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	return &Metrics{
		QueryDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scaler_prometheus_query_duration_seconds",
			Help:    "Duration of instant queries the scaler issues against Prometheus, by dimension.",
			Buckets: prometheus.DefBuckets,
		}, []string{"dimension"}),
		QueryErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "scaler_prometheus_query_errors_total",
			Help: "Count of instant queries against Prometheus that did not return a usable value, by dimension.",
		}, []string{"dimension"}),
		Saturation: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scaler_saturation",
			Help: "Last computed composite inference-saturation score, by namespace/scaledobject.",
		}, []string{"namespace", "scaledobject"}),
		SignalAge: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scaler_signal_age_seconds",
			Help: "Age of the Prometheus sample behind the last instant query, by namespace/scaledobject/dimension -- " +
				"decision time minus the sample's own Prometheus timestamp. This is the scaling control loop's " +
				"observed staleness: how old the reading a decision acted on already was, not how long the query took.",
		}, []string{"namespace", "scaledobject", "dimension"}),
		GRPCRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "scaler_grpc_requests_total",
			Help: "Count of ExternalScaler gRPC requests handled, by method.",
		}, []string{"method"}),
		StreamErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "scaler_stream_errors_total",
			Help: "Count of StreamIsActive query failures across all streams.",
		}),
		registry: reg,
	}
}

// Registry returns the registry Metrics' collectors were registered on, for
// mounting a promhttp handler.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// ObserveQueryDuration records how long an instant query for dimension
// (e.g. "queue" or "kv") took to complete, regardless of outcome.
func (m *Metrics) ObserveQueryDuration(dimension string, d time.Duration) {
	if m == nil {
		return
	}
	m.QueryDuration.WithLabelValues(dimension).Observe(d.Seconds())
}

// IncQueryError records that an instant query for dimension did not return a
// usable value -- either a transport/decode failure or an absent series.
func (m *Metrics) IncQueryError(dimension string) {
	if m == nil {
		return
	}
	m.QueryErrors.WithLabelValues(dimension).Inc()
}

// SetSaturation records the last composite saturation score computed for a
// namespace/scaledobject pair.
func (m *Metrics) SetSaturation(namespace, scaledObject string, value float64) {
	if m == nil {
		return
	}
	m.Saturation.WithLabelValues(namespace, scaledObject).Set(value)
}

// SetSignalAge records how old the Prometheus sample behind dimension's
// reading (e.g. "queue" or "kv") already was at decision time, for a
// namespace/scaledobject pair. Recorded alongside every successful instant
// query -- i.e. alongside every scaling decision -- so staleness can be
// plotted over time instead of reasoned about from the configured intervals
// alone.
func (m *Metrics) SetSignalAge(namespace, scaledObject, dimension string, age time.Duration) {
	if m == nil {
		return
	}
	m.SignalAge.WithLabelValues(namespace, scaledObject, dimension).Set(age.Seconds())
}

// IncGRPCRequest records one ExternalScaler gRPC call for method (e.g.
// "IsActive", "GetMetrics").
func (m *Metrics) IncGRPCRequest(method string) {
	if m == nil {
		return
	}
	m.GRPCRequests.WithLabelValues(method).Inc()
}

// IncStreamError records a StreamIsActive query failure. Unlike the unary
// IsActive/GetMetrics path, a broken stream produces no response message at
// all by default, so this is what makes "Prometheus has been unreachable for
// the last hour" visible to a scraper rather than only sitting in logs.
func (m *Metrics) IncStreamError() {
	if m == nil {
		return
	}
	m.StreamErrors.Inc()
}
