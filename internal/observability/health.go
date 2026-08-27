package observability

import (
	"sync"
	"time"
)

// Health tracks whether the scaler has recently been able to reach its
// configured Prometheus. It backs /readyz: a scaler that cannot see its
// data source should not be marked ready, even though its process (and
// therefore /healthz) is perfectly fine.
//
// There's one Health per process, shared across every ScaledObject the
// scaler serves -- a success on any of them counts, since they typically
// share a Prometheus. A nil *Health is treated as always ready, so callers
// that don't care about readiness (unit tests constructing a scaler
// directly) don't need to wire one up.
type Health struct {
	window    time.Duration
	startedAt time.Time

	mu     sync.Mutex
	lastOK time.Time
	hasOK  bool
}

// NewHealth creates a Health that considers the scaler ready as long as a
// query has succeeded within the last window -- or, before any query has
// ever been attempted, as long as the process itself is younger than
// window, so a freshly started pod isn't marked unready before KEDA has had
// a chance to call it even once.
func NewHealth(window time.Duration) *Health {
	return &Health{window: window, startedAt: time.Now()}
}

// RecordSuccess marks that the configured Prometheus answered successfully
// just now. "Successfully" means it was reached and returned a well-formed
// result -- including a result with no series, since that's Prometheus
// answering, just with nothing to report -- as opposed to a transport
// failure, a non-200 response, or an undecodable body.
func (h *Health) RecordSuccess() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastOK = time.Now()
	h.hasOK = true
}

// Ready reports whether the configured Prometheus has answered successfully
// within the configured window.
func (h *Health) Ready() bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.hasOK {
		return time.Since(h.startedAt) < h.window
	}
	return time.Since(h.lastOK) < h.window
}
