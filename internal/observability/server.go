package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewServer builds the scaler's self-observability HTTP server: process
// liveness, data-source readiness, and Prometheus exposition. It listens on
// addr, separate from the gRPC port, so Kubernetes probes and scraping
// don't need an HTTP/2 gRPC client.
func NewServer(addr string, m *Metrics, h *Health) *http.Server {
	mux := http.NewServeMux()

	// /healthz: the process is up and serving HTTP. It does not depend on
	// Prometheus -- that's what /readyz is for -- so it only ever reflects
	// "the scaler hasn't wedged or crashed".
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// /readyz: additionally requires that the configured Prometheus has
	// answered successfully recently. A scaler that can't see its data
	// source shouldn't be marked ready, even if the process itself is fine.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.Ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("prometheus has not answered successfully within the readiness window\n"))
	})

	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
