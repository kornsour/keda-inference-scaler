// Command fakeprom is a minimal stand-in for both vLLM and Prometheus, used
// only by the kind-based end-to-end smoke test (see test/e2e/run.sh). It
// implements just enough of Prometheus's HTTP API — GET /api/v1/query — for
// keda-inference-scaler's internal/metrics.Prometheus client to work against
// it. The value returned depends on which of the scaler's two default queries
// is asked for, so the e2e run exercises the real composite-saturation path
// end to end instead of a single canned number:
//
//   - a query containing "num_requests_waiting" (the queue-depth query) answers
//     with QUEUE_VALUE (default 9 — well above the default threshold of 3, so
//     the scaler reports the ScaledObject active and the saturation metric
//     comfortably above 100).
//   - a query containing "gpu_cache_usage_perc" (the KV-cache query) answers
//     with KV_VALUE (default 0.1 — below its default threshold of 0.7, so the
//     queue dimension is the one driving saturation, exactly like a
//     compute-bound small model in the real system).
//   - any other query answers with an empty result set, matching how real
//     Prometheus responds when a series doesn't exist.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func envFloat(name string, def float64) float64 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func main() {
	queueValue := envFloat("QUEUE_VALUE", 9)
	kvValue := envFloat("KV_VALUE", 0.1)
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	http.HandleFunc("/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		var (
			value float64
			found bool
		)
		switch {
		case strings.Contains(query, "num_requests_waiting"):
			value, found = queueValue, true
		case strings.Contains(query, "gpu_cache_usage_perc"):
			value, found = kvValue, true
		}

		w.Header().Set("Content-Type", "application/json")
		result := []any{}
		if found {
			result = []any{
				map[string]any{"metric": map[string]any{}, "value": []any{0, strconv.FormatFloat(value, 'f', -1, 64)}},
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result":     result,
			},
		})
		log.Printf("query=%q found=%v value=%v", query, found, value)
	})

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("fakeprom listening on %s (queue=%v kv=%v)", addr, queueValue, kvValue)
	log.Fatal(http.ListenAndServe(addr, nil))
}
