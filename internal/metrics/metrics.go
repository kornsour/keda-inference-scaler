// Package metrics provides the scaler's view of a metric backend: a single
// "instant query" operation returning one scalar. The Prometheus implementation
// is the only one today, but callers depend on the Source interface so a fake
// implementation can stand in for tests, and additional backends can be added
// without touching the gRPC glue.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// ErrMissing indicates an instant query returned no series at all, or a series
// whose value wasn't decodable — i.e. the metric is absent from the backend
// right now, as opposed to present with a real numeric value (including 0).
// Callers decide whether that counts as "idle" or as an error.
var ErrMissing = errors.New("metric series absent from prometheus result")

// Source runs an instant query against a metric backend at addr and returns the
// first scalar value. If the series is absent, it returns ErrMissing.
type Source interface {
	Instant(ctx context.Context, addr, query string) (float64, error)
}

// Prometheus is a Source backed by a Prometheus (or Prometheus-compatible) HTTP
// API's instant query endpoint.
type Prometheus struct {
	HTTP *http.Client
}

// Instant runs an instant query and returns the first scalar value. If the
// result set is empty or the value isn't decodable, it returns ErrMissing
// (e.g. the metric hasn't appeared yet, or its series was dropped/renamed).
func (p *Prometheus) Instant(ctx context.Context, addr, query string) (float64, error) {
	u := fmt.Sprintf("%s/api/v1/query?query=%s", addr, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus %s returned %d", addr, resp.StatusCode)
	}
	var out struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if len(out.Data.Result) == 0 {
		return 0, ErrMissing
	}
	str, ok := out.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, ErrMissing
	}
	return strconv.ParseFloat(str, 64)
}
