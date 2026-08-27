// Package metrics provides the scaler's view of a metric backend: a single
// "instant query" operation returning one scalar. The Prometheus implementation
// is the only one today, but callers depend on the Source interface so a fake
// implementation can stand in for tests, and additional backends can be added
// without touching the gRPC glue.
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// Source runs an instant query against a metric backend at addr and returns the
// first scalar value.
type Source interface {
	Instant(ctx context.Context, addr, query string) (float64, error)
}

// Prometheus is a Source backed by a Prometheus (or Prometheus-compatible) HTTP
// API's instant query endpoint.
type Prometheus struct {
	HTTP *http.Client
}

// Instant runs an instant query and returns the first scalar value (0 if the
// result set is empty — e.g. the metric hasn't appeared yet).
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
		return 0, nil
	}
	str, ok := out.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, nil
	}
	return strconv.ParseFloat(str, 64)
}
