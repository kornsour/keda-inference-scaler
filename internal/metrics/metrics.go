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
	"time"
)

// ErrMissing indicates an instant query returned no series at all, or a series
// whose value wasn't decodable — i.e. the metric is absent from the backend
// right now, as opposed to present with a real numeric value (including 0).
// Callers decide whether that counts as "idle" or as an error.
var ErrMissing = errors.New("metric series absent from prometheus result")

// Sample is one instant-query result: the scalar value together with the
// wall-clock time the backend recorded that value at (Prometheus's own
// sample timestamp), not the time the query returned it. That timestamp is
// the raw input to measuring signal staleness -- how old was the reading a
// scaling decision actually acted on, as opposed to how long the query took.
type Sample struct {
	Value float64
	Time  time.Time
}

// Source runs an instant query against a metric backend at addr and returns
// the first scalar value along with the sample's own timestamp. If the
// series is absent, it returns ErrMissing.
type Source interface {
	Instant(ctx context.Context, addr, query string) (Sample, error)
}

// Prometheus is a Source backed by a Prometheus (or Prometheus-compatible) HTTP
// API's instant query endpoint.
type Prometheus struct {
	HTTP *http.Client
}

// Instant runs an instant query and returns the first scalar value together
// with the sample's own timestamp. If the result set is empty or the value
// isn't decodable, it returns ErrMissing (e.g. the metric hasn't appeared
// yet, or its series was dropped/renamed).
func (p *Prometheus) Instant(ctx context.Context, addr, query string) (Sample, error) {
	u := fmt.Sprintf("%s/api/v1/query?query=%s", addr, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Sample{}, err
	}
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return Sample{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Sample{}, fmt.Errorf("prometheus %s returned %d", addr, resp.StatusCode)
	}
	var out struct {
		Data struct {
			Result []struct {
				// Value is Prometheus's [timestamp, value] pair: a JSON
				// number (seconds since the epoch, fractional) for the
				// sample time, and a JSON string for the sample value.
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Sample{}, err
	}
	if len(out.Data.Result) == 0 {
		return Sample{}, ErrMissing
	}
	str, ok := out.Data.Result[0].Value[1].(string)
	if !ok {
		return Sample{}, ErrMissing
	}
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return Sample{}, err
	}
	// A malformed or missing timestamp still leaves the value usable, but
	// staleness can't be computed for it -- fall back to "now" (age 0)
	// rather than failing the whole query over an ancillary field.
	ts := time.Now()
	if secs, ok := out.Data.Result[0].Value[0].(float64); ok {
		ts = time.Unix(0, int64(secs*float64(time.Second)))
	}
	return Sample{Value: v, Time: ts}, nil
}
