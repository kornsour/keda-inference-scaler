// Package config parses KEDA ScaledObject scaler metadata into the settings the
// scaler needs: where Prometheus lives, which queries to run, and the thresholds
// that turn raw readings into a saturation score.
package config

import (
	"fmt"
	"strconv"
	"time"
)

const (
	defaultQueueQuery     = "sum(vllm:num_requests_waiting)"
	defaultKVCacheQuery   = "max(vllm:gpu_cache_usage_perc)"
	defaultQueueThreshold = 3.0
	defaultKVThreshold    = 0.7
	defaultActivation     = 1.0

	// defaultCacheTTL bounds how long a Prometheus reading is reused before
	// the backend is queried again. Prometheus only refreshes a series once
	// per scrape interval (typically 15-30s); querying faster than that just
	// repeats the same sample at full network cost, so the default sits at
	// the low end of that range rather than assuming the higher one.
	defaultCacheTTL = 15 * time.Second

	// defaultStreamPollInterval is how often StreamIsActive re-evaluates and
	// pushes a result, matching the scaler's original hardcoded ticker.
	defaultStreamPollInterval = 10 * time.Second
)

// Config holds one ScaledObject's resolved scaler metadata.
type Config struct {
	PromAddr       string
	QueueQuery     string
	KVQuery        string
	QueueThreshold float64
	KVThreshold    float64
	Activation     float64

	// TreatMissingAsError makes an absent metric series surface as an error
	// instead of being read as idle (0). This matters because "absent" and
	// "idle" are otherwise indistinguishable: a dropped PodMonitor or a
	// relabel change looks exactly like no traffic.
	TreatMissingAsError bool

	// CacheTTL bounds how long a (prometheusAddress, query) reading is served
	// from cache before the backend is queried again. <= 0 disables caching
	// for this ScaledObject: every call reaches the backend, though
	// concurrent identical calls still collapse into one upstream request.
	CacheTTL time.Duration

	// StreamPollInterval is how often StreamIsActive re-evaluates IsActive
	// and pushes a result on its stream.
	StreamPollInterval time.Duration
}

// Parse builds a Config from a KEDA ScaledObjectRef's ScalerMetadata map, applying
// defaults for anything not set. prometheusAddress is the only required key.
func Parse(m map[string]string) (Config, error) {
	c := Config{
		QueueQuery:         defaultQueueQuery,
		KVQuery:            defaultKVCacheQuery,
		QueueThreshold:     defaultQueueThreshold,
		KVThreshold:        defaultKVThreshold,
		Activation:         defaultActivation,
		CacheTTL:           defaultCacheTTL,
		StreamPollInterval: defaultStreamPollInterval,
	}
	c.PromAddr = m["prometheusAddress"]
	if c.PromAddr == "" {
		return c, fmt.Errorf("scaler metadata: prometheusAddress is required")
	}
	if v := m["queueQuery"]; v != "" {
		c.QueueQuery = v
	}
	if v := m["kvCacheQuery"]; v != "" {
		c.KVQuery = v
	}
	c.QueueThreshold = floatOr(m["queueThreshold"], c.QueueThreshold)
	c.KVThreshold = floatOr(m["kvCacheThreshold"], c.KVThreshold)
	c.Activation = floatOr(m["activationThreshold"], c.Activation)
	c.TreatMissingAsError = boolOr(m["treatMissingAsError"], false)
	c.CacheTTL = durationOr(m["cacheTTL"], c.CacheTTL)
	c.StreamPollInterval = durationOr(m["streamPollInterval"], c.StreamPollInterval)
	return c, nil
}

func floatOr(s string, def float64) float64 {
	if s == "" {
		return def
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return def
}

func boolOr(s string, def bool) bool {
	if s == "" {
		return def
	}
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	return def
}

// durationOr parses s as a Go duration string (e.g. "15s"); an empty or
// unparsable s falls back to def.
func durationOr(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}
