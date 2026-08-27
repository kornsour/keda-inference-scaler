// Package config parses KEDA ScaledObject scaler metadata into the settings the
// scaler needs: where Prometheus lives, which queries to run, and the thresholds
// that turn raw readings into a saturation score.
package config

import (
	"fmt"
	"strconv"
)

const (
	defaultQueueQuery     = "sum(vllm:num_requests_waiting)"
	defaultKVCacheQuery   = "max(vllm:gpu_cache_usage_perc)"
	defaultQueueThreshold = 3.0
	defaultKVThreshold    = 0.7
	defaultActivation     = 1.0
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
}

// Parse builds a Config from a KEDA ScaledObjectRef's ScalerMetadata map, applying
// defaults for anything not set. prometheusAddress is the only required key.
func Parse(m map[string]string) (Config, error) {
	c := Config{
		QueueQuery:     defaultQueueQuery,
		KVQuery:        defaultKVCacheQuery,
		QueueThreshold: defaultQueueThreshold,
		KVThreshold:    defaultKVThreshold,
		Activation:     defaultActivation,
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
