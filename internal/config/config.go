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

	// defaultStreamPollInterval is StreamIsActive's poll period when
	// streamPollIntervalSeconds isn't set. This is the interval used between
	// successful polls; on failures the scaler backs off beyond it.
	defaultStreamPollInterval = 10 * time.Second

	// defaultStreamMaxConsecutiveFailures is how many consecutive
	// StreamIsActive query failures are tolerated before the stream gives up
	// and returns an error, letting KEDA re-establish it, when
	// streamMaxConsecutiveFailures isn't set.
	defaultStreamMaxConsecutiveFailures = 5
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

	// StreamPollInterval is how often StreamIsActive polls while queries are
	// succeeding. It is independent of KEDA's own ScaledObject-level
	// pollingInterval, which governs the IsActive/GetMetrics fallback path.
	StreamPollInterval time.Duration

	// StreamMaxConsecutiveFailures is how many consecutive query failures
	// StreamIsActive tolerates, backing off between them, before it returns
	// an error and ends the stream rather than holding it open indefinitely.
	StreamMaxConsecutiveFailures int
}

// Parse builds a Config from a KEDA ScaledObjectRef's ScalerMetadata map, applying
// defaults for anything not set. prometheusAddress is the only required key.
func Parse(m map[string]string) (Config, error) {
	c := Config{
		QueueQuery:                   defaultQueueQuery,
		KVQuery:                      defaultKVCacheQuery,
		QueueThreshold:               defaultQueueThreshold,
		KVThreshold:                  defaultKVThreshold,
		Activation:                   defaultActivation,
		StreamPollInterval:           defaultStreamPollInterval,
		StreamMaxConsecutiveFailures: defaultStreamMaxConsecutiveFailures,
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
	c.StreamPollInterval = durationSecondsOr(m["streamPollIntervalSeconds"], c.StreamPollInterval)
	c.StreamMaxConsecutiveFailures = positiveIntOr(m["streamMaxConsecutiveFailures"], c.StreamMaxConsecutiveFailures)
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

// durationSecondsOr parses s as a number of seconds (fractional seconds
// allowed, e.g. "0.5"). Empty, unparsable, or non-positive input falls back
// to def — a zero or negative interval would spin the stream loop tightly.
func durationSecondsOr(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		return def
	}
	return time.Duration(f * float64(time.Second))
}

// positiveIntOr parses s as an integer. Empty, unparsable, or non-positive
// input falls back to def.
func positiveIntOr(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return def
	}
	return n
}
