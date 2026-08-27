package config

import (
	"testing"
	"time"
)

func TestParseRequiresPromAddr(t *testing.T) {
	if _, err := Parse(map[string]string{}); err == nil {
		t.Fatal("expected error when prometheusAddress is missing")
	}
}

func TestParseAppliesDefaults(t *testing.T) {
	c, err := Parse(map[string]string{"prometheusAddress": "http://p:9090"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.QueueQuery != defaultQueueQuery {
		t.Errorf("QueueQuery = %q, want default %q", c.QueueQuery, defaultQueueQuery)
	}
	if c.KVQuery != defaultKVCacheQuery {
		t.Errorf("KVQuery = %q, want default %q", c.KVQuery, defaultKVCacheQuery)
	}
	if c.QueueThreshold != defaultQueueThreshold {
		t.Errorf("QueueThreshold = %v, want default %v", c.QueueThreshold, defaultQueueThreshold)
	}
	if c.KVThreshold != defaultKVThreshold {
		t.Errorf("KVThreshold = %v, want default %v", c.KVThreshold, defaultKVThreshold)
	}
	if c.Activation != defaultActivation {
		t.Errorf("Activation = %v, want default %v", c.Activation, defaultActivation)
	}
	if c.CacheTTL != defaultCacheTTL {
		t.Errorf("CacheTTL = %v, want default %v", c.CacheTTL, defaultCacheTTL)
	}
	if c.StreamPollInterval != defaultStreamPollInterval {
		t.Errorf("StreamPollInterval = %v, want default %v", c.StreamPollInterval, defaultStreamPollInterval)
	}
	if c.StreamMaxConsecutiveFailures != defaultStreamMaxConsecutiveFailures {
		t.Errorf("StreamMaxConsecutiveFailures = %v, want default %v", c.StreamMaxConsecutiveFailures, defaultStreamMaxConsecutiveFailures)
	}
}

func TestParseOverridesFromMetadata(t *testing.T) {
	c, err := Parse(map[string]string{
		"prometheusAddress":   "http://p:9090",
		"queueQuery":          "custom_queue",
		"kvCacheQuery":        "custom_kv",
		"queueThreshold":      "5",
		"kvCacheThreshold":    "0.9",
		"activationThreshold": "2",
		"cacheTTL":            "5s",
		"streamPollInterval":  "20s",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.QueueQuery != "custom_queue" {
		t.Errorf("QueueQuery = %q, want custom_queue", c.QueueQuery)
	}
	if c.KVQuery != "custom_kv" {
		t.Errorf("KVQuery = %q, want custom_kv", c.KVQuery)
	}
	if c.QueueThreshold != 5 {
		t.Errorf("QueueThreshold = %v, want 5", c.QueueThreshold)
	}
	if c.KVThreshold != 0.9 {
		t.Errorf("KVThreshold = %v, want 0.9", c.KVThreshold)
	}
	if c.Activation != 2 {
		t.Errorf("Activation = %v, want 2", c.Activation)
	}
	if c.CacheTTL != 5*time.Second {
		t.Errorf("CacheTTL = %v, want 5s", c.CacheTTL)
	}
	if c.StreamPollInterval != 20*time.Second {
		t.Errorf("StreamPollInterval = %v, want 20s", c.StreamPollInterval)
	}
}

func TestParseIgnoresUnparsableDuration(t *testing.T) {
	c, err := Parse(map[string]string{
		"prometheusAddress":  "http://p:9090",
		"cacheTTL":           "not-a-duration",
		"streamPollInterval": "also-not-a-duration",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.CacheTTL != defaultCacheTTL {
		t.Errorf("CacheTTL = %v, want default %v on unparsable input", c.CacheTTL, defaultCacheTTL)
	}
	if c.StreamPollInterval != defaultStreamPollInterval {
		t.Errorf("StreamPollInterval = %v, want default %v on unparsable input", c.StreamPollInterval, defaultStreamPollInterval)
	}
}

func TestParseTreatMissingAsErrorDefaultsFalse(t *testing.T) {
	c, err := Parse(map[string]string{"prometheusAddress": "http://p:9090"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.TreatMissingAsError {
		t.Fatalf("expected TreatMissingAsError to default to false, got %v", c.TreatMissingAsError)
	}
}

func TestParseTreatMissingAsErrorTrue(t *testing.T) {
	c, err := Parse(map[string]string{"prometheusAddress": "http://p:9090", "treatMissingAsError": "true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.TreatMissingAsError {
		t.Fatal("expected treatMissingAsError=true to be parsed")
	}
}

func TestParseStreamOptionsFromMetadata(t *testing.T) {
	c, err := Parse(map[string]string{
		"prometheusAddress":            "http://p:9090",
		"streamPollInterval":           "250ms",
		"streamMaxConsecutiveFailures": "3",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := 250 * time.Millisecond; c.StreamPollInterval != want {
		t.Errorf("StreamPollInterval = %v, want %v", c.StreamPollInterval, want)
	}
	if c.StreamMaxConsecutiveFailures != 3 {
		t.Errorf("StreamMaxConsecutiveFailures = %v, want 3", c.StreamMaxConsecutiveFailures)
	}
}

func TestParseStreamOptionsRejectNonPositiveValues(t *testing.T) {
	c, err := Parse(map[string]string{
		"prometheusAddress":            "http://p:9090",
		"streamPollInterval":           "0s",
		"streamMaxConsecutiveFailures": "-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.StreamPollInterval != defaultStreamPollInterval {
		t.Errorf("StreamPollInterval = %v, want default %v for a non-positive override", c.StreamPollInterval, defaultStreamPollInterval)
	}
	if c.StreamMaxConsecutiveFailures != defaultStreamMaxConsecutiveFailures {
		t.Errorf("StreamMaxConsecutiveFailures = %v, want default %v for a non-positive override", c.StreamMaxConsecutiveFailures, defaultStreamMaxConsecutiveFailures)
	}
}

func TestParseIgnoresUnparsableThreshold(t *testing.T) {
	c, err := Parse(map[string]string{"prometheusAddress": "http://p:9090", "queueThreshold": "not-a-number"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.QueueThreshold != defaultQueueThreshold {
		t.Errorf("QueueThreshold = %v, want default %v on unparsable input", c.QueueThreshold, defaultQueueThreshold)
	}
}
