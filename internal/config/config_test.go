package config

import "testing"

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
}

func TestParseOverridesFromMetadata(t *testing.T) {
	c, err := Parse(map[string]string{
		"prometheusAddress":   "http://p:9090",
		"queueQuery":          "custom_queue",
		"kvCacheQuery":        "custom_kv",
		"queueThreshold":      "5",
		"kvCacheThreshold":    "0.9",
		"activationThreshold": "2",
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

func TestParseIgnoresUnparsableThreshold(t *testing.T) {
	c, err := Parse(map[string]string{"prometheusAddress": "http://p:9090", "queueThreshold": "not-a-number"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.QueueThreshold != defaultQueueThreshold {
		t.Errorf("QueueThreshold = %v, want default %v on unparsable input", c.QueueThreshold, defaultQueueThreshold)
	}
}
