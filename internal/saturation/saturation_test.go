package saturation

import (
	"math"
	"testing"
)

func TestScoreQueueDominates(t *testing.T) {
	// queue=6/3=2.0, kv=0.35/0.7=0.5 -> max is queue -> 200.
	got := Score(6, 3, 0.35, 0.7)
	if got != 200 {
		t.Fatalf("got %.2f, want 200", got)
	}
}

func TestScoreKVDominates(t *testing.T) {
	// queue=1/3=0.33, kv=0.63/0.7=0.9 -> max is kv -> 90.
	got := Score(1, 3, 0.63, 0.7)
	if want := 90.0; math.Abs(got-want) > 0.01 {
		t.Fatalf("got %.4f, want ~%.2f", got, want)
	}
}

func TestScoreAtThresholdIsTargetValue(t *testing.T) {
	got := Score(3, 3, 0, 0.7)
	if got != TargetValue {
		t.Fatalf("got %.2f, want %.2f", got, TargetValue)
	}
}

func TestScoreNonPositiveThresholdIsIgnored(t *testing.T) {
	// Both thresholds non-positive -> both dimensions contribute 0.
	got := Score(100, 0, 100, -1)
	if got != 0 {
		t.Fatalf("got %.2f, want 0", got)
	}
}

func TestScoreZeroReadingsIsZero(t *testing.T) {
	got := Score(0, 3, 0, 0.7)
	if got != 0 {
		t.Fatalf("got %.2f, want 0", got)
	}
}
