// Package saturation implements the scaler's composite signal: the greater of
// two normalized readings, expressed as a percentage of "at threshold". It is
// pure arithmetic with no I/O, so it is testable without a metric backend.
package saturation

import "math"

// TargetValue is the value KEDA keeps this metric at: 100 means "exactly at
// threshold", above 100 means "scale out".
const TargetValue = 100.0

// Score returns max(queue/queueThreshold, kv/kvThreshold) * TargetValue. A
// threshold that is zero or negative is treated as "this dimension does not
// limit scaling" and contributes a score of 0 rather than dividing by it.
func Score(queue, queueThreshold, kv, kvThreshold float64) float64 {
	var qScore, kvScore float64
	if queueThreshold > 0 {
		qScore = queue / queueThreshold
	}
	if kvThreshold > 0 {
		kvScore = kv / kvThreshold
	}
	return math.Max(qScore, kvScore) * TargetValue
}
