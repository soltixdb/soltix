package anomaly

import (
	"math"
)

// ZScoreDetector detects anomalies using Z-Score (standard score)
// Z-Score measures how many standard deviations a point is from the mean
// Points with |Z| > threshold are considered anomalies
type ZScoreDetector struct{}

func init() {
	RegisterDetector("zscore", &ZScoreDetector{})
}

// Name returns the algorithm name
func (z *ZScoreDetector) Name() string {
	return "zscore"
}

// Detect finds anomalies using Z-Score method
func (z *ZScoreDetector) Detect(data []DataPoint, config DetectorConfig) []AnomalyResult {
	if len(data) < config.MinDataPoints {
		return nil
	}

	// Calculate mean
	var sum float64
	for _, dp := range data {
		sum += dp.Value
	}
	mean := sum / float64(len(data))

	// Calculate standard deviation
	var varianceSum float64
	for _, dp := range data {
		diff := dp.Value - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(len(data)))

	// Avoid division by zero
	if stdDev == 0 {
		return z.detectFlatline(data, config)
	}

	// Calculate expected range
	expectedRange := &Range{
		Min: mean - config.Threshold*stdDev,
		Max: mean + config.Threshold*stdDev,
	}

	var results []AnomalyResult

	for i, dp := range data {
		zScore := (dp.Value - mean) / stdDev

		if math.Abs(zScore) > config.Threshold {
			var anomalyType AnomalyType
			if zScore > 0 {
				anomalyType = AnomalyTypeSpike
			} else {
				anomalyType = AnomalyTypeDrop
			}

			results = append(results, AnomalyResult{
				Index:    i,
				Score:    math.Abs(zScore),
				Type:     anomalyType,
				Expected: expectedRange,
			})
		}
	}

	return results
}

// detectFlatline checks if data has no variation (potential sensor failure)
func (z *ZScoreDetector) detectFlatline(data []DataPoint, config DetectorConfig) []AnomalyResult {
	if len(data) < config.MinDataPoints {
		return nil
	}

	// Check if all values are the same
	firstValue := data[0].Value
	allSame := true
	for _, dp := range data[1:] {
		if dp.Value != firstValue {
			allSame = false
			break
		}
	}

	if allSame && len(data) >= config.MinDataPoints {
		// Return all points as flatline anomalies
		results := make([]AnomalyResult, len(data))
		for i := range data {
			results[i] = AnomalyResult{
				Index:    i,
				Score:    1.0, // Certainty score
				Type:     AnomalyTypeFlatline,
				Expected: nil,
			}
		}
		return results
	}

	return nil
}

// CalculateZScore calculates Z-Score for a single value given mean and stdDev
func CalculateZScore(value, mean, stdDev float64) float64 {
	if stdDev == 0 {
		return 0
	}
	return (value - mean) / stdDev
}

// CalculateMeanStdDev calculates mean and standard deviation for a slice of values
func CalculateMeanStdDev(values []float64) (mean, stdDev float64) {
	if len(values) == 0 {
		return 0, 0
	}

	// Calculate mean
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))

	// Calculate standard deviation
	var varianceSum float64
	for _, v := range values {
		diff := v - mean
		varianceSum += diff * diff
	}
	stdDev = math.Sqrt(varianceSum / float64(len(values)))

	return mean, stdDev
}
