package anomaly

import (
	"sort"
)

// IQRDetector detects anomalies using Interquartile Range (IQR) method
// IQR is robust to outliers compared to Z-Score
// Anomalies are points outside [Q1 - k*IQR, Q3 + k*IQR] where k is typically 1.5
type IQRDetector struct{}

func init() {
	RegisterDetector("iqr", &IQRDetector{})
}

// Name returns the algorithm name
func (iqr *IQRDetector) Name() string {
	return "iqr"
}

// Detect finds anomalies using IQR method
func (iqr *IQRDetector) Detect(data []DataPoint, config DetectorConfig) []AnomalyResult {
	if len(data) < config.MinDataPoints {
		return nil
	}

	// Extract values and sort them
	values := make([]float64, len(data))
	for i, dp := range data {
		values[i] = dp.Value
	}
	sortedValues := make([]float64, len(values))
	copy(sortedValues, values)
	sort.Float64s(sortedValues)

	// Calculate quartiles
	q1 := percentile(sortedValues, 25)
	q3 := percentile(sortedValues, 75)
	iqrValue := q3 - q1

	// Use threshold as IQR multiplier (default 1.5, but configurable)
	// If threshold is > 2, treat it as Z-score style and convert
	multiplier := config.Threshold
	if multiplier >= 3 {
		multiplier = 1.5 // Use standard IQR multiplier for high thresholds
	}

	// Calculate bounds
	lowerBound := q1 - multiplier*iqrValue
	upperBound := q3 + multiplier*iqrValue

	expectedRange := &Range{
		Min: lowerBound,
		Max: upperBound,
	}

	var results []AnomalyResult

	for i, dp := range data {
		if dp.Value < lowerBound || dp.Value > upperBound {
			// Calculate score based on how far outside the bounds
			var score float64
			if iqrValue > 0 {
				if dp.Value < lowerBound {
					score = (lowerBound - dp.Value) / iqrValue
				} else {
					score = (dp.Value - upperBound) / iqrValue
				}
			} else {
				score = 1.0
			}

			anomalyType := AnomalyTypeOutlier
			if dp.Value > upperBound {
				anomalyType = AnomalyTypeSpike
			} else if dp.Value < lowerBound {
				anomalyType = AnomalyTypeDrop
			}

			results = append(results, AnomalyResult{
				Index:    i,
				Score:    score,
				Type:     anomalyType,
				Expected: expectedRange,
			})
		}
	}

	return results
}

// percentile calculates the p-th percentile of sorted data
// p should be between 0 and 100
func percentile(sortedData []float64, p float64) float64 {
	if len(sortedData) == 0 {
		return 0
	}
	if len(sortedData) == 1 {
		return sortedData[0]
	}

	// Calculate the index
	index := (p / 100) * float64(len(sortedData)-1)
	lower := int(index)
	upper := lower + 1

	if upper >= len(sortedData) {
		return sortedData[len(sortedData)-1]
	}

	// Linear interpolation
	weight := index - float64(lower)
	return sortedData[lower]*(1-weight) + sortedData[upper]*weight
}

// CalculateIQR returns Q1, Q3, and IQR for a slice of values
func CalculateIQR(values []float64) (q1, q3, iqr float64) {
	if len(values) == 0 {
		return 0, 0, 0
	}

	sortedValues := make([]float64, len(values))
	copy(sortedValues, values)
	sort.Float64s(sortedValues)

	q1 = percentile(sortedValues, 25)
	q3 = percentile(sortedValues, 75)
	iqr = q3 - q1

	return q1, q3, iqr
}
