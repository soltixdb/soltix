package anomaly

import (
	"math"
)

// MovingAverageDetector detects anomalies by comparing each point
// to its local moving average. Good for detecting sudden changes in trending data.
type MovingAverageDetector struct{}

func init() {
	RegisterDetector("moving_avg", &MovingAverageDetector{})
}

// Name returns the algorithm name
func (ma *MovingAverageDetector) Name() string {
	return "moving_avg"
}

// Detect finds anomalies using moving average method
func (ma *MovingAverageDetector) Detect(data []DataPoint, config DetectorConfig) []AnomalyResult {
	if len(data) < config.MinDataPoints {
		return nil
	}

	windowSize := config.WindowSize
	if windowSize <= 0 {
		windowSize = 10 // Default window size
	}
	if windowSize > len(data) {
		windowSize = len(data) / 2
	}
	if windowSize < 3 {
		windowSize = 3
	}

	// Calculate moving averages and standard deviations
	movingAvgs := make([]float64, len(data))
	movingStdDevs := make([]float64, len(data))

	for i := range data {
		start := i - windowSize/2
		end := i + windowSize/2

		if start < 0 {
			start = 0
		}
		if end >= len(data) {
			end = len(data) - 1
		}

		// Calculate mean for window
		var sum float64
		count := 0
		for j := start; j <= end; j++ {
			if j != i { // Exclude current point
				sum += data[j].Value
				count++
			}
		}

		if count == 0 {
			movingAvgs[i] = data[i].Value
			movingStdDevs[i] = 0
			continue
		}

		mean := sum / float64(count)
		movingAvgs[i] = mean

		// Calculate standard deviation for window
		var varianceSum float64
		for j := start; j <= end; j++ {
			if j != i {
				diff := data[j].Value - mean
				varianceSum += diff * diff
			}
		}
		movingStdDevs[i] = math.Sqrt(varianceSum / float64(count))
	}

	var results []AnomalyResult

	for i, dp := range data {
		localMean := movingAvgs[i]
		localStdDev := movingStdDevs[i]

		// Calculate deviation from local average
		var deviation float64
		if localStdDev > 0 {
			deviation = math.Abs(dp.Value-localMean) / localStdDev
		} else {
			// If no variation in window, any difference is significant
			if dp.Value != localMean {
				deviation = config.Threshold + 1
			}
		}

		if deviation > config.Threshold {
			var anomalyType AnomalyType
			if dp.Value > localMean {
				anomalyType = AnomalyTypeSpike
			} else {
				anomalyType = AnomalyTypeDrop
			}

			expectedRange := &Range{
				Min: localMean - config.Threshold*localStdDev,
				Max: localMean + config.Threshold*localStdDev,
			}

			results = append(results, AnomalyResult{
				Index:    i,
				Score:    deviation,
				Type:     anomalyType,
				Expected: expectedRange,
			})
		}
	}

	return results
}

// CalculateMovingAverage calculates moving average for a slice of values
func CalculateMovingAverage(values []float64, windowSize int) []float64 {
	if len(values) == 0 {
		return nil
	}

	result := make([]float64, len(values))

	for i := range values {
		start := i - windowSize/2
		end := i + windowSize/2

		if start < 0 {
			start = 0
		}
		if end >= len(values) {
			end = len(values) - 1
		}

		var sum float64
		count := 0
		for j := start; j <= end; j++ {
			sum += values[j]
			count++
		}
		result[i] = sum / float64(count)
	}

	return result
}

// ExponentialMovingAverage calculates EMA for a slice of values
func ExponentialMovingAverage(values []float64, alpha float64) []float64 {
	if len(values) == 0 {
		return nil
	}

	result := make([]float64, len(values))
	result[0] = values[0]

	for i := 1; i < len(values); i++ {
		result[i] = alpha*values[i] + (1-alpha)*result[i-1]
	}

	return result
}
