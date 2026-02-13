package anomaly

import (
	"math"
	"sort"
)

// AutoDetector automatically selects the best anomaly detection algorithm
// based on data characteristics (distribution, trend, seasonality)
type AutoDetector struct{}

func init() {
	RegisterDetector("auto", &AutoDetector{})
}

// Name returns the algorithm name
func (a *AutoDetector) Name() string {
	return "auto"
}

// Detect finds anomalies by first analyzing data characteristics
// and then selecting the most appropriate algorithm
func (a *AutoDetector) Detect(data []DataPoint, config DetectorConfig) []AnomalyResult {
	if len(data) < config.MinDataPoints {
		return nil
	}

	// Analyze data characteristics
	characteristics := analyzeDataCharacteristics(data)

	// Select best algorithm based on characteristics
	selectedAlgorithm := selectAlgorithm(characteristics)

	// Get the detector and run it
	detector, err := GetDetector(selectedAlgorithm)
	if err != nil {
		// Fallback to Z-Score
		detector = &ZScoreDetector{}
	}

	return detector.Detect(data, config)
}

// DataCharacteristics describes properties of the data
type DataCharacteristics struct {
	// IsNormalDistribution indicates if data follows normal distribution
	IsNormalDistribution bool

	// HasTrend indicates if data has a clear upward/downward trend
	HasTrend bool

	// TrendStrength from -1 (strong downward) to 1 (strong upward)
	TrendStrength float64

	// HasOutliers indicates if there are existing outliers
	HasOutliers bool

	// OutlierPercentage percentage of potential outliers
	OutlierPercentage float64

	// Variability coefficient of variation (stdDev/mean)
	Variability float64

	// DataSize number of data points
	DataSize int

	// SelectedAlgorithm the algorithm that was selected
	SelectedAlgorithm string
}

// analyzeDataCharacteristics examines the data to determine its properties
func analyzeDataCharacteristics(data []DataPoint) DataCharacteristics {
	chars := DataCharacteristics{
		DataSize: len(data),
	}

	if len(data) < 3 {
		return chars
	}

	// Extract values
	values := make([]float64, len(data))
	for i, dp := range data {
		values[i] = dp.Value
	}

	// Calculate basic statistics
	mean, stdDev := CalculateMeanStdDev(values)

	// Calculate variability (coefficient of variation)
	if mean != 0 {
		chars.Variability = math.Abs(stdDev / mean)
	}

	// Check for trend using linear regression slope
	chars.HasTrend, chars.TrendStrength = detectTrend(values)

	// Check for normality using Shapiro-Wilk approximation (simplified)
	chars.IsNormalDistribution = checkNormality(values, mean, stdDev)

	// Check for outliers using IQR
	q1, q3, iqr := CalculateIQR(values)
	outlierCount := 0
	for _, v := range values {
		if v < q1-1.5*iqr || v > q3+1.5*iqr {
			outlierCount++
		}
	}
	chars.OutlierPercentage = float64(outlierCount) / float64(len(values)) * 100
	chars.HasOutliers = chars.OutlierPercentage > 1 // More than 1% outliers

	return chars
}

// selectAlgorithm chooses the best algorithm based on data characteristics
func selectAlgorithm(chars DataCharacteristics) string {
	// Decision tree for algorithm selection:
	//
	// 1. If data has many outliers (>5%), use IQR (more robust)
	// 2. If data has strong trend, use Moving Average
	// 3. If data is normally distributed, use Z-Score
	// 4. Default to IQR for robustness

	if chars.OutlierPercentage > 5 {
		return "iqr" // IQR is robust to existing outliers
	}

	if chars.HasTrend && math.Abs(chars.TrendStrength) > 0.3 {
		return "moving_avg" // Better for trending data
	}

	if chars.IsNormalDistribution {
		return "zscore" // Most effective for normal distribution
	}

	// Default to IQR for general robustness
	return "iqr"
}

// detectTrend checks if data has a trend using simple linear regression
func detectTrend(values []float64) (hasTrend bool, strength float64) {
	n := float64(len(values))
	if n < 3 {
		return false, 0
	}

	// Calculate means
	var sumX, sumY, sumXY, sumX2 float64
	for i, v := range values {
		x := float64(i)
		sumX += x
		sumY += v
		sumXY += x * v
		sumX2 += x * x
	}

	meanX := sumX / n
	meanY := sumY / n

	// Calculate slope
	denominator := sumX2 - n*meanX*meanX
	if denominator == 0 {
		return false, 0
	}

	slope := (sumXY - n*meanX*meanY) / denominator

	// Calculate R-squared for strength
	var ssTotal, ssResidual float64
	for i, v := range values {
		predicted := meanY + slope*(float64(i)-meanX)
		ssTotal += (v - meanY) * (v - meanY)
		ssResidual += (v - predicted) * (v - predicted)
	}

	var rSquared float64
	if ssTotal > 0 {
		rSquared = 1 - ssResidual/ssTotal
	}

	// Normalize strength to -1 to 1
	if slope > 0 {
		strength = rSquared
	} else {
		strength = -rSquared
	}

	// Consider it a trend if R-squared > 0.1
	hasTrend = rSquared > 0.1

	return hasTrend, strength
}

// checkNormality does a simplified normality check using skewness
func checkNormality(values []float64, mean, stdDev float64) bool {
	if len(values) < 10 || stdDev == 0 {
		return false
	}

	// Calculate skewness
	var skewSum float64
	for _, v := range values {
		z := (v - mean) / stdDev
		skewSum += z * z * z
	}
	skewness := skewSum / float64(len(values))

	// Calculate kurtosis
	var kurtSum float64
	for _, v := range values {
		z := (v - mean) / stdDev
		kurtSum += z * z * z * z
	}
	kurtosis := kurtSum/float64(len(values)) - 3 // Excess kurtosis

	// Data is approximately normal if skewness is close to 0
	// and kurtosis is close to 0 (excess kurtosis)
	isNormal := math.Abs(skewness) < 1 && math.Abs(kurtosis) < 2

	return isNormal
}

// AnalyzeData returns characteristics of the data (exported for testing)
func AnalyzeData(data []DataPoint) DataCharacteristics {
	chars := analyzeDataCharacteristics(data)
	chars.SelectedAlgorithm = selectAlgorithm(chars)
	return chars
}

// MedianAbsoluteDeviation calculates MAD, another robust measure
func MedianAbsoluteDeviation(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Calculate median
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	median := percentile(sorted, 50)

	// Calculate absolute deviations from median
	deviations := make([]float64, len(values))
	for i, v := range values {
		deviations[i] = math.Abs(v - median)
	}

	// Return median of deviations
	sort.Float64s(deviations)
	return percentile(deviations, 50)
}
