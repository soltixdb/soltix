package forecast

import (
	"math"
)

// AutoForecaster automatically selects the best forecasting algorithm
type AutoForecaster struct{}

// NewAutoForecaster creates a new Auto forecaster
func NewAutoForecaster() *AutoForecaster {
	return &AutoForecaster{}
}

func init() {
	RegisterForecaster("auto", NewAutoForecaster())
}

// Name returns the algorithm name
func (f *AutoForecaster) Name() string {
	return "auto"
}

// Forecast automatically selects the best algorithm based on data characteristics
func (f *AutoForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		// Use SMA for small datasets
		result, err := NewSMAForecaster().Forecast(data, config)
		if err != nil {
			return nil, err
		}
		result.ModelInfo.Algorithm = "sma (auto-selected)"
		return result, nil
	}

	// Analyze data characteristics
	hasTrend := detectTrend(data)
	hasSeasonality := detectSeasonality(data, config.SeasonalPeriod)

	var selectedForecaster Forecaster
	var selectedName string

	// Select algorithm based on data characteristics
	if hasSeasonality && len(data) >= config.SeasonalPeriod*2 {
		selectedForecaster = NewHoltWintersForecaster()
		selectedName = "holt_winters"
	} else if hasTrend {
		selectedForecaster = NewLinearRegressionForecaster()
		selectedName = "linear"
	} else if len(data) >= 20 {
		selectedForecaster = NewExponentialSmoothingForecaster()
		selectedName = "exponential"
	} else {
		selectedForecaster = NewSMAForecaster()
		selectedName = "sma"
	}

	result, err := selectedForecaster.Forecast(data, config)
	if err != nil {
		return nil, err
	}

	result.ModelInfo.Algorithm = selectedName + " (auto-selected)"
	return result, nil
}

// detectTrend detects if data has a significant trend
func detectTrend(data []DataPoint) bool {
	if len(data) < 5 {
		return false
	}

	// Simple linear regression to detect trend
	n := float64(len(data))
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0
	sumY2 := 0.0

	for i, p := range data {
		x := float64(i)
		sumX += x
		sumY += p.Value
		sumXY += x * p.Value
		sumX2 += x * x
		sumY2 += p.Value * p.Value
	}

	// Calculate correlation coefficient
	numerator := n*sumXY - sumX*sumY
	denominator := math.Sqrt((n*sumX2 - sumX*sumX) * (n*sumY2 - sumY*sumY))

	if denominator == 0 {
		return false
	}

	r := numerator / denominator

	// Consider trend significant if |r| > 0.5
	return math.Abs(r) > 0.5
}

// detectSeasonality detects if data has seasonal patterns
func detectSeasonality(data []DataPoint, period int) bool {
	if len(data) < period*2 || period <= 1 {
		return false
	}

	// Simple autocorrelation check at the seasonal period
	n := len(data)
	mean := 0.0
	for _, p := range data {
		mean += p.Value
	}
	mean /= float64(n)

	// Calculate autocorrelation at lag = period
	numerator := 0.0
	denominator := 0.0
	for i := period; i < n; i++ {
		a := data[i].Value - mean
		b := data[i-period].Value - mean
		numerator += a * b
		denominator += a * a
	}

	if denominator == 0 {
		return false
	}

	autocorr := numerator / denominator

	// Consider seasonal if autocorrelation at period > 0.5
	return autocorr > 0.5
}
