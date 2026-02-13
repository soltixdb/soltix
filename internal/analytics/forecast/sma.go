package forecast

import (
	"fmt"
	"time"
)

// SMAForecaster implements Simple Moving Average forecasting
type SMAForecaster struct{}

// NewSMAForecaster creates a new SMA forecaster
func NewSMAForecaster() *SMAForecaster {
	return &SMAForecaster{}
}

func init() {
	RegisterForecaster("sma", NewSMAForecaster())
}

// Name returns the algorithm name
func (f *SMAForecaster) Name() string {
	return "sma"
}

// Forecast generates predictions using Simple Moving Average
func (f *SMAForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: need %d, have %d", config.MinDataPoints, len(data))
	}

	windowSize := config.WindowSize
	if windowSize <= 0 {
		windowSize = 7
	}
	if windowSize > len(data) {
		windowSize = len(data)
	}

	// Calculate fitted values using moving average
	fitted := make([]float64, len(data))
	for i := 0; i < len(data); i++ {
		start := i - windowSize + 1
		if start < 0 {
			start = 0
		}
		sum := 0.0
		for j := start; j <= i; j++ {
			sum += data[j].Value
		}
		fitted[i] = sum / float64(i-start+1)
	}

	// Calculate residuals and standard error
	residuals := make([]float64, len(data))
	actual := make([]float64, len(data))
	sumSquaredError := 0.0
	for i := range data {
		actual[i] = data[i].Value
		residuals[i] = data[i].Value - fitted[i]
		sumSquaredError += residuals[i] * residuals[i]
	}
	stdError := 0.0
	if len(data) > 1 {
		stdError = calculateStdError(sumSquaredError, len(data))
	}

	// Calculate moving average of last window for forecasting
	lastWindow := data[len(data)-windowSize:]
	sum := 0.0
	for _, p := range lastWindow {
		sum += p.Value
	}
	forecastValue := sum / float64(len(lastWindow))

	// Generate predictions
	predictions := make([]ForecastPoint, config.Horizon)
	lastTime := data[len(data)-1].Time
	interval := config.Interval
	if interval == 0 && len(data) >= 2 {
		interval = data[1].Time.Sub(data[0].Time)
	}

	for i := 0; i < config.Horizon; i++ {
		predTime := lastTime.Add(interval * time.Duration(1+i))
		lower, upper := calculatePredictionInterval(forecastValue, stdError, config.Confidence)
		predictions[i] = ForecastPoint{
			Time:       predTime,
			Value:      forecastValue,
			LowerBound: lower,
			UpperBound: upper,
		}
	}

	return &ForecastResult{
		Predictions: predictions,
		Fitted:      fitted,
		Residuals:   residuals,
		ModelInfo: ModelInfo{
			Algorithm:  "sma",
			Parameters: map[string]interface{}{"window_size": windowSize},
			MAPE:       CalculateMAPE(actual, fitted),
			MAE:        CalculateMAE(actual, fitted),
			RMSE:       CalculateRMSE(actual, fitted),
			DataPoints: len(data),
		},
	}, nil
}

// calculateStdError calculates standard error from sum of squared errors
func calculateStdError(sumSquaredError float64, n int) float64 {
	if n <= 1 {
		return 0
	}
	variance := sumSquaredError / float64(n-1)
	if variance < 0 {
		return 0
	}
	return sqrt(variance)
}

// sqrt is a simple square root helper
func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Newton's method
	z := x
	for i := 0; i < 10; i++ {
		z = z - (z*z-x)/(2*z)
	}
	return z
}
