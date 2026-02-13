package forecast

import (
	"fmt"
	"math"
	"time"
)

// ExponentialSmoothingForecaster implements Simple Exponential Smoothing forecasting
type ExponentialSmoothingForecaster struct{}

// NewExponentialSmoothingForecaster creates a new Exponential Smoothing forecaster
func NewExponentialSmoothingForecaster() *ExponentialSmoothingForecaster {
	return &ExponentialSmoothingForecaster{}
}

func init() {
	RegisterForecaster("exponential", NewExponentialSmoothingForecaster())
}

// Name returns the algorithm name
func (f *ExponentialSmoothingForecaster) Name() string {
	return "exponential"
}

// Forecast generates predictions using Simple Exponential Smoothing
func (f *ExponentialSmoothingForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: need %d, have %d", config.MinDataPoints, len(data))
	}

	alpha := config.Alpha
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}

	// Calculate fitted values using exponential smoothing
	fitted := make([]float64, len(data))
	fitted[0] = data[0].Value

	for i := 1; i < len(data); i++ {
		fitted[i] = alpha*data[i-1].Value + (1-alpha)*fitted[i-1]
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
		stdError = math.Sqrt(sumSquaredError / float64(len(data)-1))
	}

	// Final forecast value
	forecastValue := alpha*data[len(data)-1].Value + (1-alpha)*fitted[len(data)-1]

	// Generate predictions
	predictions := make([]ForecastPoint, config.Horizon)
	lastTime := data[len(data)-1].Time
	interval := config.Interval
	if interval == 0 && len(data) >= 2 {
		interval = data[1].Time.Sub(data[0].Time)
	}

	for i := 0; i < config.Horizon; i++ {
		predTime := lastTime.Add(interval * time.Duration(1+i))
		// Increase uncertainty for further predictions
		adjustedStdError := stdError * math.Sqrt(float64(i+1))
		lower, upper := calculatePredictionInterval(forecastValue, adjustedStdError, config.Confidence)
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
			Algorithm:  "exponential",
			Parameters: map[string]interface{}{"alpha": alpha},
			MAPE:       CalculateMAPE(actual, fitted),
			MAE:        CalculateMAE(actual, fitted),
			RMSE:       CalculateRMSE(actual, fitted),
			DataPoints: len(data),
		},
	}, nil
}
