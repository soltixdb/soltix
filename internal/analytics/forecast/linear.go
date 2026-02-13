package forecast

import (
	"fmt"
	"math"
	"time"
)

// LinearRegressionForecaster implements Linear Regression forecasting
type LinearRegressionForecaster struct{}

// NewLinearRegressionForecaster creates a new Linear Regression forecaster
func NewLinearRegressionForecaster() *LinearRegressionForecaster {
	return &LinearRegressionForecaster{}
}

func init() {
	RegisterForecaster("linear", NewLinearRegressionForecaster())
}

// Name returns the algorithm name
func (f *LinearRegressionForecaster) Name() string {
	return "linear"
}

// Forecast generates predictions using Linear Regression
func (f *LinearRegressionForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: need %d, have %d", config.MinDataPoints, len(data))
	}

	n := float64(len(data))

	// Calculate sums for linear regression
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0

	for i, p := range data {
		x := float64(i)
		sumX += x
		sumY += p.Value
		sumXY += x * p.Value
		sumX2 += x * x
	}

	// Calculate slope and intercept
	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return nil, fmt.Errorf("cannot calculate regression: all x values are the same")
	}

	slope := (n*sumXY - sumX*sumY) / denominator
	intercept := (sumY - slope*sumX) / n

	// Calculate fitted values
	fitted := make([]float64, len(data))
	residuals := make([]float64, len(data))
	actual := make([]float64, len(data))
	sumSquaredError := 0.0

	for i := range data {
		fitted[i] = intercept + slope*float64(i)
		actual[i] = data[i].Value
		residuals[i] = data[i].Value - fitted[i]
		sumSquaredError += residuals[i] * residuals[i]
	}

	stdError := 0.0
	if len(data) > 2 {
		stdError = math.Sqrt(sumSquaredError / float64(len(data)-2))
	}

	// Generate predictions
	predictions := make([]ForecastPoint, config.Horizon)
	lastTime := data[len(data)-1].Time
	interval := config.Interval
	if interval == 0 && len(data) >= 2 {
		interval = data[1].Time.Sub(data[0].Time)
	}

	for i := 0; i < config.Horizon; i++ {
		predTime := lastTime.Add(interval * time.Duration(1+i))
		forecastIdx := float64(len(data) + i)
		forecastValue := intercept + slope*forecastIdx

		// Standard error increases for extrapolation
		meanX := sumX / n
		xDiff := forecastIdx - meanX
		predStdError := stdError * math.Sqrt(1+1/n+xDiff*xDiff/(sumX2-sumX*sumX/n))

		lower, upper := calculatePredictionInterval(forecastValue, predStdError, config.Confidence)

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
			Algorithm: "linear",
			Parameters: map[string]interface{}{
				"slope":     slope,
				"intercept": intercept,
			},
			MAPE:       CalculateMAPE(actual, fitted),
			MAE:        CalculateMAE(actual, fitted),
			RMSE:       CalculateRMSE(actual, fitted),
			DataPoints: len(data),
		},
	}, nil
}
