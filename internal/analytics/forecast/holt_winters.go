package forecast

import (
	"fmt"
	"math"
	"time"
)

// HoltWintersForecaster implements Holt-Winters (Triple Exponential Smoothing) forecasting
type HoltWintersForecaster struct{}

// NewHoltWintersForecaster creates a new Holt-Winters forecaster
func NewHoltWintersForecaster() *HoltWintersForecaster {
	return &HoltWintersForecaster{}
}

func init() {
	RegisterForecaster("holt_winters", NewHoltWintersForecaster())
}

// Name returns the algorithm name
func (f *HoltWintersForecaster) Name() string {
	return "holt_winters"
}

// Forecast generates predictions using Holt-Winters Triple Exponential Smoothing
func (f *HoltWintersForecaster) Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error) {
	if len(data) < config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: need %d, have %d", config.MinDataPoints, len(data))
	}

	alpha := config.Alpha
	beta := config.Beta
	gamma := config.Gamma
	period := config.SeasonalPeriod

	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}
	if beta <= 0 || beta > 1 {
		beta = 0.1
	}
	if gamma <= 0 || gamma > 1 {
		gamma = 0.1
	}
	if period <= 0 || period > len(data)/2 {
		period = 24 // Default to daily seasonality
	}

	// Need at least 2 complete seasons
	if len(data) < period*2 {
		// Fall back to simple exponential smoothing
		return NewExponentialSmoothingForecaster().Forecast(data, config)
	}

	n := len(data)

	// Initialize level, trend, and seasonal components
	level := make([]float64, n)
	trend := make([]float64, n)
	seasonal := make([]float64, n+config.Horizon)

	// Initialize level as mean of first season
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += data[i].Value
	}
	level[0] = sum / float64(period)

	// Initialize trend
	trend[0] = (data[period].Value - data[0].Value) / float64(period)

	// Initialize seasonal factors
	for i := 0; i < period; i++ {
		if level[0] != 0 {
			seasonal[i] = data[i].Value / level[0]
		} else {
			seasonal[i] = 1.0
		}
	}

	// Calculate fitted values
	fitted := make([]float64, n)
	fitted[0] = level[0] * seasonal[0]

	for i := 1; i < n; i++ {
		// Previous seasonal factor (from one period ago)
		var prevSeasonal float64
		if i >= period {
			prevSeasonal = seasonal[i-period]
		} else {
			prevSeasonal = seasonal[i]
		}

		if prevSeasonal == 0 {
			prevSeasonal = 1.0
		}

		// Update level
		level[i] = alpha*(data[i].Value/prevSeasonal) + (1-alpha)*(level[i-1]+trend[i-1])

		// Update trend
		trend[i] = beta*(level[i]-level[i-1]) + (1-beta)*trend[i-1]

		// Update seasonal
		if level[i] != 0 {
			seasonal[i] = gamma*(data[i].Value/level[i]) + (1-gamma)*prevSeasonal
		} else {
			seasonal[i] = prevSeasonal
		}

		// Calculate fitted value
		fitted[i] = (level[i-1] + trend[i-1]) * prevSeasonal
	}

	// Calculate residuals and standard error
	residuals := make([]float64, n)
	actual := make([]float64, n)
	sumSquaredError := 0.0
	for i := range data {
		actual[i] = data[i].Value
		residuals[i] = data[i].Value - fitted[i]
		sumSquaredError += residuals[i] * residuals[i]
	}
	stdError := 0.0
	if n > 1 {
		stdError = math.Sqrt(sumSquaredError / float64(n-1))
	}

	// Generate predictions
	predictions := make([]ForecastPoint, config.Horizon)
	lastTime := data[n-1].Time
	interval := config.Interval
	if interval == 0 && len(data) >= 2 {
		interval = data[1].Time.Sub(data[0].Time)
	}

	lastLevel := level[n-1]
	lastTrend := trend[n-1]

	for i := 0; i < config.Horizon; i++ {
		predTime := lastTime.Add(interval * time.Duration(1+i))

		// Get seasonal factor from one period ago
		seasonalIdx := (n + i) % period
		seasonalFactor := seasonal[n-period+seasonalIdx]
		if seasonalFactor == 0 {
			seasonalFactor = 1.0
		}

		forecastValue := (lastLevel + float64(i+1)*lastTrend) * seasonalFactor

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
			Algorithm: "holt_winters",
			Parameters: map[string]interface{}{
				"alpha":  alpha,
				"beta":   beta,
				"gamma":  gamma,
				"period": period,
			},
			MAPE:       CalculateMAPE(actual, fitted),
			MAE:        CalculateMAE(actual, fitted),
			RMSE:       CalculateRMSE(actual, fitted),
			DataPoints: n,
		},
	}, nil
}
