package forecast

import (
	"fmt"
	"math"
	"time"

	"github.com/soltixdb/soltix/internal/analytics"
)

// DataPoint is an alias to the shared analytics.TimeSeriesPoint type.
// This maintains backward compatibility while eliminating code duplication.
type DataPoint = analytics.TimeSeriesPoint

// ForecastPoint represents a single forecast prediction
type ForecastPoint struct {
	Time       time.Time
	Value      float64
	LowerBound float64
	UpperBound float64
}

// ModelInfo contains metadata about the forecast model
type ModelInfo struct {
	Algorithm  string                 `json:"algorithm"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	MAPE       float64                `json:"mape,omitempty"` // Mean Absolute Percentage Error
	MAE        float64                `json:"mae,omitempty"`  // Mean Absolute Error
	RMSE       float64                `json:"rmse,omitempty"` // Root Mean Squared Error
	DataPoints int                    `json:"data_points"`    // Number of data points used
}

// ForecastResult contains the forecast predictions and model information
type ForecastResult struct {
	Predictions []ForecastPoint `json:"predictions"`
	Fitted      []float64       `json:"fitted,omitempty"`    // Fitted values for historical data
	Residuals   []float64       `json:"residuals,omitempty"` // Residuals (actual - fitted)
	ModelInfo   ModelInfo       `json:"model_info"`
}

// ForecastConfig holds configuration for forecasting
type ForecastConfig struct {
	Horizon        int           // Number of periods to forecast
	WindowSize     int           // Window size for moving average methods
	Alpha          float64       // Smoothing factor for exponential methods (0-1)
	Beta           float64       // Trend smoothing factor for Holt-Winters (0-1)
	Gamma          float64       // Seasonal smoothing factor for Holt-Winters (0-1)
	SeasonalPeriod int           // Period for seasonal decomposition
	Confidence     float64       // Confidence level for prediction intervals (0-1)
	MinDataPoints  int           // Minimum data points required
	Interval       time.Duration // Time interval between data points

	// Model context for stored model forecasters
	Database   string // Database name for model lookup
	Collection string // Collection name for model lookup
	DeviceID   string // Device ID for model lookup
	Field      string // Field name for model lookup
	Algorithm  string // ML algorithm name (e.g., random_forest, lstm)
}

// DefaultForecastConfig returns default forecast configuration
func DefaultForecastConfig() ForecastConfig {
	return ForecastConfig{
		Horizon:        24,        // Forecast 24 periods ahead
		WindowSize:     7,         // 7-point moving average
		Alpha:          0.3,       // Exponential smoothing factor
		Beta:           0.1,       // Trend smoothing factor
		Gamma:          0.1,       // Seasonal smoothing factor
		SeasonalPeriod: 24,        // Daily seasonality (assuming hourly data)
		Confidence:     0.95,      // 95% confidence interval
		MinDataPoints:  10,        // Need at least 10 points
		Interval:       time.Hour, // Default hourly interval
	}
}

// Forecaster interface for all forecasting algorithms
type Forecaster interface {
	// Name returns the algorithm name
	Name() string
	// Forecast generates predictions for future time periods
	Forecast(data []DataPoint, config ForecastConfig) (*ForecastResult, error)
}

// Registry holds available forecasters
var forecasterRegistry = make(map[string]Forecaster)

// RegisterForecaster adds a forecaster to the registry
func RegisterForecaster(name string, forecaster Forecaster) {
	forecasterRegistry[name] = forecaster
}

// GetForecaster returns a forecaster by name
func GetForecaster(name string) (Forecaster, error) {
	if forecaster, ok := forecasterRegistry[name]; ok {
		return forecaster, nil
	}
	return nil, fmt.Errorf("unknown forecaster: %s", name)
}

// ListForecasters returns list of available forecaster names
func ListForecasters() []string {
	names := make([]string, 0, len(forecasterRegistry))
	for name := range forecasterRegistry {
		names = append(names, name)
	}
	return names
}

// CalculateMAPE calculates Mean Absolute Percentage Error
func CalculateMAPE(actual, predicted []float64) float64 {
	if len(actual) != len(predicted) || len(actual) == 0 {
		return 0
	}

	sum := 0.0
	count := 0
	for i := range actual {
		if actual[i] != 0 {
			sum += math.Abs((actual[i] - predicted[i]) / actual[i])
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return (sum / float64(count)) * 100
}

// CalculateMAE calculates Mean Absolute Error
func CalculateMAE(actual, predicted []float64) float64 {
	if len(actual) != len(predicted) || len(actual) == 0 {
		return 0
	}

	sum := 0.0
	for i := range actual {
		sum += math.Abs(actual[i] - predicted[i])
	}
	return sum / float64(len(actual))
}

// CalculateRMSE calculates Root Mean Squared Error
func CalculateRMSE(actual, predicted []float64) float64 {
	if len(actual) != len(predicted) || len(actual) == 0 {
		return 0
	}

	sum := 0.0
	for i := range actual {
		diff := actual[i] - predicted[i]
		sum += diff * diff
	}
	return math.Sqrt(sum / float64(len(actual)))
}

// calculatePredictionInterval calculates prediction interval bounds
func calculatePredictionInterval(value, stdError, confidence float64) (lower, upper float64) {
	// Z-score for confidence level (approximate)
	var z float64
	switch {
	case confidence >= 0.99:
		z = 2.576
	case confidence >= 0.95:
		z = 1.96
	case confidence >= 0.90:
		z = 1.645
	default:
		z = 1.96
	}

	margin := z * stdError
	return value - margin, value + margin
}
