package services

import (
	"context"
	"time"

	"github.com/soltixdb/soltix/internal/analytics/forecast"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/utils"
)

// ForecastService handles forecasting business logic
type ForecastService struct {
	logger           *logging.Logger
	metadataManager  metadata.Manager
	queryCoordinator *coordinator.QueryCoordinator
}

// NewForecastService creates a new ForecastService
func NewForecastService(
	logger *logging.Logger,
	metadataManager metadata.Manager,
	queryCoordinator *coordinator.QueryCoordinator,
) *ForecastService {
	return &ForecastService{
		logger:           logger,
		metadataManager:  metadataManager,
		queryCoordinator: queryCoordinator,
	}
}

// ForecastRequest represents a forecast request
type ForecastRequest struct {
	Database       string
	Collection     string
	StartTime      time.Time
	EndTime        time.Time
	IDs            []string
	Method         string
	Horizon        int
	Field          string
	Algorithm      string // ML algorithm (random_forest, lstm)
	SeasonalPeriod int
	Interval       time.Duration
}

// ForecastResult represents a single forecast result for a device
type ForecastResult struct {
	DeviceID    string               `json:"device_id"`
	Field       string               `json:"field"`
	Predictions []ForecastPrediction `json:"predictions"`
	ModelInfo   forecast.ModelInfo   `json:"model_info"`
}

// ForecastPrediction represents a single forecast prediction point
type ForecastPrediction struct {
	Time       string  `json:"time"`
	Value      float64 `json:"value"`
	LowerBound float64 `json:"lower_bound,omitempty"`
	UpperBound float64 `json:"upper_bound,omitempty"`
}

// ForecastResponse represents the complete forecast response
type ForecastResponse struct {
	Database   string           `json:"database"`
	Collection string           `json:"collection"`
	StartTime  string           `json:"start_time"`
	EndTime    string           `json:"end_time"`
	Forecasts  []ForecastResult `json:"forecasts"`
}

// Execute performs forecasting on historical data
func (s *ForecastService) Execute(ctx context.Context, req *ForecastRequest) (*ForecastResponse, error) {
	startExec := time.Now()

	// Validate collection existence
	if err := s.metadataManager.ValidateCollection(ctx, req.Database, req.Collection); err != nil {
		return nil, &ServiceError{
			Code:    "COLLECTION_NOT_FOUND",
			Message: err.Error(),
		}
	}

	// Validate forecaster exists
	forecaster, err := forecast.GetForecaster(req.Method)
	if err != nil {
		return nil, &ServiceError{
			Code:    "INVALID_METHOD",
			Message: err.Error(),
			Details: map[string]interface{}{
				"available_methods": forecast.ListForecasters(),
			},
		}
	}

	// Query historical data using the query coordinator
	queryReq := &coordinator.QueryRequest{
		Database:    req.Database,
		Collection:  req.Collection,
		DeviceIDs:   req.IDs,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		Fields:      nil, // get all fields
		Limit:       0,   // no limit
		Interval:    "",  // raw data
		Aggregation: "",  // no aggregation
	}

	results, err := s.queryCoordinator.Query(ctx, queryReq)
	if err != nil {
		return nil, &ServiceError{
			Code:    "QUERY_FAILED",
			Message: "Failed to query historical data",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	// Configure forecaster
	config := forecast.DefaultForecastConfig()
	config.Horizon = req.Horizon
	config.Interval = req.Interval
	config.SeasonalPeriod = req.SeasonalPeriod
	config.Database = req.Database
	config.Collection = req.Collection
	config.Algorithm = req.Algorithm

	// Generate forecasts for each device
	forecasts := s.generateForecasts(results, req.Field, forecaster, config)

	latency := time.Since(startExec)
	s.logger.Info("Forecast completed",
		"database", req.Database,
		"collection", req.Collection,
		"method", req.Method,
		"horizon", req.Horizon,
		"forecasts_count", len(forecasts),
		"latency_ms", latency.Milliseconds())

	return &ForecastResponse{
		Database:   req.Database,
		Collection: req.Collection,
		StartTime:  req.StartTime.Format(time.RFC3339),
		EndTime:    req.EndTime.Format(time.RFC3339),
		Forecasts:  forecasts,
	}, nil
}

// generateForecasts generates forecasts for each device in the results
func (s *ForecastService) generateForecasts(
	results []coordinator.FormattedQueryResult,
	targetField string,
	forecaster forecast.Forecaster,
	config forecast.ForecastConfig,
) []ForecastResult {
	var forecasts []ForecastResult

	for _, result := range results {
		// Determine which field to forecast
		fieldToForecast := targetField
		if fieldToForecast == "" {
			// Use first field if not specified
			for fieldName := range result.Fields {
				fieldToForecast = fieldName
				break
			}
		}

		if fieldToForecast == "" {
			continue
		}

		fieldValues, ok := result.Fields[fieldToForecast]
		if !ok {
			continue
		}

		// Convert to DataPoints
		dataPoints := s.convertToDataPoints(result.Times, fieldValues)
		if len(dataPoints) < config.MinDataPoints {
			continue
		}

		// Set per-device context in config (used by ML forecaster)
		config.DeviceID = result.DeviceID
		config.Field = fieldToForecast

		// Run forecast
		forecastResult, err := forecaster.Forecast(dataPoints, config)
		if err != nil {
			s.logger.Warn("Forecast failed for device",
				"device_id", result.DeviceID,
				"field", fieldToForecast,
				"error", err)
			continue
		}

		// Convert predictions to response format
		predictions := make([]ForecastPrediction, len(forecastResult.Predictions))
		for i, p := range forecastResult.Predictions {
			predictions[i] = ForecastPrediction{
				Time:       p.Time.Format(time.RFC3339),
				Value:      p.Value,
				LowerBound: p.LowerBound,
				UpperBound: p.UpperBound,
			}
		}

		forecasts = append(forecasts, ForecastResult{
			DeviceID:    result.DeviceID,
			Field:       fieldToForecast,
			Predictions: predictions,
			ModelInfo:   forecastResult.ModelInfo,
		})
	}

	return forecasts
}

// convertToDataPoints converts time strings and values to forecast.DataPoint slice
func (s *ForecastService) convertToDataPoints(times []string, values []interface{}) []forecast.DataPoint {
	dataPoints := make([]forecast.DataPoint, 0, len(times))

	for i, timeStr := range times {
		if i >= len(values) {
			break
		}

		value := values[i]
		if value == nil {
			continue
		}

		// Convert value to float64
		floatVal, ok := utils.ToFloat64(value)
		if !ok {
			continue
		}

		// Parse time
		t, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			continue
		}

		dataPoints = append(dataPoints, forecast.DataPoint{
			Time:  t,
			Value: floatVal,
		})
	}

	return dataPoints
}
