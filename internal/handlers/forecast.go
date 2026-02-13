package handlers

import (
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/analytics/forecast"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/soltixdb/soltix/internal/services"
	"github.com/soltixdb/soltix/internal/utils"
)

// ForecastRequest represents the forecast request body
type ForecastRequest struct {
	StartTime      string   `json:"start_time"`
	EndTime        string   `json:"end_time"`
	IDs            []string `json:"ids"`
	Fields         []string `json:"fields"`
	Method         string   `json:"method"`          // sma, exponential, holt_winters, linear, arima, prophet, auto, ml
	Horizon        int      `json:"horizon"`         // number of periods to forecast
	Field          string   `json:"field"`           // field to forecast
	Algorithm      string   `json:"algorithm"`       // ML algorithm (random_forest, lstm) - used when method=ml
	SeasonalPeriod int      `json:"seasonal_period"` // for holt_winters
	Interval       string   `json:"interval"`        // data interval (1m, 5m, 1h, 1d)
}

// ForecastResponse represents the forecast response
type ForecastResponse struct {
	Database   string           `json:"database"`
	Collection string           `json:"collection"`
	StartTime  string           `json:"start_time"`
	EndTime    string           `json:"end_time"`
	Forecasts  []ForecastResult `json:"forecasts"`
}

// ForecastResult represents forecast results for a single device/field
type ForecastResult struct {
	DeviceID    string               `json:"device_id"`
	Field       string               `json:"field"`
	Predictions []ForecastPrediction `json:"predictions"`
	ModelInfo   forecast.ModelInfo   `json:"model_info"`
}

// ForecastPrediction represents a single forecast prediction
type ForecastPrediction struct {
	Time       string  `json:"time"`
	Value      float64 `json:"value"`
	LowerBound float64 `json:"lower_bound,omitempty"`
	UpperBound float64 `json:"upper_bound,omitempty"`
}

// Forecast handles GET forecast requests
// GET /v1/databases/:database/collections/:collection/forecast
func (h *Handler) Forecast(c *fiber.Ctx) error {
	database := c.Params("database")
	collection := c.Params("collection")

	// Parse query parameters
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")
	method := c.Query("method", "auto")
	horizonStr := c.Query("horizon", "24")
	field := c.Query("field")
	algorithm := c.Query("algorithm")
	seasonalPeriodStr := c.Query("seasonal_period", "24")
	intervalStr := c.Query("interval", "1h")

	// Parse horizon
	horizon, err := strconv.Atoi(horizonStr)
	if err != nil || horizon <= 0 {
		horizon = 24
	}

	// Parse seasonal period
	seasonalPeriod, err := strconv.Atoi(seasonalPeriodStr)
	if err != nil || seasonalPeriod <= 0 {
		seasonalPeriod = 24
	}

	// Parse IDs
	idsStr := c.Query("ids")
	var ids []string
	if idsStr != "" {
		ids = splitAndTrim(idsStr, ",")
	}

	return h.executeForecast(c, database, collection, startTimeStr, endTimeStr,
		ids, method, horizon, field, algorithm, seasonalPeriod, intervalStr)
}

// ForecastPost handles POST forecast requests
// POST /v1/databases/:database/collections/:collection/forecast
func (h *Handler) ForecastPost(c *fiber.Ctx) error {
	database := c.Params("database")
	collection := c.Params("collection")

	var body ForecastRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_JSON",
				Message: "Failed to parse JSON body",
				Details: map[string]interface{}{"error": err.Error()},
			},
		})
	}

	// Apply defaults
	if body.Method == "" {
		body.Method = "auto"
	}
	if body.Horizon <= 0 {
		body.Horizon = 24
	}
	if body.SeasonalPeriod <= 0 {
		body.SeasonalPeriod = 24
	}
	if body.Interval == "" {
		body.Interval = "1h"
	}

	return h.executeForecast(c, database, collection, body.StartTime, body.EndTime,
		body.IDs, body.Method, body.Horizon, body.Field, body.Algorithm, body.SeasonalPeriod, body.Interval)
}

// executeForecast executes the forecast request
func (h *Handler) executeForecast(c *fiber.Ctx, database, collection, startTimeStr, endTimeStr string,
	ids []string, method string, horizon int, field string, algorithm string, seasonalPeriod int, intervalStr string,
) error {
	// Validate required parameters
	if database == "" || collection == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "database and collection are required",
			},
		})
	}

	if startTimeStr == "" || endTimeStr == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "start_time and end_time are required",
			},
		})
	}

	// Parse times
	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "start_time must be in RFC3339 format",
			},
		})
	}

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "end_time must be in RFC3339 format",
			},
		})
	}

	// Build service request
	req := &services.ForecastRequest{
		Database:       database,
		Collection:     collection,
		StartTime:      startTime,
		EndTime:        endTime,
		IDs:            ids,
		Method:         method,
		Horizon:        horizon,
		Field:          field,
		Algorithm:      algorithm,
		SeasonalPeriod: seasonalPeriod,
		Interval:       parseIntervalDuration(intervalStr),
	}

	// Execute via service layer
	result, err := h.forecastService.Execute(c.Context(), req)
	if err != nil {
		// Handle service errors
		if svcErr, ok := err.(*services.ServiceError); ok {
			status := fiber.StatusInternalServerError
			switch svcErr.Code {
			case "COLLECTION_NOT_FOUND":
				status = fiber.StatusNotFound
			case "INVALID_METHOD":
				status = fiber.StatusBadRequest
			}
			return c.Status(status).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    svcErr.Code,
					Message: svcErr.Message,
					Details: svcErr.Details,
				},
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "FORECAST_FAILED",
				Message: err.Error(),
			},
		})
	}

	// Convert service result to handler response
	forecasts := make([]ForecastResult, len(result.Forecasts))
	for i, f := range result.Forecasts {
		predictions := make([]ForecastPrediction, len(f.Predictions))
		for j, p := range f.Predictions {
			predictions[j] = ForecastPrediction{
				Time:       p.Time,
				Value:      p.Value,
				LowerBound: p.LowerBound,
				UpperBound: p.UpperBound,
			}
		}
		forecasts[i] = ForecastResult{
			DeviceID:    f.DeviceID,
			Field:       f.Field,
			Predictions: predictions,
			ModelInfo:   f.ModelInfo,
		}
	}

	return c.JSON(ForecastResponse{
		Database:   result.Database,
		Collection: result.Collection,
		StartTime:  result.StartTime,
		EndTime:    result.EndTime,
		Forecasts:  forecasts,
	})
}

// parseIntervalDuration converts interval string to duration
func parseIntervalDuration(interval string) time.Duration {
	switch interval {
	case "1m":
		return time.Minute
	case "5m":
		return utils.ForecastInterval1m
	case "15m":
		return utils.ForecastInterval5m
	case "30m":
		return utils.ForecastInterval15m
	case "1h":
		return time.Hour
	case "4h":
		return utils.ForecastInterval1h
	case "1d":
		return utils.ForecastInterval1d
	case "1w":
		return utils.ForecastInterval1w
	default:
		return time.Hour
	}
}

// splitAndTrim splits a string and trims whitespace from each part
func splitAndTrim(s, sep string) []string {
	parts := make([]string, 0)
	for _, part := range strings.Split(s, sep) {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}
