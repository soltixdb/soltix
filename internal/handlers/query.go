package handlers

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/soltixdb/soltix/internal/services"
)

// Query handles GET query requests
// GET /v1/databases/:database/collections/:collection/query?start_time=xxx&end_time=xxx&ids=xxx&fields=xxx&limit=xxx
func (h *Handler) Query(c *fiber.Ctx) error {
	// Extract parameters from fiber context
	idsStr := c.Query("ids")
	fieldsStr := c.Query("fields")
	limitStr := c.Query("limit", "0")

	// Parse device IDs (comma-separated)
	var deviceIDs []string
	if idsStr != "" {
		deviceIDs = strings.Split(idsStr, ",")
		for i := range deviceIDs {
			deviceIDs[i] = strings.TrimSpace(deviceIDs[i])
		}
	}

	// Parse fields (comma-separated)
	var fields []string
	if fieldsStr != "" {
		fields = strings.Split(fieldsStr, ",")
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
	}

	// Parse limit
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil && limitStr != "0" && limitStr != "" {
		h.logger.Warn("Failed to parse limit parameter, using default 0",
			"limit", limitStr,
			"error", err,
		)
		limit = 0
	}

	// Parse downsampling threshold
	downsamplingThresholdStr := c.Query("downsampling_threshold", "0")
	downsamplingThreshold, err := strconv.Atoi(downsamplingThresholdStr)
	if err != nil && downsamplingThresholdStr != "0" && downsamplingThresholdStr != "" {
		h.logger.Warn("Failed to parse downsampling_threshold parameter, using default 0",
			"downsampling_threshold", downsamplingThresholdStr,
			"error", err,
		)
		downsamplingThreshold = 0
	}

	// Parse anomaly detection parameters
	anomalyThresholdStr := c.Query("anomaly_threshold", "3.0")
	anomalyThreshold, err := strconv.ParseFloat(anomalyThresholdStr, 64)
	if err != nil && anomalyThresholdStr != "3.0" && anomalyThresholdStr != "" {
		h.logger.Warn("Failed to parse anomaly_threshold parameter, using default 3.0",
			"anomaly_threshold", anomalyThresholdStr,
			"error", err,
		)
		anomalyThreshold = 3.0
	}

	// Create query request with primitive types
	input := models.NewQueryRequest(
		c.Params("database"),
		c.Params("collection"),
		c.Query("start_time"),
		c.Query("end_time"),
		deviceIDs,
		fields,
		limit,
		c.Query("interval"),
		c.Query("aggregation", "sum"),
		c.Query("downsampling", "none"),
		downsamplingThreshold,
		c.Query("anomaly_detection", "none"),
		anomalyThreshold,
		c.Query("anomaly_field"),
	)

	return h.executeQuery(c, input)
}

// QueryPost handles POST query requests with JSON body
// POST /v1/databases/:database/collections/:collection/query
func (h *Handler) QueryPost(c *fiber.Ctx) error {
	// Parse request body
	var body struct {
		StartTime             string   `json:"start_time"`
		EndTime               string   `json:"end_time"`
		IDs                   []string `json:"ids"`
		Fields                []string `json:"fields"`
		Limit                 int64    `json:"limit"`
		Interval              string   `json:"interval"`
		Aggregation           string   `json:"aggregation"`
		Downsampling          string   `json:"downsampling"`
		DownsamplingThreshold int      `json:"downsampling_threshold"`
		AnomalyDetection      string   `json:"anomaly_detection"`
		AnomalyThreshold      float64  `json:"anomaly_threshold"`
		AnomalyField          string   `json:"anomaly_field"`
	}

	if err := c.BodyParser(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_JSON",
				Message: "Failed to parse JSON body",
				Details: map[string]interface{}{"error": err.Error()},
			},
		})
	}

	// Apply default aggregation if not provided
	if body.Aggregation == "" {
		body.Aggregation = "sum"
	}

	// Apply default downsampling if not provided
	if body.Downsampling == "" {
		body.Downsampling = "none"
	}

	// Apply default anomaly detection if not provided
	if body.AnomalyDetection == "" {
		body.AnomalyDetection = "none"
	}

	// Create query request with primitive types
	input := models.NewQueryRequest(
		c.Params("database"),
		c.Params("collection"),
		body.StartTime,
		body.EndTime,
		body.IDs,
		body.Fields,
		body.Limit,
		body.Interval,
		body.Aggregation,
		body.Downsampling,
		body.DownsamplingThreshold,
		body.AnomalyDetection,
		body.AnomalyThreshold,
		body.AnomalyField,
	)

	return h.executeQuery(c, input)
}

// executeQuery executes the query and returns the response
func (h *Handler) executeQuery(c *fiber.Ctx, input *models.QueryRequest) error {
	// Validate input
	err := input.Validate()
	if err != nil {
		fiberErr := err.(*fiber.Error)
		return c.Status(fiberErr.Code).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: fiberErr.Message,
			},
		})
	}

	// Execute query via service layer
	result, err := h.queryService.Execute(c.Context(), input)
	if err != nil {
		// Handle service errors
		if svcErr, ok := err.(*services.ServiceError); ok {
			status := fiber.StatusInternalServerError
			if svcErr.Code == "COLLECTION_NOT_FOUND" {
				status = fiber.StatusNotFound
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
				Code:    "QUERY_FAILED",
				Message: err.Error(),
			},
		})
	}

	// Build response
	response := fiber.Map{
		"database":   result.Database,
		"collection": result.Collection,
		"start_time": result.StartTime,
		"end_time":   result.EndTime,
		"results":    result.Results,
	}

	// Include anomalies if any were detected
	if len(result.Anomalies) > 0 {
		response["anomalies"] = result.Anomalies
	}

	return c.JSON(response)
}
