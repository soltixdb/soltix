package handlers

import (
	"bufio"
	"context"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/soltixdb/soltix/internal/services"
)

// QueryStream handles GET streaming query requests with SSE
// GET /v1/databases/:database/collections/:collection/query/stream?start_time=xxx&end_time=xxx&chunk_size=1000
func (h *Handler) QueryStream(c *fiber.Ctx) error {
	// Parse parameters from query string
	idsStr := c.Query("ids")
	fieldsStr := c.Query("fields")
	chunkSizeStr := c.Query("chunk_size", "1000")
	chunkInterval := c.Query("chunk_interval", "")

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

	// Parse chunk_size
	chunkSize, err := strconv.Atoi(chunkSizeStr)
	if err != nil && chunkSizeStr != "" {
		h.logger.Warn("Failed to parse chunk_size parameter, using default 1000",
			"chunk_size", chunkSizeStr,
			"error", err,
		)
		chunkSize = 1000
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

	// Create stream query request
	input := models.NewStreamQueryRequest(
		c.Params("database"),
		c.Params("collection"),
		c.Query("start_time"),
		c.Query("end_time"),
		deviceIDs,
		fields,
		0, // limit not supported for streaming
		c.Query("interval"),
		c.Query("aggregation", "sum"),
		c.Query("downsampling", "none"),
		downsamplingThreshold,
		c.Query("anomaly_detection", "none"),
		anomalyThreshold,
		c.Query("anomaly_field"),
		chunkSize,
		chunkInterval,
	)

	return h.executeStreamQuery(c, input)
}

// QueryStreamPost handles POST streaming query requests with JSON body
// POST /v1/databases/:database/collections/:collection/query/stream
func (h *Handler) QueryStreamPost(c *fiber.Ctx) error {
	// Parse request body
	var body struct {
		StartTime             string   `json:"start_time"`
		EndTime               string   `json:"end_time"`
		IDs                   []string `json:"ids"`
		Fields                []string `json:"fields"`
		Interval              string   `json:"interval"`
		Aggregation           string   `json:"aggregation"`
		Downsampling          string   `json:"downsampling"`
		DownsamplingThreshold int      `json:"downsampling_threshold"`
		AnomalyDetection      string   `json:"anomaly_detection"`
		AnomalyThreshold      float64  `json:"anomaly_threshold"`
		AnomalyField          string   `json:"anomaly_field"`
		ChunkSize             int      `json:"chunk_size"`
		ChunkInterval         string   `json:"chunk_interval"`
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

	// Apply defaults
	if body.Aggregation == "" {
		body.Aggregation = "sum"
	}
	if body.Downsampling == "" {
		body.Downsampling = "none"
	}
	if body.AnomalyDetection == "" {
		body.AnomalyDetection = "none"
	}
	if body.ChunkSize == 0 && body.ChunkInterval == "" {
		body.ChunkSize = 1000
	}

	// Create stream query request
	input := models.NewStreamQueryRequest(
		c.Params("database"),
		c.Params("collection"),
		body.StartTime,
		body.EndTime,
		body.IDs,
		body.Fields,
		0, // limit not supported for streaming
		body.Interval,
		body.Aggregation,
		body.Downsampling,
		body.DownsamplingThreshold,
		body.AnomalyDetection,
		body.AnomalyThreshold,
		body.AnomalyField,
		body.ChunkSize,
		body.ChunkInterval,
	)

	return h.executeStreamQuery(c, input)
}

// executeStreamQuery executes the streaming query with SSE
func (h *Handler) executeStreamQuery(c *fiber.Ctx, input *models.StreamQueryRequest) error {
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

	// Check if client wants legacy mode (load all then chunk)
	// By default, use true gRPC streaming
	useLegacyMode := c.Query("legacy", "false") == "true"

	// Set SSE headers
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("Transfer-Encoding", "chunked")
	c.Set("X-Accel-Buffering", "no") // Disable nginx buffering

	// Create a background context for the streaming operation
	// since the Fiber context may be released after the handler returns
	streamCtx := context.Background()

	// Set stream body writer
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		// Create SSE writer
		sseWriter := services.NewSSEWriter(w)

		var execErr error
		if useLegacyMode {
			// Legacy mode: load all data first, then chunk at HTTP layer
			// Send start event for legacy mode
			if err := sseWriter.WriteEvent("start", map[string]interface{}{
				"database":   input.Database,
				"collection": input.Collection,
				"start_time": input.StartTime,
				"end_time":   input.EndTime,
				"mode":       "legacy",
			}); err != nil {
				h.logger.Error("Failed to write start event", "error", err)
				return
			}
			if err := sseWriter.Flush(); err != nil {
				h.logger.Error("Failed to flush start event", "error", err)
				return
			}
			execErr = h.streamQueryService.ExecuteStream(streamCtx, input, sseWriter)
		} else {
			// True gRPC streaming: stream from storage nodes
			execErr = h.streamQueryService.ExecuteStreamWithGRPC(streamCtx, input, sseWriter)
		}

		if execErr != nil {
			// Send error event
			h.logger.Error("Streaming query failed", "error", execErr)
			if svcErr, ok := execErr.(*services.ServiceError); ok {
				_ = sseWriter.WriteEvent("error", map[string]interface{}{
					"code":    svcErr.Code,
					"message": svcErr.Message,
					"details": svcErr.Details,
				})
			} else {
				_ = sseWriter.WriteEvent("error", map[string]interface{}{
					"code":    "QUERY_FAILED",
					"message": execErr.Error(),
				})
			}
			_ = sseWriter.Flush()
			return
		}

		// Send done event
		if err := sseWriter.WriteEvent("done", map[string]interface{}{
			"completed": true,
		}); err != nil {
			h.logger.Error("Failed to write done event", "error", err)
			return
		}
		if err := sseWriter.Flush(); err != nil {
			h.logger.Error("Failed to flush done event", "error", err)
		}

		h.logger.Info("Streaming query completed successfully",
			"database", input.Database,
			"collection", input.Collection,
		)
	})

	return nil
}
