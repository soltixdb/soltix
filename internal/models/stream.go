package models

import (
	"time"

	"github.com/gofiber/fiber/v2"
)

// StreamQueryRequest represents a streaming query request
type StreamQueryRequest struct {
	*QueryRequest
	ChunkSize     int    `json:"chunk_size"`     // Number of data points per chunk (default: 1000)
	ChunkInterval string `json:"chunk_interval"` // Time interval per chunk (e.g., "1d", "1h") - mutually exclusive with ChunkSize
}

// NewStreamQueryRequest creates a new StreamQueryRequest
func NewStreamQueryRequest(
	database, collection, startTime, endTime string,
	ids, fields []string,
	limit int64,
	interval, aggregation, downsampling string,
	downsamplingThreshold int,
	anomalyDetection string,
	anomalyThreshold float64,
	anomalyField string,
	chunkSize int,
	chunkInterval string,
) *StreamQueryRequest {
	baseQuery := NewQueryRequest(
		database, collection, startTime, endTime,
		ids, fields, limit, interval, aggregation, downsampling,
		downsamplingThreshold, anomalyDetection, anomalyThreshold, anomalyField,
	)

	// Apply default chunk size if not set
	if chunkSize <= 0 && chunkInterval == "" {
		chunkSize = 1000
	}

	return &StreamQueryRequest{
		QueryRequest:  baseQuery,
		ChunkSize:     chunkSize,
		ChunkInterval: chunkInterval,
	}
}

// Validate validates the streaming query request
// Streaming API has relaxed time range limits compared to regular query
func (s *StreamQueryRequest) Validate() error {
	// Validate required parameters (without time range restrictions)
	if err := s.validateBase(); err != nil {
		return err
	}

	// Validate streaming-specific time range limits (relaxed)
	if err := s.validateStreamingTimeRange(); err != nil {
		return err
	}

	// Validate chunk parameters
	if s.ChunkSize > 0 && s.ChunkInterval != "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "chunk_size and chunk_interval are mutually exclusive",
		}
	}

	// Validate chunk_size
	if s.ChunkSize > 0 {
		if s.ChunkSize < 10 {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "chunk_size must be at least 10",
			}
		}
		if s.ChunkSize > 10000 {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "chunk_size cannot exceed 10000",
			}
		}
	}

	// Validate chunk_interval
	if s.ChunkInterval != "" {
		validChunkIntervals := map[string]bool{
			"5m": true, "15m": true, "30m": true, "1h": true, "6h": true, "12h": true, "1d": true,
		}
		if !validChunkIntervals[s.ChunkInterval] {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "chunk_interval must be one of: 5m, 15m, 30m, 1h, 6h, 12h, 1d",
			}
		}
	}

	// Streaming doesn't support limit (it's meant for large datasets)
	if s.Limit > 0 {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "limit is not supported for streaming queries",
		}
	}

	return nil
}

// validateBase validates common parameters without time range restrictions
func (s *StreamQueryRequest) validateBase() error {
	// Validate required parameters
	if s.Database == "" || s.Collection == "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "database and collection are required",
		}
	}

	if len(s.IDs) == 0 {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "'ids' (device IDs) is required",
		}
	}

	if s.StartTime == "" || s.EndTime == "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "start_time and end_time are required",
		}
	}

	// Parse start_time
	startTime, err := time.Parse(time.RFC3339, s.StartTime)
	if err != nil {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "start_time must be in RFC3339 format (e.g., 2006-01-02T15:04:05Z)",
		}
	}

	// Parse end_time
	endTime, err := time.Parse(time.RFC3339, s.EndTime)
	if err != nil {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "end_time must be in RFC3339 format (e.g., 2006-01-02T15:04:05Z)",
		}
	}

	// Validate time range
	if endTime.Before(startTime) {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "end_time must be after start_time",
		}
	}

	s.StartTimeParsed = startTime
	s.EndTimeParsed = endTime

	// Validate interval if provided
	if s.Interval != "" {
		validIntervals := map[string]bool{
			"1m": true, "5m": true, "1h": true, "1d": true, "1mo": true, "1y": true,
		}
		if !validIntervals[s.Interval] {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "interval must be one of: 1m, 5m, 1h, 1d, 1mo, 1y",
			}
		}
	}

	// Validate aggregation
	validAggregations := map[string]bool{
		"sum": true, "avg": true, "min": true, "max": true, "count": true,
		"min_time": true, "max_time": true,
	}
	if !validAggregations[s.Aggregation] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "aggregation must be one of: sum, avg, min, max, count, min_time, max_time",
		}
	}

	// Validate downsampling
	validDownsamplingModes := map[string]bool{
		"none": true, "auto": true, "lttb": true, "minmax": true, "avg": true, "m4": true,
	}
	if !validDownsamplingModes[s.Downsampling] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "downsampling must be one of: none, auto, lttb, minmax, avg, m4",
		}
	}

	return nil
}

// validateStreamingTimeRange validates time range with relaxed limits for streaming
// Streaming API allows larger time ranges because data is chunked and streamed
func (s *StreamQueryRequest) validateStreamingTimeRange() error {
	duration := s.EndTimeParsed.Sub(s.StartTimeParsed)

	switch s.Interval {
	case "", "1m", "5m":
		// Raw data (no interval or 1m/5m): max 30 days (vs 7 days for regular query)
		if duration > 30*24*time.Hour {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "time range for raw data (interval 1m/5m or empty) cannot exceed 30 days for streaming",
			}
		}
	case "1h":
		// Hourly aggregates: max 90 days (vs 7 days for regular query)
		if duration > 90*24*time.Hour {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "time range for interval 1h cannot exceed 90 days for streaming",
			}
		}
	case "1d":
		// Daily aggregates: max 1 year (vs 90 days for regular query)
		if duration > 365*24*time.Hour {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "time range for interval 1d cannot exceed 1 year for streaming",
			}
		}
	case "1mo":
		// Monthly aggregates: max 10 years (vs 3 years for regular query)
		if duration > 10*365*24*time.Hour {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "time range for interval 1mo cannot exceed 10 years for streaming",
			}
		}
		// case "1y": no limit for yearly aggregates
	}

	return nil
}

// GetChunkDuration returns the duration for chunk_interval
func (s *StreamQueryRequest) GetChunkDuration() time.Duration {
	intervalDurations := map[string]time.Duration{
		"5m":  5 * time.Minute,
		"15m": 15 * time.Minute,
		"30m": 30 * time.Minute,
		"1h":  1 * time.Hour,
		"6h":  6 * time.Hour,
		"12h": 12 * time.Hour,
		"1d":  24 * time.Hour,
	}
	return intervalDurations[s.ChunkInterval]
}
