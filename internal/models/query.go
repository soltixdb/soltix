package models

import (
	"time"

	"github.com/gofiber/fiber/v2"
)

// QueryRequest represents the parsed query input
type QueryRequest struct {
	Database              string
	Collection            string
	StartTime             string
	EndTime               string
	IDs                   []string
	Fields                []string
	Limit                 int64
	Interval              string  // 1m, 5m, 1h, 1d, 1mo, 1y
	Aggregation           string  // max, min, avg, sum, count (default: sum)
	Downsampling          string  // none (default), auto, lttb, minmax, avg, m4
	DownsamplingThreshold int     // target point count (0 = auto-calculate based on data size)
	AnomalyDetection      string  // none (default), zscore, iqr, moving_avg, auto
	AnomalyThreshold      float64 // threshold for anomaly detection (default: 3.0 for zscore)
	AnomalyField          string  // field to detect anomalies on (empty = all fields)
	StartTimeParsed       time.Time
	EndTimeParsed         time.Time
}

// NewQueryRequest creates a new QueryRequest with primitive types
func NewQueryRequest(database, collection, startTime, endTime string, ids, fields []string, limit int64, interval, aggregation, downsampling string, downsamplingThreshold int, anomalyDetection string, anomalyThreshold float64, anomalyField string) *QueryRequest {
	// Apply default limit if not set
	// 0 means no limit
	if limit < 0 {
		limit = 1000
	}

	// Apply default aggregation
	if aggregation == "" {
		aggregation = "sum"
	}

	// Apply default downsampling
	if downsampling == "" {
		downsampling = "none"
	}

	// Apply default anomaly detection
	if anomalyDetection == "" {
		anomalyDetection = "none"
	}

	// Apply default anomaly threshold (3 standard deviations)
	if anomalyThreshold <= 0 {
		anomalyThreshold = 3.0
	}

	return &QueryRequest{
		Database:              database,
		Collection:            collection,
		StartTime:             startTime,
		EndTime:               endTime,
		IDs:                   ids,
		Fields:                fields,
		Limit:                 limit,
		Interval:              interval,
		Aggregation:           aggregation,
		Downsampling:          downsampling,
		DownsamplingThreshold: downsamplingThreshold,
		AnomalyDetection:      anomalyDetection,
		AnomalyThreshold:      anomalyThreshold,
		AnomalyField:          anomalyField,
	}
}

// Validate validates the query input and parses times into StartTimeParsed and EndTimeParsed fields
func (q *QueryRequest) Validate() error {
	// Validate required parameters
	if q.Database == "" || q.Collection == "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "database and collection are required",
		}
	}

	if len(q.IDs) == 0 {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "'ids' (device IDs) is required",
		}
	}

	if q.StartTime == "" || q.EndTime == "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "start_time and end_time are required",
		}
	}

	// Parse start_time
	startTime, err := time.Parse(time.RFC3339, q.StartTime)
	if err != nil {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "start_time must be in RFC3339 format (e.g., 2006-01-02T15:04:05Z)",
		}
	}

	// Parse end_time
	endTime, err := time.Parse(time.RFC3339, q.EndTime)
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

	// Validate limit
	if q.Limit < 0 {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "limit must be a positive integer",
		}
	}

	q.StartTimeParsed = startTime
	q.EndTimeParsed = endTime

	// Validate interval if provided
	if q.Interval != "" {
		validIntervals := map[string]bool{
			"1m": true, "5m": true, "1h": true, "1d": true, "1mo": true, "1y": true,
		}
		if !validIntervals[q.Interval] {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "interval must be one of: 1m, 5m, 1h, 1d, 1mo, 1y",
			}
		}

		// Validate time range based on interval
		duration := endTime.Sub(startTime)
		switch q.Interval {
		case "1m", "5m", "1h":
			// Raw data and hourly aggregates: max 7 days
			if duration > 7*24*time.Hour {
				return &fiber.Error{
					Code:    fiber.StatusBadRequest,
					Message: "time range for interval 1m/5m/1h cannot exceed 7 days",
				}
			}
		case "1d":
			// Daily aggregates: max 3 months (90 days)
			if duration > 90*24*time.Hour {
				return &fiber.Error{
					Code:    fiber.StatusBadRequest,
					Message: "time range for interval 1d cannot exceed 3 months (90 days)",
				}
			}
		case "1mo":
			// Monthly aggregates: max 3 years
			if duration > 3*365*24*time.Hour {
				return &fiber.Error{
					Code:    fiber.StatusBadRequest,
					Message: "time range for interval 1mo cannot exceed 3 years",
				}
			}
			// case "1y": no limit for yearly aggregates
		}
	}

	// Validate aggregation
	validAggregations := map[string]bool{
		"sum": true, "avg": true, "min": true, "max": true, "count": true,
	}
	if !validAggregations[q.Aggregation] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "aggregation must be one of: sum, avg, min, max, count",
		}
	}

	// Validate downsampling
	validDownsamplingModes := map[string]bool{
		"none": true, "auto": true, "lttb": true, "minmax": true, "avg": true, "m4": true,
	}
	if !validDownsamplingModes[q.Downsampling] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "downsampling must be one of: none, auto, lttb, minmax, avg, m4",
		}
	}

	// Validate downsampling threshold
	if q.Downsampling != "none" && q.DownsamplingThreshold < 0 {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "downsampling_threshold must be a non-negative integer",
		}
	}

	return nil
}
