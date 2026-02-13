// Package processing provides post-processing utilities for query results
// including downsampling, timezone conversion, and anomaly detection.
package processing

import (
	"time"

	"github.com/soltixdb/soltix/internal/analytics/anomaly"
	"github.com/soltixdb/soltix/internal/logging"
)

// Processor handles post-processing of query results
type Processor struct {
	logger *logging.Logger
}

// NewProcessor creates a new Processor instance
func NewProcessor(logger *logging.Logger) *Processor {
	return &Processor{
		logger: logger,
	}
}

// QueryResult represents query results in time-series format
type QueryResult struct {
	DeviceID string                   `json:"id"`
	Times    []string                 `json:"times"`
	Fields   map[string][]interface{} `json:"-"` // field_name -> [value1, value2, ...] (nil for missing)
}

// AnomalyResult represents detected anomalies in query results
type AnomalyResult struct {
	Time      string              `json:"time"`
	DeviceID  string              `json:"device_id"`
	Field     string              `json:"field"`
	Value     float64             `json:"value"`
	Expected  *anomaly.Range      `json:"expected,omitempty"`
	Score     float64             `json:"score"`
	Type      anomaly.AnomalyType `json:"type"`
	Algorithm string              `json:"algorithm"`
}

// DownsamplingConfig holds configuration for downsampling
type DownsamplingConfig struct {
	Mode      string // none, auto, lttb, minmax, avg, m4
	Threshold int    // target number of points
}

// AnomalyConfig holds configuration for anomaly detection
type AnomalyConfig struct {
	Algorithm   string  // none, zscore, iqr, moving_avg, auto
	Threshold   float64 // detection threshold
	TargetField string  // optional: specific field to analyze
}

// TimezoneConfig holds configuration for timezone conversion
type TimezoneConfig struct {
	Location *time.Location // target timezone
}

// CountPoints counts total data points across all results
func CountPoints(results []QueryResult) int {
	total := 0
	for _, result := range results {
		total += len(result.Times)
	}
	return total
}
