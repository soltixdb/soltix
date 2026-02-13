package models

import (
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewStreamQueryRequest(t *testing.T) {
	tests := []struct {
		name          string
		chunkSize     int
		chunkInterval string
		expectedSize  int
	}{
		{
			name:          "default chunk size when both empty",
			chunkSize:     0,
			chunkInterval: "",
			expectedSize:  1000,
		},
		{
			name:          "custom chunk size",
			chunkSize:     500,
			chunkInterval: "",
			expectedSize:  500,
		},
		{
			name:          "chunk interval without size",
			chunkSize:     0,
			chunkInterval: "1h",
			expectedSize:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := NewStreamQueryRequest(
				"testdb", "testcoll",
				"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
				[]string{"device1"}, []string{"field1"},
				0, "1h", "sum", "none", 0,
				"none", 3.0, "",
				tt.chunkSize, tt.chunkInterval,
			)

			assert.Equal(t, tt.expectedSize, req.ChunkSize)
			assert.Equal(t, tt.chunkInterval, req.ChunkInterval)
		})
	}
}

func TestStreamQueryRequest_Validate(t *testing.T) {
	tests := []struct {
		name          string
		database      string
		collection    string
		startTime     string
		endTime       string
		chunkSize     int
		chunkInterval string
		limit         int64
		expectError   bool
		errorContains string
	}{
		{
			name:        "valid request with chunk size",
			database:    "testdb",
			collection:  "testcoll",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-01-02T00:00:00Z",
			chunkSize:   1000,
			expectError: false,
		},
		{
			name:          "valid request with chunk interval",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkInterval: "1h",
			expectError:   false,
		},
		{
			name:          "invalid both chunk_size and chunk_interval",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     1000,
			chunkInterval: "1h",
			expectError:   true,
			errorContains: "mutually exclusive",
		},
		{
			name:          "invalid chunk_size too small",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     5,
			expectError:   true,
			errorContains: "at least 10",
		},
		{
			name:          "invalid chunk_size too large",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     20000,
			expectError:   true,
			errorContains: "cannot exceed 10000",
		},
		{
			name:          "invalid limit not supported",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     1000,
			limit:         100,
			expectError:   true,
			errorContains: "limit is not supported",
		},
		{
			name:          "invalid chunk_interval",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkInterval: "invalid",
			expectError:   true,
			errorContains: "chunk_interval must be one of",
		},
		{
			name:          "missing database",
			database:      "",
			collection:    "testcoll",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     1000,
			expectError:   true,
			errorContains: "database and collection are required",
		},
		{
			name:          "missing start_time",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     1000,
			expectError:   true,
			errorContains: "start_time and end_time are required",
		},
		{
			name:          "invalid start_time format",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-01",
			endTime:       "2026-01-02T00:00:00Z",
			chunkSize:     1000,
			expectError:   true,
			errorContains: "RFC3339 format",
		},
		{
			name:          "end_time before start_time",
			database:      "testdb",
			collection:    "testcoll",
			startTime:     "2026-01-02T00:00:00Z",
			endTime:       "2026-01-01T00:00:00Z",
			chunkSize:     1000,
			expectError:   true,
			errorContains: "end_time must be after start_time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := NewStreamQueryRequest(
				tt.database, tt.collection,
				tt.startTime, tt.endTime,
				[]string{"device1"}, []string{"field1"},
				tt.limit, "1h", "sum", "none", 0,
				"none", 3.0, "",
				tt.chunkSize, tt.chunkInterval,
			)

			err := req.Validate()

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					fiberErr, ok := err.(*fiber.Error)
					assert.True(t, ok)
					assert.Contains(t, fiberErr.Message, tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStreamQueryRequest_GetChunkDuration(t *testing.T) {
	tests := []struct {
		interval string
		expected time.Duration
	}{
		{"5m", 5 * time.Minute},
		{"15m", 15 * time.Minute},
		{"30m", 30 * time.Minute},
		{"1h", 1 * time.Hour},
		{"6h", 6 * time.Hour},
		{"12h", 12 * time.Hour},
		{"1d", 24 * time.Hour},
		{"invalid", 0},
	}

	for _, tt := range tests {
		t.Run(tt.interval, func(t *testing.T) {
			req := NewStreamQueryRequest(
				"testdb", "testcoll",
				"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
				nil, nil, 0, "1h", "sum", "none", 0,
				"none", 3.0, "",
				0, tt.interval,
			)

			duration := req.GetChunkDuration()
			assert.Equal(t, tt.expected, duration)
		})
	}
}

func TestStreamQueryRequest_RelaxedTimeRangeLimits(t *testing.T) {
	tests := []struct {
		name          string
		interval      string
		startTime     string
		endTime       string
		expectError   bool
		errorContains string
	}{
		// Raw data (no interval) - max 30 days for streaming
		{
			name:        "raw data 15 days - within limit",
			interval:    "",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-01-16T00:00:00Z",
			expectError: false,
		},
		{
			name:        "raw data 29 days - within limit",
			interval:    "",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-01-30T00:00:00Z",
			expectError: false,
		},
		{
			name:          "raw data 31 days - exceeds limit",
			interval:      "",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-02-01T00:00:00Z",
			expectError:   true,
			errorContains: "cannot exceed 30 days",
		},
		// 1m interval - max 30 days for streaming
		{
			name:        "1m interval 20 days - within limit",
			interval:    "1m",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-01-21T00:00:00Z",
			expectError: false,
		},
		{
			name:          "1m interval 35 days - exceeds limit",
			interval:      "1m",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-02-05T00:00:00Z",
			expectError:   true,
			errorContains: "cannot exceed 30 days",
		},
		// 1h interval - max 90 days for streaming
		{
			name:        "1h interval 60 days - within limit",
			interval:    "1h",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-03-02T00:00:00Z",
			expectError: false,
		},
		{
			name:        "1h interval 89 days - within limit",
			interval:    "1h",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-03-31T00:00:00Z",
			expectError: false,
		},
		{
			name:          "1h interval 100 days - exceeds limit",
			interval:      "1h",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2026-04-11T00:00:00Z",
			expectError:   true,
			errorContains: "cannot exceed 90 days",
		},
		// 1d interval - max 1 year for streaming
		{
			name:        "1d interval 6 months - within limit",
			interval:    "1d",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-07-01T00:00:00Z",
			expectError: false,
		},
		{
			name:        "1d interval 11 months - within limit",
			interval:    "1d",
			startTime:   "2026-01-01T00:00:00Z",
			endTime:     "2026-12-01T00:00:00Z",
			expectError: false,
		},
		{
			name:          "1d interval 400 days - exceeds limit",
			interval:      "1d",
			startTime:     "2026-01-01T00:00:00Z",
			endTime:       "2027-02-05T00:00:00Z",
			expectError:   true,
			errorContains: "cannot exceed 1 year",
		},
		// 1mo interval - max 10 years for streaming
		{
			name:        "1mo interval 5 years - within limit",
			interval:    "1mo",
			startTime:   "2021-01-01T00:00:00Z",
			endTime:     "2026-01-01T00:00:00Z",
			expectError: false,
		},
		{
			name:          "1mo interval 11 years - exceeds limit",
			interval:      "1mo",
			startTime:     "2015-01-01T00:00:00Z",
			endTime:       "2026-01-01T00:00:00Z",
			expectError:   true,
			errorContains: "cannot exceed 10 years",
		},
		// 1y interval - no limit
		{
			name:        "1y interval 20 years - no limit",
			interval:    "1y",
			startTime:   "2006-01-01T00:00:00Z",
			endTime:     "2026-01-01T00:00:00Z",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := NewStreamQueryRequest(
				"testdb", "testcoll",
				tt.startTime, tt.endTime,
				[]string{"device1"}, []string{"field1"},
				0, tt.interval, "sum", "none", 0,
				"none", 3.0, "",
				1000, "",
			)

			err := req.Validate()

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					fiberErr, ok := err.(*fiber.Error)
					assert.True(t, ok, "expected fiber.Error")
					assert.Contains(t, fiberErr.Message, tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStreamQueryRequest_ValidateAggregation(t *testing.T) {
	tests := []struct {
		aggregation   string
		expectError   bool
		errorContains string
	}{
		{"sum", false, ""},
		{"avg", false, ""},
		{"min", false, ""},
		{"max", false, ""},
		{"count", false, ""},
		{"invalid", true, "aggregation must be one of"},
	}

	for _, tt := range tests {
		t.Run(tt.aggregation, func(t *testing.T) {
			req := NewStreamQueryRequest(
				"testdb", "testcoll",
				"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
				[]string{"device1"}, nil, 0, "1h", tt.aggregation, "none", 0,
				"none", 3.0, "",
				1000, "",
			)

			err := req.Validate()

			if tt.expectError {
				assert.Error(t, err)
				fiberErr, ok := err.(*fiber.Error)
				assert.True(t, ok)
				assert.Contains(t, fiberErr.Message, tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStreamQueryRequest_ValidateDownsampling(t *testing.T) {
	tests := []struct {
		downsampling  string
		expectError   bool
		errorContains string
	}{
		{"none", false, ""},
		{"auto", false, ""},
		{"lttb", false, ""},
		{"minmax", false, ""},
		{"avg", false, ""},
		{"m4", false, ""},
		{"invalid", true, "downsampling must be one of"},
	}

	for _, tt := range tests {
		t.Run(tt.downsampling, func(t *testing.T) {
			req := NewStreamQueryRequest(
				"testdb", "testcoll",
				"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
				[]string{"device1"}, nil, 0, "1h", "sum", tt.downsampling, 0,
				"none", 3.0, "",
				1000, "",
			)

			err := req.Validate()

			if tt.expectError {
				assert.Error(t, err)
				fiberErr, ok := err.(*fiber.Error)
				assert.True(t, ok)
				assert.Contains(t, fiberErr.Message, tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStreamQueryRequest_ValidateInterval(t *testing.T) {
	tests := []struct {
		interval      string
		expectError   bool
		errorContains string
	}{
		{"", false, ""}, // raw data
		{"1m", false, ""},
		{"5m", false, ""},
		{"1h", false, ""},
		{"1d", false, ""},
		{"1mo", false, ""},
		{"1y", false, ""},
		{"2h", true, "interval must be one of"},
		{"invalid", true, "interval must be one of"},
	}

	for _, tt := range tests {
		t.Run("interval_"+tt.interval, func(t *testing.T) {
			req := NewStreamQueryRequest(
				"testdb", "testcoll",
				"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
				[]string{"device1"}, nil, 0, tt.interval, "sum", "none", 0,
				"none", 3.0, "",
				1000, "",
			)

			err := req.Validate()

			if tt.expectError {
				assert.Error(t, err)
				fiberErr, ok := err.(*fiber.Error)
				assert.True(t, ok)
				assert.Contains(t, fiberErr.Message, tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
