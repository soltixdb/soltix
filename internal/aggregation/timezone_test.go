package aggregation

import (
	"fmt"
	"testing"
	"time"
)

// TestTruncateWithTimezone tests that truncation works correctly with timezone
func TestTruncateWithTimezone(t *testing.T) {
	loc := time.FixedZone("JST", 9*60*60)

	tests := []struct {
		name     string
		input    time.Time
		level    AggregationLevel
		expected time.Time
	}{
		{
			name:     "Daily 00:00 JST",
			input:    time.Date(2026, 1, 13, 0, 0, 0, 0, loc),
			level:    AggregationDaily,
			expected: time.Date(2026, 1, 13, 0, 0, 0, 0, loc),
		},
		{
			name:     "Daily 09:00 JST",
			input:    time.Date(2026, 1, 13, 9, 0, 0, 0, loc),
			level:    AggregationDaily,
			expected: time.Date(2026, 1, 13, 0, 0, 0, 0, loc),
		},
		{
			name:     "Daily 23:59 JST",
			input:    time.Date(2026, 1, 13, 23, 59, 59, 0, loc),
			level:    AggregationDaily,
			expected: time.Date(2026, 1, 13, 0, 0, 0, 0, loc),
		},
		{
			name:     "Monthly first day",
			input:    time.Date(2026, 1, 1, 0, 0, 0, 0, loc),
			level:    AggregationMonthly,
			expected: time.Date(2026, 1, 1, 0, 0, 0, 0, loc),
		},
		{
			name:     "Monthly mid month",
			input:    time.Date(2026, 1, 15, 9, 0, 0, 0, loc),
			level:    AggregationMonthly,
			expected: time.Date(2026, 1, 1, 0, 0, 0, 0, loc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a dummy data point
			point := &RawDataPoint{
				Time:   tt.input,
				Fields: map[string]interface{}{"test": 100.0},
			}

			// Aggregate
			result, err := AggregateRawDataPoints("device-001", []*RawDataPoint{point}, tt.level)
			if err != nil {
				t.Fatalf("AggregateRawDataPoints failed: %v", err)
			}

			if !result.Time.Equal(tt.expected) {
				t.Errorf("Expected time %s, got %s",
					tt.expected.Format("2006-01-02 15:04:05 MST"),
					result.Time.Format("2006-01-02 15:04:05 MST"))
			}
		})
	}
}

// TestAggregatedPointsWithTimezone tests aggregating already aggregated points
func TestAggregatedPointsWithTimezone(t *testing.T) {
	loc := time.FixedZone("JST", 9*60*60)

	// Create hourly points at different times
	hour1 := &AggregatedPoint{
		Time:     time.Date(2026, 1, 13, 0, 0, 0, 0, loc),
		DeviceID: "device-001",
		Level:    AggregationHourly,
		Fields: map[string]*AggregatedField{
			"test": {Count: 60, Sum: 6000, Min: 50, Max: 150, Avg: 100},
		},
	}

	hour2 := &AggregatedPoint{
		Time:     time.Date(2026, 1, 13, 9, 0, 0, 0, loc), // 9 AM same day
		DeviceID: "device-001",
		Level:    AggregationHourly,
		Fields: map[string]*AggregatedField{
			"test": {Count: 60, Sum: 6000, Min: 50, Max: 150, Avg: 100},
		},
	}

	// Aggregate to daily - should produce single point at 00:00
	result, err := AggregateAggregatedPoints("device-001", []*AggregatedPoint{hour1, hour2}, AggregationDaily)
	if err != nil {
		t.Fatalf("AggregateAggregatedPoints failed: %v", err)
	}

	expected := time.Date(2026, 1, 13, 0, 0, 0, 0, loc)
	if !result.Time.Equal(expected) {
		t.Errorf("Expected daily time %s, got %s",
			expected.Format("2006-01-02 15:04:05 MST"),
			result.Time.Format("2006-01-02 15:04:05 MST"))
	}

	// Check merged counts
	if result.Fields["test"].Count != 120 {
		t.Errorf("Expected count 120, got %d", result.Fields["test"].Count)
	}

	fmt.Printf("✓ Both hourly points (00:00 and 09:00) correctly merged into single daily point at %s\n",
		result.Time.Format("2006-01-02 15:04:05 MST"))
}
