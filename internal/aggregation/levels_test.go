package aggregation

import (
	"testing"
	"time"
)

func TestGetLevelDuration(t *testing.T) {
	tests := []struct {
		level    AggregationLevel
		expected time.Duration
	}{
		{AggregationHourly, time.Hour},
		{AggregationDaily, 24 * time.Hour},
		{AggregationMonthly, 24 * time.Hour},
		{AggregationYearly, 24 * time.Hour},
		{"invalid", time.Hour}, // Default case
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			duration := GetLevelDuration(tt.level)
			if duration != tt.expected {
				t.Errorf("Expected duration=%v for level=%s, got %v", tt.expected, tt.level, duration)
			}
		})
	}
}

func TestTruncateToMonth(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			name:     "mid-month",
			input:    time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC),
			expected: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "first day of month",
			input:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "last day of month",
			input:    time.Date(2024, 2, 29, 23, 59, 59, 999999999, time.UTC),
			expected: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "december",
			input:    time.Date(2024, 12, 25, 12, 0, 0, 0, time.UTC),
			expected: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateToMonth(tt.input)
			if !result.Equal(tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTruncateToYear(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			name:     "mid-year",
			input:    time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC),
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "first day of year",
			input:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "last day of year",
			input:    time.Date(2024, 12, 31, 23, 59, 59, 999999999, time.UTC),
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "leap year",
			input:    time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateToYear(tt.input)
			if !result.Equal(tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTruncateToMonth_PreservesLocation(t *testing.T) {
	location := time.FixedZone("TEST", 7*3600) // UTC+7
	input := time.Date(2024, 6, 15, 14, 30, 45, 0, location)

	result := TruncateToMonth(input)

	if result.Location() != location {
		t.Errorf("Expected location %v, got %v", location, result.Location())
	}

	expected := time.Date(2024, 6, 1, 0, 0, 0, 0, location)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestTruncateToYear_PreservesLocation(t *testing.T) {
	location := time.FixedZone("TEST", -5*3600) // UTC-5
	input := time.Date(2024, 6, 15, 14, 30, 45, 0, location)

	result := TruncateToYear(input)

	if result.Location() != location {
		t.Errorf("Expected location %v, got %v", location, result.Location())
	}

	expected := time.Date(2024, 1, 1, 0, 0, 0, 0, location)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestAggregationLevelConstants(t *testing.T) {
	if AggregationHourly != "1h" {
		t.Errorf("Expected AggregationHourly='1h', got '%s'", AggregationHourly)
	}
	if AggregationDaily != "1d" {
		t.Errorf("Expected AggregationDaily='1d', got '%s'", AggregationDaily)
	}
	if AggregationMonthly != "1M" {
		t.Errorf("Expected AggregationMonthly='1M', got '%s'", AggregationMonthly)
	}
	if AggregationYearly != "1y" {
		t.Errorf("Expected AggregationYearly='1y', got '%s'", AggregationYearly)
	}
}
