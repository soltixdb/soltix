package aggregation

import (
	"testing"
	"time"
)

func TestPipelineLevel_String(t *testing.T) {
	tests := []struct {
		name     string
		level    PipelineLevel
		expected string
	}{
		{"hourly", PipelineLevelHourly, "hourly"},
		{"daily", PipelineLevelDaily, "daily"},
		{"monthly", PipelineLevelMonthly, "monthly"},
		{"yearly", PipelineLevelYearly, "yearly"},
		{"unknown", PipelineLevel(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.level.String()
			if result != tt.expected {
				t.Errorf("PipelineLevel.String() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestPipelineLevel_ToStorageLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    PipelineLevel
		expected AggregationLevel
	}{
		{"hourly to 1h", PipelineLevelHourly, AggregationHourly},
		{"daily to 1d", PipelineLevelDaily, AggregationDaily},
		{"monthly to 1M", PipelineLevelMonthly, AggregationMonthly},
		{"yearly to 1y", PipelineLevelYearly, AggregationYearly},
		{"unknown defaults to 1h", PipelineLevel(999), AggregationHourly},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.level.ToStorageLevel()
			if result != tt.expected {
				t.Errorf("PipelineLevel.ToStorageLevel() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestPartitionKey(t *testing.T) {
	tests := []struct {
		name     string
		database string
		date     time.Time
		expected string
	}{
		{
			name:     "basic partition key",
			database: "testdb",
			date:     time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected: "testdb:2024-05-15",
		},
		{
			name:     "different database",
			database: "mydb",
			date:     time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
			expected: "mydb:2025-12-31",
		},
		{
			name:     "early date",
			database: "logs",
			date:     time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "logs:2000-01-01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PartitionKey(tt.database, tt.date)
			if result != tt.expected {
				t.Errorf("PartitionKey() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestCollectionPartitionKey(t *testing.T) {
	tests := []struct {
		name       string
		database   string
		collection string
		date       time.Time
		expected   string
	}{
		{
			name:       "basic collection partition key",
			database:   "testdb",
			collection: "metrics",
			date:       time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected:   "testdb:metrics:2024-05-15",
		},
		{
			name:       "different collection",
			database:   "prod",
			collection: "sensors",
			date:       time.Date(2025, 6, 20, 0, 0, 0, 0, time.UTC),
			expected:   "prod:sensors:2025-06-20",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CollectionPartitionKey(tt.database, tt.collection, tt.date)
			if result != tt.expected {
				t.Errorf("CollectionPartitionKey() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestHourPartitionKey(t *testing.T) {
	tests := []struct {
		name       string
		database   string
		collection string
		hour       time.Time
		expected   string
	}{
		{
			name:       "morning hour",
			database:   "testdb",
			collection: "metrics",
			hour:       time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC),
			expected:   "testdb:metrics:2024-05-15T10",
		},
		{
			name:       "midnight hour",
			database:   "prod",
			collection: "sensors",
			hour:       time.Date(2025, 6, 20, 0, 0, 0, 0, time.UTC),
			expected:   "prod:sensors:2025-06-20T00",
		},
		{
			name:       "end of day hour",
			database:   "logs",
			collection: "events",
			hour:       time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			expected:   "logs:events:2024-12-31T23",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HourPartitionKey(tt.database, tt.collection, tt.hour)
			if result != tt.expected {
				t.Errorf("HourPartitionKey() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestFlushCompleteEvent(t *testing.T) {
	now := time.Now()
	event := FlushCompleteEvent{
		Database:    "testdb",
		Date:        now,
		Collections: []string{"col1", "col2"},
		PointCount:  100,
		FlushTime:   now,
	}

	if event.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", event.Database, "testdb")
	}
	if !event.Date.Equal(now) {
		t.Errorf("Date = %v, expected %v", event.Date, now)
	}
	if len(event.Collections) != 2 {
		t.Errorf("Collections length = %d, expected 2", len(event.Collections))
	}
	if event.PointCount != 100 {
		t.Errorf("PointCount = %d, expected 100", event.PointCount)
	}
}

func TestAggregateCompleteEvent(t *testing.T) {
	now := time.Now()
	event := AggregateCompleteEvent{
		Database:   "testdb",
		Collection: "metrics",
		Level:      PipelineLevelHourly,
		TimeKey:    now,
		DeviceIDs:  []string{"dev1", "dev2", "dev3"},
	}

	if event.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", event.Database, "testdb")
	}
	if event.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", event.Collection, "metrics")
	}
	if event.Level != PipelineLevelHourly {
		t.Errorf("Level = %v, expected %v", event.Level, PipelineLevelHourly)
	}
	if !event.TimeKey.Equal(now) {
		t.Errorf("TimeKey = %v, expected %v", event.TimeKey, now)
	}
	if len(event.DeviceIDs) != 3 {
		t.Errorf("DeviceIDs length = %d, expected 3", len(event.DeviceIDs))
	}
}
