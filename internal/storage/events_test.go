package storage

import (
	"testing"
	"time"
)

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

func TestWriteNotification(t *testing.T) {
	now := time.Now()
	notif := WriteNotification{
		Database:   "testdb",
		Date:       now,
		Collection: "metrics",
		EntryCount: 100,
		Immediate:  true,
	}

	if notif.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", notif.Database, "testdb")
	}
	if !notif.Date.Equal(now) {
		t.Errorf("Date = %v, expected %v", notif.Date, now)
	}
	if notif.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", notif.Collection, "metrics")
	}
	if notif.EntryCount != 100 {
		t.Errorf("EntryCount = %d, expected 100", notif.EntryCount)
	}
	if !notif.Immediate {
		t.Error("Immediate = false, expected true")
	}
}

func TestWriteNotification_NonImmediate(t *testing.T) {
	notif := WriteNotification{
		Database:   "testdb",
		Date:       time.Now(),
		Collection: "logs",
		EntryCount: 50,
		Immediate:  false,
	}

	if notif.Immediate {
		t.Error("Immediate = true, expected false")
	}
	if notif.EntryCount != 50 {
		t.Errorf("EntryCount = %d, expected 50", notif.EntryCount)
	}
}
