package aggregation

import (
	"os"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestStorage_WriteHourly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
		{
			Time:     now.Add(time.Hour),
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 260.0, Avg: 26.0, Min: 21.0, Max: 31.0},
			},
		},
	}

	err = storage.WriteHourly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}
}

func TestStorage_WriteHourly_Empty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	err = storage.WriteHourly("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteHourly with empty points failed: %v", err)
	}
}

func TestStorage_WriteHourly_MultiplesDays(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Points spanning multiple days
	day1 := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	day2 := time.Date(2024, 5, 16, 10, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     day1,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
		{
			Time:     day2,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 260.0, Avg: 26.0, Min: 21.0, Max: 31.0},
			},
		},
	}

	err = storage.WriteHourly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}

	// Verify both days have partitions
	partitions, err := storage.ListPartitions(AggregationHourly, "testdb", "metrics")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}

	if len(partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(partitions))
	}
}

func TestStorage_WriteDaily(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationDaily,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 240, Sum: 6000.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
	}

	err = storage.WriteDaily("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteDaily failed: %v", err)
	}
}

func TestStorage_WriteDaily_Empty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	err = storage.WriteDaily("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteDaily with empty points failed: %v", err)
	}
}

func TestStorage_WriteDaily_MultipleMonths(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Points spanning multiple months
	month1 := time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC)
	month2 := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     month1,
			DeviceID: "device1",
			Level:    AggregationDaily,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 240, Sum: 6000.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
		{
			Time:     month2,
			DeviceID: "device1",
			Level:    AggregationDaily,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 240, Sum: 6240.0, Avg: 26.0, Min: 21.0, Max: 31.0},
			},
		},
	}

	err = storage.WriteDaily("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteDaily failed: %v", err)
	}
}

func TestStorage_WriteMonthly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationMonthly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 7200, Sum: 180000.0, Avg: 25.0, Min: 15.0, Max: 35.0},
			},
		},
	}

	err = storage.WriteMonthly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteMonthly failed: %v", err)
	}
}

func TestStorage_WriteMonthly_Empty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	err = storage.WriteMonthly("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteMonthly with empty points failed: %v", err)
	}
}

func TestStorage_WriteYearly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationYearly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 87600, Sum: 2190000.0, Avg: 25.0, Min: 10.0, Max: 40.0},
			},
		},
	}

	err = storage.WriteYearly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteYearly failed: %v", err)
	}
}

func TestStorage_WriteYearly_Empty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	err = storage.WriteYearly("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteYearly with empty points failed: %v", err)
	}
}

func TestStorage_ReadHourly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)

	// Write points
	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
	}

	err = storage.WriteHourly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}

	// Read points
	readPoints, err := storage.ReadHourly("testdb", "metrics", "20240515")
	if err != nil {
		t.Fatalf("ReadHourly failed: %v", err)
	}

	if len(readPoints) != 1 {
		t.Errorf("Expected 1 point, got %d", len(readPoints))
	}
}

func TestStorage_ReadHourly_InvalidDate(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Try to read with invalid date format
	_, err = storage.ReadHourly("testdb", "metrics", "invalid")
	if err == nil {
		t.Error("Expected error for invalid date format, got nil")
	}
}

func TestStorage_ReadHourlyForDay(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	day := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)

	// Write points for multiple hours
	points := []*AggregatedPoint{
		{
			Time:     day,
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 250.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
		{
			Time:     day.Add(time.Hour),
			DeviceID: "device1",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 10, Sum: 260.0, Avg: 26.0, Min: 21.0, Max: 31.0},
			},
		},
	}

	err = storage.WriteHourly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}

	// Read for day
	readPoints, err := storage.ReadHourlyForDay("testdb", "metrics", day)
	if err != nil {
		t.Fatalf("ReadHourlyForDay failed: %v", err)
	}

	if len(readPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(readPoints))
	}
}

func TestStorage_ReadDailyForMonth(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	month := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)

	// Write points for multiple days
	points := []*AggregatedPoint{
		{
			Time:     month,
			DeviceID: "device1",
			Level:    AggregationDaily,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 240, Sum: 6000.0, Avg: 25.0, Min: 20.0, Max: 30.0},
			},
		},
		{
			Time:     month.AddDate(0, 0, 1),
			DeviceID: "device1",
			Level:    AggregationDaily,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 240, Sum: 6240.0, Avg: 26.0, Min: 21.0, Max: 31.0},
			},
		},
	}

	err = storage.WriteDaily("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteDaily failed: %v", err)
	}

	// Read for month
	readPoints, err := storage.ReadDailyForMonth("testdb", "metrics", month)
	if err != nil {
		t.Fatalf("ReadDailyForMonth failed: %v", err)
	}

	if len(readPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(readPoints))
	}
}

func TestStorage_ReadMonthlyForYear(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	year := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Write points for multiple months
	points := []*AggregatedPoint{
		{
			Time:     year,
			DeviceID: "device1",
			Level:    AggregationMonthly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 7200, Sum: 180000.0, Avg: 25.0, Min: 15.0, Max: 35.0},
			},
		},
		{
			Time:     year.AddDate(0, 1, 0),
			DeviceID: "device1",
			Level:    AggregationMonthly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 7200, Sum: 187200.0, Avg: 26.0, Min: 16.0, Max: 36.0},
			},
		},
	}

	err = storage.WriteMonthly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteMonthly failed: %v", err)
	}

	// Read for year
	readPoints, err := storage.ReadMonthlyForYear("testdb", "metrics", year)
	if err != nil {
		t.Fatalf("ReadMonthlyForYear failed: %v", err)
	}

	if len(readPoints) != 2 {
		t.Errorf("Expected 2 points, got %d", len(readPoints))
	}
}

func TestStorage_ReadYearly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Write points
	points := []*AggregatedPoint{
		{
			Time:     now,
			DeviceID: "device1",
			Level:    AggregationYearly,
			Fields: map[string]*AggregatedField{
				"temperature": {Count: 87600, Sum: 2190000.0, Avg: 25.0, Min: 10.0, Max: 40.0},
			},
		},
	}

	err = storage.WriteYearly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteYearly failed: %v", err)
	}

	// Read yearly
	readPoints, err := storage.ReadYearly("testdb", "metrics")
	if err != nil {
		t.Fatalf("ReadYearly failed: %v", err)
	}

	if len(readPoints) != 1 {
		t.Errorf("Expected 1 point, got %d", len(readPoints))
	}
}

func TestStorage_SetGetTimezone(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agg_storage_compat_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	logger := logging.NewDevelopment()
	storage := NewStorage(tempDir, logger)

	// Default should be UTC
	if storage.GetTimezone() != time.UTC {
		t.Errorf("Default timezone = %v, expected UTC", storage.GetTimezone())
	}

	// Set to different timezone
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	storage.SetTimezone(tokyo)

	if storage.GetTimezone() != tokyo {
		t.Errorf("Timezone = %v, expected Asia/Tokyo", storage.GetTimezone())
	}

	// Set nil should not change
	storage.SetTimezone(nil)
	if storage.GetTimezone() != tokyo {
		t.Errorf("Timezone should remain Asia/Tokyo after setting nil, got %v", storage.GetTimezone())
	}
}
