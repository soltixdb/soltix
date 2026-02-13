package grpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
)

// TestStorageServiceHandler_QueryAggregatedData tests the main aggregated query function
func TestStorageServiceHandler_QueryAggregatedData(t *testing.T) {
	logger := logging.NewDevelopment()

	// Create temporary directory for aggregate storage
	tmpDir, err := os.MkdirTemp("", "aggregate_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create aggregate storage with test data
	aggStorage := aggregation.NewStorage(tmpDir, logger)

	// Prepare test data - hourly aggregates
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	hourlyPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     startTime,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 25.0, Avg: 25.0, Min: 25.0, Max: 25.0, Count: 1},
			},
		},
		{
			DeviceID: "device2",
			Time:     startTime.Add(1 * time.Hour),
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 30.0, Avg: 30.0, Min: 30.0, Max: 30.0, Count: 1},
			},
		},
	}

	if err := aggStorage.WriteHourly("testdb", "testcol", hourlyPoints); err != nil {
		t.Fatalf("Failed to write hourly aggregates: %v", err)
	}

	// Create handler with aggregate storage
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	handler := &StorageServiceHandler{
		memoryStore:      memStore,
		aggregateStorage: aggStorage,
		logger:           logger,
	}

	tests := []struct {
		name        string
		request     *pb.QueryShardRequest
		expectError bool
		errorCode   string
	}{
		{
			name: "hourly_aggregation_sum",
			request: &pb.QueryShardRequest{
				ShardId:     1,
				Database:    "testdb",
				Collection:  "testcol",
				StartTime:   startTime.Unix(),
				EndTime:     startTime.Add(2 * time.Hour).Unix(),
				Interval:    "1h",
				Aggregation: "sum",
			},
			expectError: false,
		},
		{
			name: "hourly_aggregation_avg",
			request: &pb.QueryShardRequest{
				ShardId:     1,
				Database:    "testdb",
				Collection:  "testcol",
				StartTime:   startTime.Unix(),
				EndTime:     startTime.Add(2 * time.Hour).Unix(),
				Interval:    "1h",
				Aggregation: "avg",
			},
			expectError: false,
		},
		{
			name: "invalid_interval",
			request: &pb.QueryShardRequest{
				ShardId:    1,
				Database:   "testdb",
				Collection: "testcol",
				StartTime:  startTime.Unix(),
				EndTime:    startTime.Add(2 * time.Hour).Unix(),
				Interval:   "5m", // Should trigger aggregated query but invalid for it
			},
			expectError: true,
			errorCode:   "InvalidArgument",
		},
		{
			name: "with_device_filter",
			request: &pb.QueryShardRequest{
				ShardId:     1,
				Database:    "testdb",
				Collection:  "testcol",
				StartTime:   startTime.Unix(),
				EndTime:     startTime.Add(2 * time.Hour).Unix(),
				Interval:    "1h",
				Aggregation: "sum",
				Ids:         []string{"device1"},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			resp, err := handler.queryAggregatedData(ctx, tt.request,
				time.Unix(tt.request.StartTime, 0), time.Unix(tt.request.EndTime, 0))

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if resp == nil {
					t.Errorf("Expected response but got nil")
				} else {
					if !resp.Success {
						t.Errorf("Expected success=true but got false")
					}
					if resp.ShardId != tt.request.ShardId {
						t.Errorf("Expected ShardId=%d but got %d", tt.request.ShardId, resp.ShardId)
					}
				}
			}
		})
	}
}

// TestStorageServiceHandler_QueryHourlyAggregates tests hourly aggregate queries
func TestStorageServiceHandler_QueryHourlyAggregates(t *testing.T) {
	logger := logging.NewDevelopment()

	tmpDir, err := os.MkdirTemp("", "hourly_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	aggStorage := aggregation.NewStorage(tmpDir, logger)

	// Create test data spanning multiple days
	day1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	hourlyPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     day1,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 25.0, Avg: 25.0, Min: 25.0, Max: 25.0, Count: 1},
			},
		},
		{
			DeviceID: "device1",
			Time:     day1.Add(1 * time.Hour),
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 26.0, Avg: 26.0, Min: 26.0, Max: 26.0, Count: 1},
			},
		},
		{
			DeviceID: "device1",
			Time:     day2,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 27.0, Avg: 27.0, Min: 27.0, Max: 27.0, Count: 1},
			},
		},
	}

	if err := aggStorage.WriteHourly("testdb", "testcol", hourlyPoints); err != nil {
		t.Fatalf("Failed to write hourly aggregates: %v", err)
	}

	handler := &StorageServiceHandler{
		aggregateStorage: aggStorage,
		logger:           logger,
	}

	tests := []struct {
		name       string
		database   string
		collection string
		startTime  time.Time
		endTime    time.Time
		wantCount  int
	}{
		{
			name:       "single_day",
			database:   "testdb",
			collection: "testcol",
			startTime:  day1,
			endTime:    day1.Add(23 * time.Hour),
			wantCount:  2,
		},
		{
			name:       "multiple_days",
			database:   "testdb",
			collection: "testcol",
			startTime:  day1,
			endTime:    day2.Add(23 * time.Hour),
			wantCount:  3,
		},
		{
			name:       "partial_day",
			database:   "testdb",
			collection: "testcol",
			startTime:  day1,
			endTime:    day1.Add(30 * time.Minute),
			wantCount:  1,
		},
		{
			name:       "nonexistent_collection",
			database:   "testdb",
			collection: "nonexistent",
			startTime:  day1,
			endTime:    day1.Add(23 * time.Hour),
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := handler.queryHourlyAggregates(tt.database, tt.collection, tt.startTime, tt.endTime)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if len(points) != tt.wantCount {
				t.Errorf("Expected %d points but got %d", tt.wantCount, len(points))
			}
		})
	}
}

// TestStorageServiceHandler_QueryDailyAggregates tests daily aggregate queries
func TestStorageServiceHandler_QueryDailyAggregates(t *testing.T) {
	logger := logging.NewDevelopment()

	tmpDir, err := os.MkdirTemp("", "daily_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	aggStorage := aggregation.NewStorage(tmpDir, logger)

	// Create test data spanning multiple months
	jan1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	jan15 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	feb1 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	dailyPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     jan1,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 300.0, Avg: 25.0, Min: 20.0, Max: 30.0, Count: 12},
			},
		},
		{
			DeviceID: "device1",
			Time:     jan15,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 360.0, Avg: 30.0, Min: 25.0, Max: 35.0, Count: 12},
			},
		},
		{
			DeviceID: "device1",
			Time:     feb1,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 240.0, Avg: 20.0, Min: 15.0, Max: 25.0, Count: 12},
			},
		},
	}

	if err := aggStorage.WriteDaily("testdb", "testcol", dailyPoints); err != nil {
		t.Fatalf("Failed to write daily aggregates: %v", err)
	}

	handler := &StorageServiceHandler{
		aggregateStorage: aggStorage,
		logger:           logger,
	}

	tests := []struct {
		name       string
		database   string
		collection string
		startTime  time.Time
		endTime    time.Time
		wantCount  int
	}{
		{
			name:       "single_month",
			database:   "testdb",
			collection: "testcol",
			startTime:  jan1,
			endTime:    jan1.AddDate(0, 1, -1),
			wantCount:  2,
		},
		{
			name:       "multiple_months",
			database:   "testdb",
			collection: "testcol",
			startTime:  jan1,
			endTime:    feb1.AddDate(0, 0, 7),
			wantCount:  3,
		},
		{
			name:       "partial_month",
			database:   "testdb",
			collection: "testcol",
			startTime:  jan1,
			endTime:    jan1.AddDate(0, 0, 10),
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := handler.queryDailyAggregates(tt.database, tt.collection, tt.startTime, tt.endTime)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if len(points) != tt.wantCount {
				t.Errorf("Expected %d points but got %d", tt.wantCount, len(points))
			}
		})
	}
}

// TestStorageServiceHandler_QueryMonthlyAggregates tests monthly aggregate queries
func TestStorageServiceHandler_QueryMonthlyAggregates(t *testing.T) {
	logger := logging.NewDevelopment()

	tmpDir, err := os.MkdirTemp("", "monthly_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	aggStorage := aggregation.NewStorage(tmpDir, logger)

	// Create test data spanning multiple years
	jan2024 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	jun2024 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	jan2025 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	monthlyPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     jan2024,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 7500.0, Avg: 25.0, Min: 20.0, Max: 30.0, Count: 300},
			},
		},
		{
			DeviceID: "device1",
			Time:     jun2024,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 9000.0, Avg: 30.0, Min: 25.0, Max: 35.0, Count: 300},
			},
		},
		{
			DeviceID: "device1",
			Time:     jan2025,
			Fields: map[string]*aggregation.AggregatedField{
				"temp": {Sum: 6000.0, Avg: 20.0, Min: 15.0, Max: 25.0, Count: 300},
			},
		},
	}

	if err := aggStorage.WriteMonthly("testdb", "testcol", monthlyPoints); err != nil {
		t.Fatalf("Failed to write monthly aggregates: %v", err)
	}

	handler := &StorageServiceHandler{
		aggregateStorage: aggStorage,
		logger:           logger,
	}

	tests := []struct {
		name       string
		database   string
		collection string
		startTime  time.Time
		endTime    time.Time
		wantCount  int
	}{
		{
			name:       "single_year",
			database:   "testdb",
			collection: "testcol",
			startTime:  jan2024,
			endTime:    jan2024.AddDate(1, 0, -1),
			wantCount:  2,
		},
		{
			name:       "multiple_years",
			database:   "testdb",
			collection: "testcol",
			startTime:  jan2024,
			endTime:    jan2025.AddDate(0, 1, 0),
			wantCount:  3,
		},
		{
			name:       "partial_year",
			database:   "testdb",
			collection: "testcol",
			startTime:  jan2024,
			endTime:    jan2024.AddDate(0, 3, 0),
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := handler.queryMonthlyAggregates(tt.database, tt.collection, tt.startTime, tt.endTime)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if len(points) != tt.wantCount {
				t.Errorf("Expected %d points but got %d", tt.wantCount, len(points))
			}
		})
	}
}

// TestStorageServiceHandler_QueryYearlyAggregates tests yearly aggregate queries
func TestStorageServiceHandler_QueryYearlyAggregates(t *testing.T) {
	logger := logging.NewDevelopment()

	tmpDir, err := os.MkdirTemp("", "yearly_agg_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	aggStorage := aggregation.NewStorage(tmpDir, logger)

	handler := &StorageServiceHandler{
		logger:           logger,
		aggregateStorage: aggStorage,
	}

	// Test the implementation
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	points, err := handler.queryYearlyAggregates("testdb", "testcol", startTime, endTime)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// No data written, so should return empty
	if len(points) != 0 {
		t.Errorf("Expected 0 points but got %d", len(points))
	}
}

// TestStorageServiceHandler_AggregateIntegration tests full integration of aggregate queries
func TestStorageServiceHandler_AggregateIntegration(t *testing.T) {
	logger := logging.NewDevelopment()

	tmpDir, err := os.MkdirTemp("", "integration_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	aggStorage := aggregation.NewStorage(tmpDir, logger)

	// Create comprehensive test data
	startTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	hourlyPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     startTime,
			Fields: map[string]*aggregation.AggregatedField{
				"temp":     {Sum: 25.0, Avg: 25.0, Min: 25.0, Max: 25.0, Count: 1},
				"humidity": {Sum: 60.0, Avg: 60.0, Min: 60.0, Max: 60.0, Count: 1},
			},
		},
		{
			DeviceID: "device2",
			Time:     startTime.Add(1 * time.Hour),
			Fields: map[string]*aggregation.AggregatedField{
				"temp":     {Sum: 30.0, Avg: 30.0, Min: 30.0, Max: 30.0, Count: 1},
				"humidity": {Sum: 65.0, Avg: 65.0, Min: 65.0, Max: 65.0, Count: 1},
			},
		},
	}

	if err := aggStorage.WriteHourly("testdb", "testcol", hourlyPoints); err != nil {
		t.Fatalf("Failed to write hourly aggregates: %v", err)
	}

	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	handler := &StorageServiceHandler{
		memoryStore:      memStore,
		aggregateStorage: aggStorage,
		logger:           logger,
	}

	// Test different aggregation types
	tests := []struct {
		name        string
		aggregation string
		fields      []string
		expectCount int
	}{
		{
			name:        "sum_all_fields",
			aggregation: "sum",
			fields:      nil,
			expectCount: 2, // 2 devices
		},
		{
			name:        "avg_specific_field",
			aggregation: "avg",
			fields:      []string{"temp"},
			expectCount: 2,
		},
		{
			name:        "min_multiple_fields",
			aggregation: "min",
			fields:      []string{"temp", "humidity"},
			expectCount: 2,
		},
		{
			name:        "max_all_fields",
			aggregation: "max",
			fields:      nil,
			expectCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.QueryShardRequest{
				ShardId:     1,
				Database:    "testdb",
				Collection:  "testcol",
				StartTime:   startTime.Unix(),
				EndTime:     startTime.Add(3 * time.Hour).Unix(),
				Interval:    "1h",
				Aggregation: tt.aggregation,
				Fields:      tt.fields,
			}

			resp, err := handler.queryAggregatedData(context.Background(), req,
				time.Unix(req.StartTime, 0), time.Unix(req.EndTime, 0))
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if !resp.Success {
				t.Errorf("Expected success=true but got false")
			}

			if len(resp.Results) != tt.expectCount {
				t.Errorf("Expected %d device results but got %d", tt.expectCount, len(resp.Results))
			}

			// Verify each device has data points
			for _, result := range resp.Results {
				if len(result.DataPoints) == 0 {
					t.Errorf("Device %s has no data points", result.Id)
				}
			}
		})
	}
}
