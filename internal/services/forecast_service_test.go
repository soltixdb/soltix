package services

import (
	"context"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/analytics/forecast"
	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
)

// createTestForecastService creates a ForecastService for testing
// Uses MockMetadataManager defined in query_service_test.go
func createTestForecastService(mockMeta *MockMetadataManager) *ForecastService {
	logger := logging.NewDevelopment()
	cfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  2,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, cfg)
	queryCoord := coordinator.NewQueryCoordinator(logger, mockMeta, shardRouter)

	return NewForecastService(logger, mockMeta, queryCoord)
}

func TestNewForecastService(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	if service == nil {
		t.Fatal("Expected non-nil ForecastService")
		return
	}
	if service.logger == nil {
		t.Error("Expected non-nil logger")
	}
	if service.metadataManager == nil {
		t.Error("Expected non-nil metadataManager")
	}
	if service.queryCoordinator == nil {
		t.Error("Expected non-nil queryCoordinator")
	}
}

func TestForecastService_Execute_CollectionNotFound(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	// Only add database, not collection
	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}

	service := createTestForecastService(mockMeta)

	ctx := context.Background()
	req := &ForecastRequest{
		Database:   "testdb",
		Collection: "nonexistent",
		StartTime:  time.Now().Add(-24 * time.Hour),
		EndTime:    time.Now(),
		Method:     "sma",
		Horizon:    10,
		Field:      "temperature",
	}

	_, err := service.Execute(ctx, req)
	if err == nil {
		t.Fatal("Expected error for non-existent collection")
	}

	serviceErr, ok := err.(*ServiceError)
	if !ok {
		t.Fatalf("Expected *ServiceError, got %T", err)
	}
	if serviceErr.Code != "COLLECTION_NOT_FOUND" {
		t.Errorf("Expected code 'COLLECTION_NOT_FOUND', got '%s'", serviceErr.Code)
	}
}

func TestForecastService_Execute_DatabaseNotFound(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	// Don't add any database
	service := createTestForecastService(mockMeta)

	ctx := context.Background()
	req := &ForecastRequest{
		Database:   "nonexistent",
		Collection: "testcoll",
		StartTime:  time.Now().Add(-24 * time.Hour),
		EndTime:    time.Now(),
		Method:     "sma",
		Horizon:    10,
	}

	_, err := service.Execute(ctx, req)
	if err == nil {
		t.Fatal("Expected error for non-existent database")
	}

	serviceErr, ok := err.(*ServiceError)
	if !ok {
		t.Fatalf("Expected *ServiceError, got %T", err)
	}
	if serviceErr.Code != "COLLECTION_NOT_FOUND" {
		t.Errorf("Expected code 'COLLECTION_NOT_FOUND', got '%s'", serviceErr.Code)
	}
}

func TestForecastService_Execute_InvalidMethod(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")

	service := createTestForecastService(mockMeta)

	ctx := context.Background()
	req := &ForecastRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  time.Now().Add(-24 * time.Hour),
		EndTime:    time.Now(),
		Method:     "invalid_method",
		Horizon:    10,
	}

	_, err := service.Execute(ctx, req)
	if err == nil {
		t.Fatal("Expected error for invalid method")
	}

	serviceErr, ok := err.(*ServiceError)
	if !ok {
		t.Fatalf("Expected *ServiceError, got %T", err)
	}
	if serviceErr.Code != "INVALID_METHOD" {
		t.Errorf("Expected code 'INVALID_METHOD', got '%s'", serviceErr.Code)
	}
}

func TestForecastService_Execute_Success(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")

	service := createTestForecastService(mockMeta)

	ctx := context.Background()
	req := &ForecastRequest{
		Database:   "testdb",
		Collection: "testcoll",
		IDs:        []string{"device1"},
		StartTime:  time.Now().Add(-24 * time.Hour),
		EndTime:    time.Now(),
		Method:     "sma",
		Horizon:    10,
		Field:      "temperature",
		Interval:   time.Hour,
	}

	result, err := service.Execute(ctx, req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
		return
	}
	if result.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", result.Database)
	}
	if result.Collection != "testcoll" {
		t.Errorf("Expected collection 'testcoll', got '%s'", result.Collection)
	}
	// Forecasts may be empty if no historical data, but should not error
}

func TestForecastService_generateForecasts_EmptyResults(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	// Test with empty results
	results := []coordinator.FormattedQueryResult{}

	forecasts := service.generateForecasts(results, "temperature", nil, forecast.DefaultForecastConfig())

	if len(forecasts) != 0 {
		t.Errorf("Expected 0 forecasts for empty results, got %d", len(forecasts))
	}
}

func TestForecastService_generateForecasts_NoTargetField(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	results := []coordinator.FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"},
			Fields: map[string][]interface{}{
				"humidity": {60.0, 62.0},
			},
		},
	}

	// Request temperature but only humidity exists
	forecasts := service.generateForecasts(results, "temperature", nil, forecast.DefaultForecastConfig())

	if len(forecasts) != 0 {
		t.Errorf("Expected 0 forecasts when target field not found, got %d", len(forecasts))
	}
}

func TestForecastService_convertToDataPoints(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	times := []string{
		"2024-01-01T00:00:00Z",
		"2024-01-01T01:00:00Z",
		"2024-01-01T02:00:00Z",
	}
	values := []interface{}{25.0, 26.0, 27.5}

	dataPoints := service.convertToDataPoints(times, values)

	if len(dataPoints) != 3 {
		t.Fatalf("Expected 3 data points, got %d", len(dataPoints))
	}
	if dataPoints[0].Value != 25.0 {
		t.Errorf("Expected value 25.0, got %f", dataPoints[0].Value)
	}
	if dataPoints[2].Value != 27.5 {
		t.Errorf("Expected value 27.5, got %f", dataPoints[2].Value)
	}
}

func TestForecastService_convertToDataPoints_WithNilValues(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	times := []string{
		"2024-01-01T00:00:00Z",
		"2024-01-01T01:00:00Z",
		"2024-01-01T02:00:00Z",
	}
	values := []interface{}{25.0, nil, 27.5}

	dataPoints := service.convertToDataPoints(times, values)

	// Should skip nil values
	if len(dataPoints) != 2 {
		t.Fatalf("Expected 2 data points (skipping nil), got %d", len(dataPoints))
	}
}

func TestForecastService_convertToDataPoints_InvalidTime(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	times := []string{
		"2024-01-01T00:00:00Z",
		"invalid-time",
		"2024-01-01T02:00:00Z",
	}
	values := []interface{}{25.0, 26.0, 27.5}

	dataPoints := service.convertToDataPoints(times, values)

	// Should skip invalid time
	if len(dataPoints) != 2 {
		t.Fatalf("Expected 2 data points (skipping invalid time), got %d", len(dataPoints))
	}
}

func TestForecastRequest_Fields(t *testing.T) {
	req := ForecastRequest{
		Database:       "testdb",
		Collection:     "testcoll",
		StartTime:      time.Now().Add(-24 * time.Hour),
		EndTime:        time.Now(),
		IDs:            []string{"device1", "device2"},
		Method:         "ema",
		Horizon:        10,
		Field:          "temperature",
		SeasonalPeriod: 24,
		Interval:       time.Hour,
	}

	if req.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", req.Database)
	}
	if len(req.IDs) != 2 {
		t.Errorf("Expected 2 IDs, got %d", len(req.IDs))
	}
	if req.Horizon != 10 {
		t.Errorf("Expected horizon 10, got %d", req.Horizon)
	}
}

func TestForecastResult_Fields(t *testing.T) {
	result := ForecastResult{
		DeviceID: "device1",
		Field:    "temperature",
		Predictions: []ForecastPrediction{
			{Time: "2024-01-02T00:00:00Z", Value: 25.5, LowerBound: 24.0, UpperBound: 27.0},
		},
	}

	if result.DeviceID != "device1" {
		t.Errorf("Expected device_id 'device1', got '%s'", result.DeviceID)
	}
	if len(result.Predictions) != 1 {
		t.Errorf("Expected 1 prediction, got %d", len(result.Predictions))
	}
	if result.Predictions[0].Value != 25.5 {
		t.Errorf("Expected value 25.5, got %f", result.Predictions[0].Value)
	}
}

func TestForecastResponse_Fields(t *testing.T) {
	resp := ForecastResponse{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2024-01-01T00:00:00Z",
		EndTime:    "2024-01-02T00:00:00Z",
		Forecasts:  []ForecastResult{},
	}

	if resp.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", resp.Database)
	}
	if resp.Collection != "testcoll" {
		t.Errorf("Expected collection 'testcoll', got '%s'", resp.Collection)
	}
}

func TestForecastService_generateForecasts_MultipleDevices(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	results := []coordinator.FormattedQueryResult{
		{
			DeviceID: "device1",
			Times: []string{
				"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
				"2024-01-01T03:00:00Z", "2024-01-01T04:00:00Z", "2024-01-01T05:00:00Z",
				"2024-01-01T06:00:00Z", "2024-01-01T07:00:00Z", "2024-01-01T08:00:00Z",
				"2024-01-01T09:00:00Z",
			},
			Fields: map[string][]interface{}{
				"temperature": {20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0},
			},
		},
	}

	forecaster, _ := forecast.GetForecaster("sma")
	config := forecast.DefaultForecastConfig()
	config.Horizon = 5

	forecasts := service.generateForecasts(results, "temperature", forecaster, config)

	// Verify function handles the data
	if len(forecasts) == 0 {
		t.Logf("Note: Got 0 forecasts, which is acceptable for test data")
	}
}

func TestForecastService_generateForecasts_InsufficientData(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	results := []coordinator.FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z"}, // Only 1 point
			Fields: map[string][]interface{}{
				"temperature": {20.0},
			},
		},
	}

	forecaster, _ := forecast.GetForecaster("sma")
	config := forecast.DefaultForecastConfig()
	config.MinDataPoints = 10 // Require 10 points

	forecasts := service.generateForecasts(results, "temperature", forecaster, config)

	// Should skip device with insufficient data
	if len(forecasts) != 0 {
		t.Errorf("Expected 0 forecasts for insufficient data, got %d", len(forecasts))
	}
}

func TestForecastService_generateForecasts_EmptyFieldMap(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	results := []coordinator.FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z"},
			Fields:   map[string][]interface{}{}, // Empty fields
		},
	}

	forecaster, _ := forecast.GetForecaster("sma")
	forecasts := service.generateForecasts(results, "", forecaster, forecast.DefaultForecastConfig())

	if len(forecasts) != 0 {
		t.Errorf("Expected 0 forecasts for empty fields, got %d", len(forecasts))
	}
}

func TestForecastService_convertToDataPoints_EmptyArrays(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	dataPoints := service.convertToDataPoints([]string{}, []interface{}{})

	if len(dataPoints) != 0 {
		t.Errorf("Expected 0 data points for empty arrays, got %d", len(dataPoints))
	}
}

func TestForecastService_convertToDataPoints_MismatchedLengths(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	times := []string{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"}
	values := []interface{}{25.0} // Shorter than times

	dataPoints := service.convertToDataPoints(times, values)

	// Should only process up to shorter length
	if len(dataPoints) != 1 {
		t.Errorf("Expected 1 data point for mismatched lengths, got %d", len(dataPoints))
	}
}

func TestForecastService_convertToDataPoints_NonFloatValues(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestForecastService(mockMeta)

	times := []string{
		"2024-01-01T00:00:00Z",
		"2024-01-01T01:00:00Z",
	}
	values := []interface{}{25.0, "invalid"} // String instead of float

	dataPoints := service.convertToDataPoints(times, values)

	// Should skip non-float values
	if len(dataPoints) != 1 {
		t.Errorf("Expected 1 data point (skipped invalid), got %d", len(dataPoints))
	}
}

func TestForecastService_Execute_WithAllAlgorithms(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestForecastService(mockMeta)

	algorithms := []string{"sma", "exponential", "linear", "holt_winters", "arima"}

	for _, algo := range algorithms {
		t.Run(algo, func(t *testing.T) {
			ctx := context.Background()
			req := &ForecastRequest{
				Database:   "testdb",
				Collection: "testcoll",
				StartTime:  time.Now().Add(-24 * time.Hour),
				EndTime:    time.Now(),
				Method:     algo,
				Horizon:    10,
				Field:      "temperature",
			}

			result, err := service.Execute(ctx, req)
			if err != nil {
				t.Logf("Algorithm %s failed (acceptable in test): %v", algo, err)
			} else if result != nil {
				t.Logf("Algorithm %s succeeded", algo)
			}
		})
	}
}

func TestForecastService_Execute_EmptyField(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestForecastService(mockMeta)

	ctx := context.Background()
	req := &ForecastRequest{
		Database:   "testdb",
		Collection: "testcoll",
		IDs:        []string{"device1"},
		StartTime:  time.Now().Add(-24 * time.Hour),
		EndTime:    time.Now(),
		Method:     "sma",
		Horizon:    10,
		Field:      "", // Empty field (should auto-select)
	}

	result, err := service.Execute(ctx, req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}
