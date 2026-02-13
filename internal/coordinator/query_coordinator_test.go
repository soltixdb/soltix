package coordinator

import (
	"testing"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

func TestNewQueryCoordinator(t *testing.T) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	cfg := config.CoordinatorConfig{
		HashThreshold:  20,
		VNodeCount:     200,
		ShardKeyFormat: "daily",
	}
	shardRouter := NewShardRouter(logger, mockMeta, cfg)

	qc := NewQueryCoordinator(logger, mockMeta, shardRouter)

	if qc == nil {
		t.Fatal("Expected non-nil QueryCoordinator")
		return
	}

	if qc.logger == nil {
		t.Error("Expected non-nil logger")
	}

	if qc.metadataManager == nil {
		t.Error("Expected non-nil metadataManager")
	}

	if qc.shardRouter == nil {
		t.Error("Expected non-nil shardRouter")
	}

	if qc.grpcPool == nil {
		t.Error("Expected non-nil grpcPool")
	}
}

func TestMergeAndFormatResults(t *testing.T) {
	results := []QueryResult{
		{
			DeviceID: "device1",
			DataPoints: []models.DataPointView{
				{
					Time: "2024-03-15T10:00:00Z",
					ID:   "device1",
					Fields: map[string]interface{}{
						"temperature": 25.5,
						"humidity":    60.0,
					},
				},
				{
					Time: "2024-03-15T11:00:00Z",
					ID:   "device1",
					Fields: map[string]interface{}{
						"temperature": 26.0,
						"humidity":    62.0,
					},
				},
			},
		},
		{
			DeviceID: "device2",
			DataPoints: []models.DataPointView{
				{
					Time: "2024-03-15T10:00:00Z",
					ID:   "device2",
					Fields: map[string]interface{}{
						"temperature": 24.0,
					},
				},
			},
		},
	}

	formatter := NewResultFormatter()
	formatted := formatter.MergeAndFormat(results, []string{"temperature", "humidity"})

	if len(formatted) != 2 {
		t.Errorf("Expected 2 devices, got %d", len(formatted))
	}

	// Find device1 result
	var device1Result *FormattedQueryResult
	for i := range formatted {
		if formatted[i].DeviceID == "device1" {
			device1Result = &formatted[i]
			break
		}
	}

	if device1Result == nil {
		t.Fatal("Expected device1 result")
		return
	}

	if len(device1Result.Times) != 2 {
		t.Errorf("Expected 2 time points for device1, got %d", len(device1Result.Times))
	}

	if device1Result.Times[0] != "2024-03-15T10:00:00Z" {
		t.Errorf("Expected first time to be 2024-03-15T10:00:00Z, got %s", device1Result.Times[0])
	}

	if len(device1Result.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(device1Result.Fields))
	}

	tempValues, ok := device1Result.Fields["temperature"]
	if !ok {
		t.Error("Expected temperature field")
	}

	if len(tempValues) != 2 {
		t.Errorf("Expected 2 temperature values, got %d", len(tempValues))
	}
}

func TestMergeAndFormatResults_NilValues(t *testing.T) {
	results := []QueryResult{
		{
			DeviceID: "device1",
			DataPoints: []models.DataPointView{
				{
					Time: "2024-03-15T10:00:00Z",
					ID:   "device1",
					Fields: map[string]interface{}{
						"temperature": 25.5,
					},
				},
				{
					Time: "2024-03-15T11:00:00Z",
					ID:   "device1",
					Fields: map[string]interface{}{
						"humidity": 60.0,
					},
				},
			},
		},
	}

	formatter := NewResultFormatter()
	formatted := formatter.MergeAndFormat(results, []string{"temperature", "humidity"})

	if len(formatted) != 1 {
		t.Errorf("Expected 1 device, got %d", len(formatted))
	}

	device := formatted[0]

	tempValues := device.Fields["temperature"]
	if tempValues[0] == nil {
		t.Error("Expected temperature value at index 0")
	}
	if tempValues[1] != nil {
		t.Error("Expected nil temperature value at index 1")
	}

	humidityValues := device.Fields["humidity"]
	if humidityValues[0] != nil {
		t.Error("Expected nil humidity value at index 0")
	}
	if humidityValues[1] == nil {
		t.Error("Expected humidity value at index 1")
	}
}

func TestCountFormattedPoints(t *testing.T) {
	results := []FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"t1", "t2", "t3"},
		},
		{
			DeviceID: "device2",
			Times:    []string{"t1", "t2"},
		},
	}

	formatter := NewResultFormatter()
	count := formatter.CountPoints(results)

	if count != 5 {
		t.Errorf("Expected 5 total points, got %d", count)
	}
}

func TestApplyFormattedLimit(t *testing.T) {
	results := []FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"t1", "t2", "t3"},
			Fields: map[string][]interface{}{
				"temp": {10, 20, 30},
			},
		},
		{
			DeviceID: "device2",
			Times:    []string{"t1", "t2"},
			Fields: map[string][]interface{}{
				"temp": {40, 50},
			},
		},
	}

	formatter := NewResultFormatter()
	limited := formatter.ApplyLimit(results, 4)

	totalPoints := 0
	for _, result := range limited {
		totalPoints += len(result.Times)
	}

	if totalPoints != 4 {
		t.Errorf("Expected 4 total points after limit, got %d", totalPoints)
	}

	// First device should have all 3 points
	if len(limited[0].Times) != 3 {
		t.Errorf("Expected 3 points for first device, got %d", len(limited[0].Times))
	}

	// Second device should have only 1 point (4 - 3 = 1)
	if len(limited[1].Times) != 1 {
		t.Errorf("Expected 1 point for second device, got %d", len(limited[1].Times))
	}
}

func TestApplyFormattedLimit_ZeroLimit(t *testing.T) {
	results := []FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"t1", "t2", "t3"},
		},
	}

	formatter := NewResultFormatter()
	limited := formatter.ApplyLimit(results, 0)

	// limit = 0 means unlimited, should return all results
	if len(limited) != 1 {
		t.Errorf("Expected 1 result with limit 0 (unlimited), got %d", len(limited))
	}
	if len(limited[0].Times) != 3 {
		t.Errorf("Expected 3 time points (all data), got %d", len(limited[0].Times))
	}
}

func TestFormattedQueryResult_MarshalJSON(t *testing.T) {
	result := FormattedQueryResult{
		DeviceID: "device1",
		Times:    []string{"2024-03-15T10:00:00Z", "2024-03-15T11:00:00Z"},
		Fields: map[string][]interface{}{
			"temperature": {25.5, 26.0},
			"humidity":    {60.0, 62.0},
		},
	}

	jsonData, err := result.MarshalJSON()
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	jsonStr := string(jsonData)

	// Check that JSON contains expected fields at top level
	if !contains(jsonStr, "\"id\"") {
		t.Error("Expected 'id' field in JSON")
	}

	if !contains(jsonStr, "\"times\"") {
		t.Error("Expected 'times' field in JSON")
	}

	if !contains(jsonStr, "\"temperature\"") {
		t.Error("Expected 'temperature' field in JSON")
	}

	if !contains(jsonStr, "\"humidity\"") {
		t.Error("Expected 'humidity' field in JSON")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || contains(s[1:], substr)))
}
