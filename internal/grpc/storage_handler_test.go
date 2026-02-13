package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
)

func TestNewStorageServiceHandler(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage // nil is acceptable for tests

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	if handler == nil {
		t.Fatal("Expected non-nil StorageServiceHandler")
		return
	}

	if handler.nodeID != "node1" {
		t.Errorf("Expected nodeID 'node1', got '%s'", handler.nodeID)
	}

	if handler.logger == nil {
		t.Error("Expected non-nil logger")
	}

	if handler.memoryStore == nil {
		t.Error("Expected non-nil memoryStore")
	}

	if handler.columnarStorage == nil {
		t.Error("Expected non-nil columnarStorage")
	}
}

func TestStorageServiceHandler_QueryShard_InvalidRequest(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	tests := []struct {
		name    string
		req     *pb.QueryShardRequest
		wantErr bool
	}{
		{
			name: "missing database",
			req: &pb.QueryShardRequest{
				ShardId:    1,
				Collection: "devices",
				StartTime:  1000,
				EndTime:    2000,
			},
			wantErr: true,
		},
		{
			name: "missing collection",
			req: &pb.QueryShardRequest{
				ShardId:   1,
				Database:  "testdb",
				StartTime: 1000,
				EndTime:   2000,
			},
			wantErr: true,
		},
		{
			name: "missing start time",
			req: &pb.QueryShardRequest{
				ShardId:    1,
				Database:   "testdb",
				Collection: "devices",
				EndTime:    2000,
			},
			wantErr: true,
		},
		{
			name: "missing end time",
			req: &pb.QueryShardRequest{
				ShardId:    1,
				Database:   "testdb",
				Collection: "devices",
				StartTime:  1000,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, err := handler.QueryShard(ctx, tt.req)

			if (err != nil) != tt.wantErr {
				t.Errorf("QueryShard() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStorageServiceHandler_ShouldUseAggregates(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	tests := []struct {
		interval string
		expected bool
	}{
		{"1m", false},
		{"5m", false},
		{"1h", true},
		{"1d", true},
		{"1mo", true},
		{"1y", true},
		{"", false},
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.interval, func(t *testing.T) {
			result := handler.shouldUseAggregates(tt.interval)
			if result != tt.expected {
				t.Errorf("shouldUseAggregates(%q) = %v, want %v", tt.interval, result, tt.expected)
			}
		})
	}
}

func TestStorageServiceHandler_MergeAndDeduplicate(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()

	tests := []struct {
		name         string
		memoryPoints []*storage.DataPoint
		filePoints   []*storage.DataPoint
		expected     int
	}{
		{
			name:         "empty both",
			memoryPoints: []*storage.DataPoint{},
			filePoints:   []*storage.DataPoint{},
			expected:     0,
		},
		{
			name: "only memory points",
			memoryPoints: []*storage.DataPoint{
				{ID: "device1", Time: now, InsertedAt: now},
				{ID: "device1", Time: now.Add(time.Hour), InsertedAt: now},
			},
			filePoints: []*storage.DataPoint{},
			expected:   2,
		},
		{
			name:         "only file points",
			memoryPoints: []*storage.DataPoint{},
			filePoints: []*storage.DataPoint{
				{ID: "device1", Time: now, InsertedAt: now},
				{ID: "device1", Time: now.Add(time.Hour), InsertedAt: now},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.mergeAndDeduplicate(tt.memoryPoints, tt.filePoints)

			if len(result) != tt.expected {
				t.Errorf("mergeAndDeduplicate() returned %d points, want %d", len(result), tt.expected)
			}
		})
	}
}

func TestStorageServiceHandler_FilterFields(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	points := []*storage.DataPoint{
		{
			ID:   "device1",
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    60.0,
				"pressure":    1013.0,
			},
		},
	}

	tests := []struct {
		name           string
		fields         []string
		expectedFields int
	}{
		{
			name:           "no filter",
			fields:         []string{},
			expectedFields: 3,
		},
		{
			name:           "single field",
			fields:         []string{"temperature"},
			expectedFields: 1,
		},
		{
			name:           "multiple fields",
			fields:         []string{"temperature", "humidity"},
			expectedFields: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.filterFields(points, tt.fields)

			if len(result) != len(points) {
				t.Errorf("filterFields() changed number of points: got %d, want %d", len(result), len(points))
			}

			if len(result) > 0 {
				fieldCount := len(result[0].Fields)
				if fieldCount != tt.expectedFields {
					t.Errorf("filterFields() returned %d fields, want %d", fieldCount, tt.expectedFields)
				}
			}
		})
	}
}

func TestStorageServiceHandler_GroupByDevice(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	points := []*storage.DataPoint{
		{
			ID:   "device1",
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 25.0,
			},
			InsertedAt: now,
		},
		{
			ID:   "device1",
			Time: now.Add(time.Hour),
			Fields: map[string]interface{}{
				"temperature": 26.0,
			},
			InsertedAt: now,
		},
		{
			ID:   "device2",
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 24.0,
			},
			InsertedAt: now,
		},
	}

	result := handler.groupByDevice(points)

	if len(result) != 2 {
		t.Errorf("groupByDevice() returned %d devices, want 2", len(result))
	}

	// Check device1 has 2 points
	var device1Result *pb.DataPointResult
	for _, r := range result {
		if r.Id == "device1" {
			device1Result = r
			break
		}
	}

	if device1Result == nil {
		t.Error("Expected device1 in results")
	} else if len(device1Result.DataPoints) != 2 {
		t.Errorf("Expected 2 points for device1, got %d", len(device1Result.DataPoints))
	}
}

func TestStorageServiceHandler_FilterAggregatedByDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	points := []*aggregation.AggregatedPoint{
		{DeviceID: "device1", Time: now},
		{DeviceID: "device2", Time: now},
		{DeviceID: "device3", Time: now},
	}

	tests := []struct {
		name      string
		deviceIDs []string
		expected  int
	}{
		{
			name:      "no filter",
			deviceIDs: []string{},
			expected:  3,
		},
		{
			name:      "single device",
			deviceIDs: []string{"device1"},
			expected:  1,
		},
		{
			name:      "multiple devices",
			deviceIDs: []string{"device1", "device3"},
			expected:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.filterAggregatedByDevices(points, tt.deviceIDs)

			if len(result) != tt.expected {
				t.Errorf("filterAggregatedByDevices() returned %d points, want %d", len(result), tt.expected)
			}
		})
	}
}

func TestStorageServiceHandler_ConvertAggregatedToDataPoints(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	aggPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     now,
			Fields: map[string]*aggregation.AggregatedField{
				"temperature": {
					Sum:   100.0,
					Avg:   25.0,
					Min:   20.0,
					Max:   30.0,
					Count: 4,
				},
			},
		},
	}

	tests := []struct {
		name            string
		aggregationType string
		expectedValue   float64
	}{
		{
			name:            "sum aggregation",
			aggregationType: "sum",
			expectedValue:   100.0,
		},
		{
			name:            "avg aggregation",
			aggregationType: "avg",
			expectedValue:   25.0,
		},
		{
			name:            "min aggregation",
			aggregationType: "min",
			expectedValue:   20.0,
		},
		{
			name:            "max aggregation",
			aggregationType: "max",
			expectedValue:   30.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.convertAggregatedToDataPoints(aggPoints, tt.aggregationType, []string{})

			if len(result) != 1 {
				t.Errorf("convertAggregatedToDataPoints() returned %d points, want 1", len(result))
				return
			}

			if len(result[0].Fields) != 1 {
				t.Errorf("Expected 1 field, got %d", len(result[0].Fields))
				return
			}

			temp, ok := result[0].Fields["temperature"]
			if !ok {
				t.Error("Expected temperature field")
				return
			}

			if temp != tt.expectedValue {
				t.Errorf("Expected value %f, got %f", tt.expectedValue, temp)
			}
		})
	}
}

func TestStorageServiceHandler_GetShardInfo(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	ctx := context.Background()
	req := &pb.GetShardInfoRequest{
		ShardId: 12345,
	}

	resp, err := handler.GetShardInfo(ctx, req)
	if err != nil {
		t.Fatalf("GetShardInfo() error = %v", err)
	}

	if resp.ShardId != req.ShardId {
		t.Errorf("Expected ShardId %d, got %d", req.ShardId, resp.ShardId)
	}

	if resp.NodeId != "node1" {
		t.Errorf("Expected NodeId 'node1', got %s", resp.NodeId)
	}

	if resp.Status != "active" {
		t.Errorf("Expected Status 'active', got %s", resp.Status)
	}
}

func TestStorageServiceHandler_QueryShard_ValidRequest(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	// Add test data to memory store
	now := time.Now()
	testData := &storage.DataPoint{
		Database:   "testdb",
		Collection: "devices",
		ID:         "device1",
		Time:       now,
		Fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
		},
		InsertedAt: now,
	}
	_ = memStore.Write(testData)

	ctx := context.Background()
	req := &pb.QueryShardRequest{
		ShardId:    1,
		Database:   "testdb",
		Collection: "devices",
		Ids:        []string{"device1"},
		StartTime:  now.Add(-1 * time.Hour).UnixMilli(),
		EndTime:    now.Add(1 * time.Hour).UnixMilli(),
		Fields:     []string{"temperature"},
		Limit:      10,
		Interval:   "1m",
	}

	resp, err := handler.QueryShard(ctx, req)
	if err != nil {
		t.Fatalf("QueryShard() error = %v", err)
	}

	if !resp.Success {
		t.Error("Expected success = true")
	}

	if len(resp.Results) == 0 {
		t.Error("Expected at least one result")
	}
}

func TestStorageServiceHandler_QueryShard_WithLimit(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	// Add multiple data points
	now := time.Now()
	for i := 0; i < 5; i++ {
		testData := &storage.DataPoint{
			Database:   "testdb",
			Collection: "devices",
			ID:         "device1",
			Time:       now.Add(time.Duration(i) * time.Minute),
			Fields: map[string]interface{}{
				"temperature": float64(20 + i),
			},
			InsertedAt: now,
		}
		_ = memStore.Write(testData)
	}

	ctx := context.Background()
	req := &pb.QueryShardRequest{
		ShardId:    1,
		Database:   "testdb",
		Collection: "devices",
		Ids:        []string{"device1"},
		StartTime:  now.Add(-1 * time.Hour).UnixMilli(),
		EndTime:    now.Add(1 * time.Hour).UnixMilli(),
		Limit:      2, // Limit to 2 results
	}

	resp, err := handler.QueryShard(ctx, req)
	if err != nil {
		t.Fatalf("QueryShard() error = %v", err)
	}

	if resp.Count > 2 {
		t.Errorf("Expected count <= 2, got %d", resp.Count)
	}
}

func TestStorageServiceHandler_QueryShard_AllDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	// Add data for multiple devices
	now := time.Now()
	for i := 1; i <= 3; i++ {
		testData := &storage.DataPoint{
			Database:   "testdb",
			Collection: "devices",
			ID:         "device" + string(rune('0'+i)),
			Time:       now,
			Fields: map[string]interface{}{
				"temperature": float64(20 + i),
			},
			InsertedAt: now,
		}
		_ = memStore.Write(testData)
	}

	ctx := context.Background()
	req := &pb.QueryShardRequest{
		ShardId:    1,
		Database:   "testdb",
		Collection: "devices",
		Ids:        []string{}, // Empty = all devices
		StartTime:  now.Add(-1 * time.Hour).UnixMilli(),
		EndTime:    now.Add(1 * time.Hour).UnixMilli(),
	}

	resp, err := handler.QueryShard(ctx, req)
	if err != nil {
		t.Fatalf("QueryShard() error = %v", err)
	}

	if !resp.Success {
		t.Error("Expected success = true")
	}
}

func TestStorageServiceHandler_QueryShard_FieldFilter(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	testData := &storage.DataPoint{
		Database:   "testdb",
		Collection: "devices",
		ID:         "device1",
		Time:       now,
		Fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
			"pressure":    1013.0,
		},
		InsertedAt: now,
	}
	_ = memStore.Write(testData)

	ctx := context.Background()
	req := &pb.QueryShardRequest{
		ShardId:    1,
		Database:   "testdb",
		Collection: "devices",
		Ids:        []string{"device1"},
		StartTime:  now.Add(-1 * time.Hour).UnixMilli(),
		EndTime:    now.Add(1 * time.Hour).UnixMilli(),
		Fields:     []string{"temperature", "humidity"}, // Only these fields
	}

	resp, err := handler.QueryShard(ctx, req)
	if err != nil {
		t.Fatalf("QueryShard() error = %v", err)
	}

	if len(resp.Results) > 0 && len(resp.Results[0].DataPoints) > 0 {
		fields := resp.Results[0].DataPoints[0].Fields
		if len(fields) > 2 {
			t.Errorf("Expected max 2 fields after filter, got %d", len(fields))
		}
	}
}

func TestStorageServiceHandler_MergeAndDeduplicate_WithDuplicates(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	// Memory point (newer)
	memoryPoints := []*storage.DataPoint{
		{
			ID:         "device1",
			Time:       now,
			InsertedAt: now.Add(time.Minute),
			Fields: map[string]interface{}{
				"temperature": 26.0,
			},
		},
	}

	// File point (older, same timestamp)
	filePoints := []*storage.DataPoint{
		{
			ID:         "device1",
			Time:       now,
			InsertedAt: now,
			Fields: map[string]interface{}{
				"temperature": 25.0,
			},
		},
	}

	result := handler.mergeAndDeduplicate(memoryPoints, filePoints)

	// Should only have 1 point (deduplicated)
	if len(result) != 1 {
		t.Errorf("Expected 1 point after deduplication, got %d", len(result))
	}

	// Should use newer value from memory
	if temp, ok := result[0].Fields["temperature"].(float64); ok {
		if temp != 26.0 {
			t.Errorf("Expected temperature 26.0 (from memory), got %f", temp)
		}
	}
}

func TestStorageServiceHandler_GroupByDevice_MultipleDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	points := []*storage.DataPoint{
		{
			ID:   "device1",
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 25.0,
			},
			InsertedAt: now,
		},
		{
			ID:   "device2",
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 24.0,
			},
			InsertedAt: now,
		},
		{
			ID:   "device1",
			Time: now.Add(time.Minute),
			Fields: map[string]interface{}{
				"temperature": 26.0,
			},
			InsertedAt: now,
		},
	}

	result := handler.groupByDevice(points)

	if len(result) != 2 {
		t.Errorf("Expected 2 devices, got %d", len(result))
	}

	// Check device1 has 2 points
	for _, r := range result {
		if r.Id == "device1" {
			if len(r.DataPoints) != 2 {
				t.Errorf("Expected 2 points for device1, got %d", len(r.DataPoints))
			}
		}
		if r.Id == "device2" {
			if len(r.DataPoints) != 1 {
				t.Errorf("Expected 1 point for device2, got %d", len(r.DataPoints))
			}
		}
	}
}

func TestStorageServiceHandler_GroupByDevice_NonNumericFields(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	points := []*storage.DataPoint{
		{
			ID:   "device1",
			Time: now,
			Fields: map[string]interface{}{
				"temperature": 25.5,
				"status":      "active", // Non-numeric, should now be included as FieldValue
				"enabled":     true,     // Bool, should also be included
			},
			InsertedAt: now,
		},
	}

	result := handler.groupByDevice(points)

	if len(result) == 0 {
		t.Fatal("Expected at least one result")
	}

	// Check all field types are included
	if len(result[0].DataPoints) > 0 {
		fields := result[0].DataPoints[0].Fields
		if _, ok := fields["temperature"]; !ok {
			t.Error("Expected numeric field 'temperature' to be present")
		}
		if fv, ok := fields["status"]; !ok {
			t.Error("Expected string field 'status' to be present")
		} else if sv, ok := fv.Value.(*pb.FieldValue_StringValue); !ok || sv.StringValue != "active" {
			t.Errorf("Expected status='active', got %v", fv.Value)
		}
		if fv, ok := fields["enabled"]; !ok {
			t.Error("Expected bool field 'enabled' to be present")
		} else if bv, ok := fv.Value.(*pb.FieldValue_BoolValue); !ok || !bv.BoolValue {
			t.Errorf("Expected enabled=true, got %v", fv.Value)
		}
	}
}

func TestStorageServiceHandler_ConvertAggregatedToDataPoints_EmptyFields(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	aggPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     now,
			Fields:   map[string]*aggregation.AggregatedField{}, // Empty fields
		},
	}

	result := handler.convertAggregatedToDataPoints(aggPoints, "sum", []string{})

	// Should not add points with empty fields
	if len(result) != 0 {
		t.Errorf("Expected 0 points with empty fields, got %d", len(result))
	}
}

func TestStorageServiceHandler_ConvertAggregatedToDataPoints_MultipleDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	handler := NewStorageServiceHandler("/tmp/test", "node1", logger, memStore, columnarStorage, aggStorage)

	now := time.Now()
	aggPoints := []*aggregation.AggregatedPoint{
		{
			DeviceID: "device1",
			Time:     now,
			Fields: map[string]*aggregation.AggregatedField{
				"temperature": {Sum: 100.0, Avg: 25.0, Min: 20.0, Max: 30.0, Count: 4},
			},
		},
		{
			DeviceID: "device2",
			Time:     now,
			Fields: map[string]*aggregation.AggregatedField{
				"temperature": {Sum: 80.0, Avg: 20.0, Min: 15.0, Max: 25.0, Count: 4},
			},
		},
	}

	result := handler.convertAggregatedToDataPoints(aggPoints, "avg", []string{})

	if len(result) != 2 {
		t.Errorf("Expected 2 data points, got %d", len(result))
	}

	// Check device1 has correct avg value
	for _, point := range result {
		if point.ID == "device1" {
			if temp, ok := point.Fields["temperature"].(float64); ok {
				if temp != 25.0 {
					t.Errorf("Expected avg 25.0, got %f", temp)
				}
			}
		}
	}
}
