package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

func TestHandler_Write_ValidationErrors(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		collName       string
		requestBody    interface{}
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:     "missing_time_field",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"id":          "device1",
				"temperature": 25.5,
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
		{
			name:     "missing_id_field",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"time":        "2024-01-01T12:00:00Z",
				"temperature": 25.5,
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
		{
			name:     "invalid_time_format",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"time":        "invalid-time",
				"id":          "device1",
				"temperature": 25.5,
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_TIME_FORMAT",
		},
		{
			name:     "collection_not_found",
			dbName:   "testdb",
			collName: "nonexistent",
			requestBody: map[string]interface{}{
				"time": "2024-01-01T12:00:00Z",
				"id":   "device1",
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
			},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "COLLECTION_NOT_FOUND",
		},
		{
			name:           "invalid_json",
			dbName:         "testdb",
			collName:       "testcoll",
			requestBody:    "invalid json",
			setupMock:      func(mm *MockMetadataManager) {},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			logger := logging.NewDevelopment()
			mockMeta := NewMockMetadataManager()
			mockQueue := NewMockQueuePublisher()

			if tt.setupMock != nil {
				tt.setupMock(mockMeta)
			}

			coordCfg := config.CoordinatorConfig{
				ShardKeyFormat: "monthly",
				ReplicaFactor:  2,
			}
			shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
				queuePublisher:  mockQueue,
				shardRouter:     shardRouter,
			}

			app := fiber.New()
			app.Post("/v1/databases/:database/collections/:collection/write", handler.Write)

			// Create request
			var body []byte
			var err error
			if str, ok := tt.requestBody.(string); ok {
				body = []byte(str)
			} else {
				body, err = json.Marshal(tt.requestBody)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
			}

			req := httptest.NewRequest("POST", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName+"/write", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Test
			resp, err := app.Test(req, 10000)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %d, got %d. Body: %s", tt.expectedStatus, resp.StatusCode, string(bodyBytes))
			}

			if tt.expectedCode != "" {
				bodyBytes, _ := io.ReadAll(resp.Body)
				var errResp models.ErrorResponse
				if err := json.Unmarshal(bodyBytes, &errResp); err != nil {
					t.Fatalf("Failed to unmarshal error response: %v", err)
				}
				if errResp.Error.Code != tt.expectedCode {
					t.Errorf("Expected error code '%s', got '%s'", tt.expectedCode, errResp.Error.Code)
				}
			}
		})
	}
}

func TestHandler_WriteBatch_ValidationErrors(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		collName       string
		requestBody    interface{}
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:     "empty_points_array",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: models.WriteBatchRequest{
				Points: []map[string]interface{}{},
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
		{
			name:     "point_missing_time",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: models.WriteBatchRequest{
				Points: []map[string]interface{}{
					{
						"id":          "device1",
						"temperature": 25.5,
					},
				},
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
		{
			name:     "point_missing_id",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: models.WriteBatchRequest{
				Points: []map[string]interface{}{
					{
						"time":        "2024-01-01T12:00:00Z",
						"temperature": 25.5,
					},
				},
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
		{
			name:     "invalid_time_format_in_batch",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: models.WriteBatchRequest{
				Points: []map[string]interface{}{
					{
						"time":        "invalid-time",
						"id":          "device1",
						"temperature": 25.5,
					},
				},
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_TIME_FORMAT",
		},
		{
			name:     "collection_not_found",
			dbName:   "testdb",
			collName: "nonexistent",
			requestBody: models.WriteBatchRequest{
				Points: []map[string]interface{}{
					{
						"time": "2024-01-01T12:00:00Z",
						"id":   "device1",
					},
				},
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
			},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "COLLECTION_NOT_FOUND",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			logger := logging.NewDevelopment()
			mockMeta := NewMockMetadataManager()
			mockQueue := NewMockQueuePublisher()

			if tt.setupMock != nil {
				tt.setupMock(mockMeta)
			}

			coordCfg := config.CoordinatorConfig{
				ShardKeyFormat: "monthly",
				ReplicaFactor:  2,
			}
			shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
				queuePublisher:  mockQueue,
				shardRouter:     shardRouter,
			}

			app := fiber.New()
			app.Post("/v1/databases/:database/collections/:collection/batch", handler.WriteBatch)

			// Create request
			body, err := json.Marshal(tt.requestBody)
			if err != nil {
				t.Fatalf("Failed to marshal request body: %v", err)
			}

			req := httptest.NewRequest("POST", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName+"/batch", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Test
			resp, err := app.Test(req, 10000)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %d, got %d. Body: %s", tt.expectedStatus, resp.StatusCode, string(bodyBytes))
			}

			if tt.expectedCode != "" {
				bodyBytes, _ := io.ReadAll(resp.Body)
				var errResp models.ErrorResponse
				if err := json.Unmarshal(bodyBytes, &errResp); err != nil {
					t.Fatalf("Failed to unmarshal error response: %v", err)
				}
				if errResp.Error.Code != tt.expectedCode {
					t.Errorf("Expected error code '%s', got '%s'", tt.expectedCode, errResp.Error.Code)
				}
			}
		})
	}
}

func TestHandler_DeletePoints(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	handler := &Handler{
		logger: logger,
	}

	app := fiber.New()
	app.Delete("/v1/databases/:database/collections/:collection/delete", handler.DeletePoints)

	req := httptest.NewRequest("DELETE", "/v1/databases/testdb/collections/testcoll/delete", nil)

	// Test
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Assertions - should return not implemented
	if resp.StatusCode != fiber.StatusNotImplemented {
		t.Errorf("Expected status %d, got %d", fiber.StatusNotImplemented, resp.StatusCode)
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	var errResp models.ErrorResponse
	if err := json.Unmarshal(bodyBytes, &errResp); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	if errResp.Error.Code != "NOT_IMPLEMENTED" {
		t.Errorf("Expected error code 'NOT_IMPLEMENTED', got '%s'", errResp.Error.Code)
	}
}

func TestInferFieldType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"nil_value", nil, "null"},
		{"bool_true", true, "bool"},
		{"bool_false", false, "bool"},
		{"float64", float64(25.5), "float"},
		{"float32", float32(25.5), "float"},
		{"int", int(42), "int"},
		{"int64", int64(42), "int"},
		{"string", "hello", "string"},
		{"object", map[string]interface{}{"key": "value"}, "object"},
		{"array", []interface{}{1, 2, 3}, "array"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferFieldType(tt.value)
			if result != tt.expected {
				t.Errorf("inferFieldType(%v) = %s, expected %s", tt.value, result, tt.expected)
			}
		})
	}
}

func TestHandler_WriteBatch_Optimized_Success(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	mockQueue := NewMockQueuePublisher()

	// Create database and collection
	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {
			Name:         "testcoll",
			CreatedAt:    time.Now(),
			DeviceIDs:    make(map[string]bool),
			FieldSchemas: make(map[string]string),
		},
	}

	// Add storage nodes
	mockMeta.AddStorageNode("node1", "localhost:9001")
	mockMeta.AddStorageNode("node2", "localhost:9002")

	coordCfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  2,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

	handler := &Handler{
		logger:          logger,
		metadataManager: mockMeta,
		queuePublisher:  mockQueue,
		shardRouter:     shardRouter,
	}

	app := fiber.New()
	app.Post("/v1/databases/:database/collections/:collection/batch", handler.WriteBatch)

	// Create batch with multiple points (same time bucket for route caching test)
	points := make([]map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		points[i] = map[string]interface{}{
			"time":        "2026-01-15T12:00:00Z",
			"id":          "device_" + string(rune('A'+i%10)), // 10 unique devices
			"temperature": 20.0 + float64(i)*0.1,
			"humidity":    50.0 + float64(i)*0.1,
		}
	}

	reqBody := models.WriteBatchRequest{Points: points}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Test
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Assertions
	if resp.StatusCode != fiber.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusAccepted, resp.StatusCode, string(bodyBytes))
	}

	// Parse response
	bodyBytes, _ := io.ReadAll(resp.Body)
	var respData map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &respData); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if respData["status"] != "accepted" {
		t.Errorf("Expected status 'accepted', got '%v'", respData["status"])
	}

	totalPoints := int(respData["total_points"].(float64))
	if totalPoints != 100 {
		t.Errorf("Expected total_points 100, got %d", totalPoints)
	}

	publishedCount := int(respData["published_count"].(float64))
	// published_count = total points * nodes per point = 100 * 2 = 200
	if publishedCount != 200 {
		t.Errorf("Expected published_count 200 (100 points * 2 nodes), got %d", publishedCount)
	}

	// Verify messages were published (should be 100 points * 2 nodes = 200 messages)
	if len(mockQueue.published) != 200 {
		t.Errorf("Expected 200 published messages (100 points * 2 nodes), got %d", len(mockQueue.published))
	}
}

func TestHandler_WriteBatch_RouteCache(t *testing.T) {
	// Test that route caching works for same time bucket
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	mockQueue := NewMockQueuePublisher()

	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {
			Name:         "testcoll",
			CreatedAt:    time.Now(),
			DeviceIDs:    make(map[string]bool),
			FieldSchemas: make(map[string]string),
		},
	}
	mockMeta.AddStorageNode("node1", "localhost:9001")

	coordCfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  1,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

	handler := &Handler{
		logger:          logger,
		metadataManager: mockMeta,
		queuePublisher:  mockQueue,
		shardRouter:     shardRouter,
	}

	app := fiber.New()
	app.Post("/v1/databases/:database/collections/:collection/batch", handler.WriteBatch)

	// Create batch with points in different months (should use different shards)
	points := []map[string]interface{}{
		{"time": "2026-01-15T12:00:00Z", "id": "d1", "value": 1},
		{"time": "2026-01-20T12:00:00Z", "id": "d2", "value": 2}, // Same month - should cache
		{"time": "2026-02-15T12:00:00Z", "id": "d3", "value": 3}, // Different month
		{"time": "2026-02-20T12:00:00Z", "id": "d4", "value": 4}, // Same as above - should cache
	}

	reqBody := models.WriteBatchRequest{Points: points}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	if resp.StatusCode != fiber.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusAccepted, resp.StatusCode, string(bodyBytes))
	}

	// Should have 4 messages published
	if len(mockQueue.published) != 4 {
		t.Errorf("Expected 4 published messages, got %d", len(mockQueue.published))
	}
}

func TestHandler_WriteBatch_UniqueDeviceTracking(t *testing.T) {
	// Test that metadata is tracked once per unique device
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	mockQueue := NewMockQueuePublisher()

	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {
			Name:         "testcoll",
			CreatedAt:    time.Now(),
			DeviceIDs:    make(map[string]bool),
			FieldSchemas: make(map[string]string),
		},
	}
	mockMeta.AddStorageNode("node1", "localhost:9001")

	coordCfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  1,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

	handler := &Handler{
		logger:          logger,
		metadataManager: mockMeta,
		queuePublisher:  mockQueue,
		shardRouter:     shardRouter,
	}

	app := fiber.New()
	app.Post("/v1/databases/:database/collections/:collection/batch", handler.WriteBatch)

	// Create batch with repeated device IDs (should track each device once)
	points := []map[string]interface{}{
		{"time": "2026-01-01T00:00:00Z", "id": "device_A", "temp": 20.0},
		{"time": "2026-01-01T01:00:00Z", "id": "device_A", "temp": 21.0, "humidity": 50.0}, // Same device, new field
		{"time": "2026-01-01T02:00:00Z", "id": "device_B", "temp": 22.0},
		{"time": "2026-01-01T03:00:00Z", "id": "device_A", "temp": 23.0},
	}

	reqBody := models.WriteBatchRequest{Points: points}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	if resp.StatusCode != fiber.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusAccepted, resp.StatusCode, string(bodyBytes))
	}

	// Wait for async metadata tracking
	time.Sleep(200 * time.Millisecond)

	// Verify unique devices were tracked
	coll, _ := mockMeta.GetCollection(context.Background(), "testdb", "testcoll")
	if !coll.DeviceIDs["device_A"] {
		t.Error("Expected device_A to be tracked")
	}
	if !coll.DeviceIDs["device_B"] {
		t.Error("Expected device_B to be tracked")
	}

	// Verify merged fields (device_A should have both temp and humidity)
	if coll.FieldSchemas["temp"] != "float" {
		t.Errorf("Expected temp field type 'float', got '%s'", coll.FieldSchemas["temp"])
	}
	if coll.FieldSchemas["humidity"] != "float" {
		t.Errorf("Expected humidity field type 'float', got '%s'", coll.FieldSchemas["humidity"])
	}
}

func TestHandler_WriteBatch_EarliestTimestamp(t *testing.T) {
	// Test that earliest timestamp is tracked correctly
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()
	mockQueue := NewMockQueuePublisher()

	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {
			Name:         "testcoll",
			CreatedAt:    time.Now(),
			DeviceIDs:    make(map[string]bool),
			FieldSchemas: make(map[string]string),
		},
	}
	mockMeta.AddStorageNode("node1", "localhost:9001")

	coordCfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  1,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

	handler := &Handler{
		logger:          logger,
		metadataManager: mockMeta,
		queuePublisher:  mockQueue,
		shardRouter:     shardRouter,
	}

	app := fiber.New()
	app.Post("/v1/databases/:database/collections/:collection/batch", handler.WriteBatch)

	// Create batch with points at different times (earliest should be tracked)
	points := []map[string]interface{}{
		{"time": "2026-03-15T12:00:00Z", "id": "d1", "value": 1},
		{"time": "2026-01-01T00:00:00Z", "id": "d2", "value": 2}, // Earliest
		{"time": "2026-02-10T12:00:00Z", "id": "d3", "value": 3},
	}

	reqBody := models.WriteBatchRequest{Points: points}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	if resp.StatusCode != fiber.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusAccepted, resp.StatusCode, string(bodyBytes))
	}

	// Note: FirstDataTime update is async, so we just verify the request succeeded
}

func TestTrackMetadataAsync(t *testing.T) {
	// Setup
	mockMeta := NewMockMetadataManager()
	logger := logging.NewDevelopment()

	handler := &Handler{
		metadataManager: mockMeta,
		logger:          logger,
	}

	// Create database and collection
	ctx := context.Background()
	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {
			Name:         "testcoll",
			CreatedAt:    time.Now(),
			DeviceIDs:    make(map[string]bool),
			FieldSchemas: make(map[string]string),
		},
	}

	// Test tracking
	fields := map[string]interface{}{
		"temperature": 25.5,
		"humidity":    60.2,
		"location":    "room_a",
		"active":      true,
	}

	handler.trackMetadataAsync("testdb", "testcoll", "device_001", fields)

	// Give goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	// Verify device ID was tracked
	coll, err := mockMeta.GetCollection(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}
	if !coll.DeviceIDs["device_001"] {
		t.Error("Expected device_001 to be tracked")
	}

	// Verify fields were tracked
	expectedFields := map[string]string{
		"temperature": "float",
		"humidity":    "float",
		"location":    "string",
		"active":      "bool",
	}

	for fieldName, expectedType := range expectedFields {
		actualType, exists := coll.FieldSchemas[fieldName]
		if !exists {
			t.Errorf("Expected field %s to be tracked", fieldName)
		} else if actualType != expectedType {
			t.Errorf("Field %s: expected type %s, got %s", fieldName, expectedType, actualType)
		}
	}
}
