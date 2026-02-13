package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// createTestHandler creates a properly initialized handler for testing
func createTestHandler(logger *logging.Logger, mockMeta *MockMetadataManager) *Handler {
	coordCfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  2,
	}
	return New(logger, mockMeta, nil, coordCfg, "/tmp/soltix-test-downloads")
}

func TestHandler_Query_ValidationErrors(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		collName       string
		queryParams    string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:        "collection_not_found",
			dbName:      "testdb",
			collName:    "nonexistent",
			queryParams: "?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z&ids=device1",
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
			},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "COLLECTION_NOT_FOUND",
		},
		{
			name:        "invalid_start_time",
			dbName:      "testdb",
			collName:    "testcoll",
			queryParams: "?start_time=invalid&end_time=2024-01-02T00:00:00Z",
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
			name:        "missing_start_time",
			dbName:      "testdb",
			collName:    "testcoll",
			queryParams: "?end_time=2024-01-02T00:00:00Z",
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
			name:        "missing_end_time",
			dbName:      "testdb",
			collName:    "testcoll",
			queryParams: "?start_time=2024-01-01T00:00:00Z",
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
				mm.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			logger := logging.NewDevelopment()
			mockMeta := NewMockMetadataManager()

			if tt.setupMock != nil {
				tt.setupMock(mockMeta)
			}

			handler := createTestHandler(logger, mockMeta)

			app := fiber.New()
			app.Get("/v1/databases/:database/collections/:collection/query", handler.Query)

			req := httptest.NewRequest("GET", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName+"/query"+tt.queryParams, nil)

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

func TestHandler_QueryPost_ValidationErrors(t *testing.T) {
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
			name:           "invalid_json_body",
			dbName:         "testdb",
			collName:       "testcoll",
			requestBody:    "invalid json",
			setupMock:      func(mm *MockMetadataManager) {},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_JSON",
		},
		{
			name:     "missing_required_fields",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"start_time": "2024-01-01T00:00:00Z",
				// Missing end_time
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
			name:     "collection_not_found",
			dbName:   "testdb",
			collName: "nonexistent",
			requestBody: map[string]interface{}{
				"start_time": "2024-01-01T00:00:00Z",
				"end_time":   "2024-01-02T00:00:00Z",
				"ids":        []string{"device1"},
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

			if tt.setupMock != nil {
				tt.setupMock(mockMeta)
			}

			handler := createTestHandler(logger, mockMeta)

			app := fiber.New()
			app.Post("/v1/databases/:database/collections/:collection/query", handler.QueryPost)

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

			req := httptest.NewRequest("POST", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName+"/query", bytes.NewReader(body))
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
