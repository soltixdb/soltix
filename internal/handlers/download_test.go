package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

func TestHandler_CreateDownload_ValidationErrors(t *testing.T) {
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
			name:     "collection_not_found",
			dbName:   "testdb",
			collName: "nonexistent",
			requestBody: map[string]interface{}{
				"start_time": "2026-01-01T00:00:00Z",
				"end_time":   "2026-01-31T00:00:00Z",
				"format":     "csv",
				"ids":        []string{"device1"},
			},
			setupMock: func(mm *MockMetadataManager) {
				mm.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
			},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "COLLECTION_NOT_FOUND",
		},
		{
			name:     "missing_start_time",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"end_time": "2026-01-31T00:00:00Z",
				"format":   "csv",
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
			name:     "missing_end_time",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"start_time": "2026-01-01T00:00:00Z",
				"format":     "csv",
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
			name:     "invalid_start_time_format",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"start_time": "2026-01-01",
				"end_time":   "2026-01-31T00:00:00Z",
				"format":     "csv",
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
			name:     "invalid_format",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"start_time": "2026-01-01T00:00:00Z",
				"end_time":   "2026-01-31T00:00:00Z",
				"format":     "xml",
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
			name:     "end_time_before_start_time",
			dbName:   "testdb",
			collName: "testcoll",
			requestBody: map[string]interface{}{
				"start_time": "2026-01-31T00:00:00Z",
				"end_time":   "2026-01-01T00:00:00Z",
				"format":     "csv",
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
			name:           "invalid_json_body",
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

			if tt.setupMock != nil {
				tt.setupMock(mockMeta)
			}

			handler := createTestHandler(logger, mockMeta)

			app := fiber.New()
			app.Post("/v1/databases/:database/collections/:collection/download", handler.CreateDownload)

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

			req := httptest.NewRequest("POST", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName+"/download", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Test
			resp, err := app.Test(req, 10000)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Read body once
			bodyBytes, _ := io.ReadAll(resp.Body)

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d. Body: %s", tt.expectedStatus, resp.StatusCode, string(bodyBytes))
			}

			if tt.expectedCode != "" {
				var errResp models.ErrorResponse
				if err := json.Unmarshal(bodyBytes, &errResp); err != nil {
					t.Fatalf("Failed to unmarshal error response: %v. Body: %s", err, string(bodyBytes))
				}
				if errResp.Error.Code != tt.expectedCode {
					t.Errorf("Expected error code '%s', got '%s'", tt.expectedCode, errResp.Error.Code)
				}
			}
		})
	}
}

func TestHandler_CreateDownload_Success(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()

	// Add test database and collection
	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
	}

	handler := createTestHandler(logger, mockMeta)

	app := fiber.New()
	app.Post("/v1/databases/:database/collections/:collection/download", handler.CreateDownload)

	// Create request
	reqBody := map[string]interface{}{
		"start_time": "2026-01-01T00:00:00Z",
		"end_time":   "2026-01-31T00:00:00Z",
		"format":     "csv",
		"ids":        []string{"device1"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/download", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Test
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Should return 202 Accepted
	if resp.StatusCode != fiber.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusAccepted, resp.StatusCode, string(bodyBytes))
	}

	// Parse response
	bodyBytes, _ := io.ReadAll(resp.Body)
	var createResp models.DownloadCreateResponse
	if err := json.Unmarshal(bodyBytes, &createResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Validate response
	if createResp.RequestID == "" {
		t.Error("Expected non-empty request_id")
	}
	// Status could be pending or processing depending on timing
	if createResp.Status != "pending" && createResp.Status != "processing" {
		t.Errorf("Expected status 'pending' or 'processing', got '%s'", createResp.Status)
	}
	if createResp.ExpiresAt.IsZero() {
		t.Error("Expected non-zero expires_at")
	}
}

func TestHandler_GetDownloadStatus_NotFound(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()

	handler := createTestHandler(logger, mockMeta)

	app := fiber.New()
	app.Get("/v1/download/status/:request_id", handler.GetDownloadStatus)

	// Create request with non-existent request_id
	req := httptest.NewRequest("GET", "/v1/download/status/non-existent-id", nil)

	// Test
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Should return 404
	if resp.StatusCode != fiber.StatusNotFound {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusNotFound, resp.StatusCode, string(bodyBytes))
	}
}

func TestHandler_DownloadFile_NotFound(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()

	handler := createTestHandler(logger, mockMeta)

	app := fiber.New()
	app.Get("/v1/download/file/:request_id", handler.DownloadFile)

	// Create request with non-existent request_id
	req := httptest.NewRequest("GET", "/v1/download/file/non-existent-id", nil)

	// Test
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Should return 404
	if resp.StatusCode != fiber.StatusNotFound {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusNotFound, resp.StatusCode, string(bodyBytes))
	}
}

func TestHandler_CreateDownload_WithOptions(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()

	// Add test database and collection
	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {Name: "testcoll", CreatedAt: time.Now()},
	}

	handler := createTestHandler(logger, mockMeta)

	app := fiber.New()
	app.Post("/v1/databases/:database/collections/:collection/download", handler.CreateDownload)

	// Create request with all options
	reqBody := map[string]interface{}{
		"start_time":   "2026-01-01T00:00:00Z",
		"end_time":     "2026-01-31T00:00:00Z",
		"format":       "json",
		"interval":     "1h",
		"aggregation":  "avg",
		"downsampling": "lttb",
		"ids":          []string{"device1", "device2"},
		"fields":       []string{"temperature"},
		"filename":     "my_export.json",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/download", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Test
	resp, err := app.Test(req, 10000)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Should return 202 Accepted
	if resp.StatusCode != fiber.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status %d, got %d. Body: %s", fiber.StatusAccepted, resp.StatusCode, string(bodyBytes))
	}

	// Parse response
	bodyBytes, _ := io.ReadAll(resp.Body)
	var createResp models.DownloadCreateResponse
	if err := json.Unmarshal(bodyBytes, &createResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if createResp.RequestID == "" {
		t.Error("Expected non-empty request_id")
	}
}
