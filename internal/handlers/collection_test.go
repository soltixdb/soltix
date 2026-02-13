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

func TestHandler_CreateCollection(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		requestBody    interface{}
		expectedStatus int
		expectedCode   string
		setupMock      func(*MockMetadataManager)
	}{
		{
			name:   "valid_collection_creation",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name:        "testcoll",
				Description: "Test collection",
				SchemaHints: map[string]string{"field1": "float"},
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "timestamp", Required: true},
					{Name: "id", Type: "string", Required: true},
					{Name: "value", Type: "number", Required: false},
				},
			},
			expectedStatus: fiber.StatusCreated,
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "missing_time_field",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "testcoll",
				Fields: []models.FieldDefinition{
					{Name: "id", Type: "string", Required: true},
				},
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_FIELDS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "missing_id_field",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "testcoll",
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "timestamp", Required: true},
				},
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_FIELDS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "invalid_time_field_type",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "testcoll",
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "string", Required: true},
					{Name: "id", Type: "string", Required: true},
				},
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_FIELDS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "invalid_id_field_type",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "testcoll",
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "timestamp", Required: true},
					{Name: "id", Type: "number", Required: true},
				},
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_FIELDS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "duplicate_field_names",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "testcoll",
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "timestamp", Required: true},
					{Name: "id", Type: "string", Required: true},
					{Name: "value", Type: "number", Required: false},
					{Name: "value", Type: "string", Required: false},
				},
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_FIELDS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "database_not_found",
			dbName: "nonexistent",
			requestBody: models.CreateCollectionRequest{
				Name: "testcoll",
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "timestamp", Required: true},
					{Name: "id", Type: "string", Required: true},
				},
			},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "DATABASE_NOT_FOUND",
			setupMock:      func(m *MockMetadataManager) {},
		},
		{
			name:   "invalid_collection_name",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "test@coll",
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_NAME",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "collection_name_too_long",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "this_is_a_very_long_collection_name_that_exceeds_maximum_length_limit_of_64_characters",
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_NAME",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "reserved_collection_name",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "metadata",
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_NAME",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:   "collection_already_exists",
			dbName: "testdb",
			requestBody: models.CreateCollectionRequest{
				Name: "existing",
				Fields: []models.FieldDefinition{
					{Name: "time", Type: "timestamp", Required: true},
					{Name: "id", Type: "string", Required: true},
				},
			},
			expectedStatus: fiber.StatusConflict,
			expectedCode:   "COLLECTION_EXISTS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
				m.collections["testdb"] = map[string]*metadata.Collection{
					"existing": {
						Name:      "existing",
						CreatedAt: time.Now(),
					},
				}
			},
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

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
			}

			app := fiber.New()
			app.Post("/v1/databases/:database/collections", handler.CreateCollection)

			// Create request
			body, err := json.Marshal(tt.requestBody)
			if err != nil {
				t.Fatalf("Failed to marshal request body: %v", err)
			}

			req := httptest.NewRequest("POST", "/v1/databases/"+tt.dbName+"/collections", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Test
			resp, err := app.Test(req)
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

func TestHandler_ListCollections(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCount  int
		expectedCode   string
	}{
		{
			name:   "list_empty_collections",
			dbName: "testdb",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
			expectedStatus: fiber.StatusOK,
			expectedCount:  0,
		},
		{
			name:   "list_multiple_collections",
			dbName: "testdb",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
				m.collections["testdb"] = map[string]*metadata.Collection{
					"coll1": {Name: "coll1", CreatedAt: time.Now()},
					"coll2": {Name: "coll2", CreatedAt: time.Now()},
				}
			},
			expectedStatus: fiber.StatusOK,
			expectedCount:  2,
		},
		{
			name:           "database_not_found",
			dbName:         "nonexistent",
			setupMock:      func(m *MockMetadataManager) {},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "DATABASE_NOT_FOUND",
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

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
			}

			app := fiber.New()
			app.Get("/v1/databases/:database/collections", handler.ListCollections)

			req := httptest.NewRequest("GET", "/v1/databases/"+tt.dbName+"/collections", nil)

			// Test
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			if tt.expectedCode == "" {
				bodyBytes, _ := io.ReadAll(resp.Body)
				var listResp models.CollectionListResponse
				if err := json.Unmarshal(bodyBytes, &listResp); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}
				if len(listResp.Collections) != tt.expectedCount {
					t.Errorf("Expected %d collections, got %d", tt.expectedCount, len(listResp.Collections))
				}
			}
		})
	}
}

func TestHandler_GetCollection(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		collName       string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:     "get_existing_collection",
			dbName:   "testdb",
			collName: "testcoll",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
				m.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {
						Name:        "testcoll",
						Description: "Test collection",
						CreatedAt:   time.Now(),
					},
				}
			},
			expectedStatus: fiber.StatusOK,
		},
		{
			name:           "database_not_found",
			dbName:         "nonexistent",
			collName:       "testcoll",
			setupMock:      func(m *MockMetadataManager) {},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "DATABASE_NOT_FOUND",
		},
		{
			name:     "collection_not_found",
			dbName:   "testdb",
			collName: "nonexistent",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
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

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
			}

			app := fiber.New()
			app.Get("/v1/databases/:database/collections/:collection", handler.GetCollection)

			req := httptest.NewRequest("GET", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName, nil)

			// Test
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
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

func TestHandler_DeleteCollection(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		collName       string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:     "delete_existing_collection",
			dbName:   "testdb",
			collName: "testcoll",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
				m.collections["testdb"] = map[string]*metadata.Collection{
					"testcoll": {
						Name:      "testcoll",
						CreatedAt: time.Now(),
					},
				}
			},
			expectedStatus: fiber.StatusNoContent,
		},
		{
			name:     "collection_not_found",
			dbName:   "testdb",
			collName: "nonexistent",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
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

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
			}

			app := fiber.New()
			app.Delete("/v1/databases/:database/collections/:collection", handler.DeleteCollection)

			req := httptest.NewRequest("DELETE", "/v1/databases/"+tt.dbName+"/collections/"+tt.collName, nil)

			// Test
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %d, got %d. Body: %s", tt.expectedStatus, resp.StatusCode, string(bodyBytes))
			}
		})
	}
}
