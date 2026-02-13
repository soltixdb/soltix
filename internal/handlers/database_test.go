package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// MockMetadataManager is a mock implementation of metadata.Manager
type MockMetadataManager struct {
	mu          sync.RWMutex
	databases   map[string]*metadata.Database
	collections map[string]map[string]*metadata.Collection
	nodesData   map[string]string // For GetPrefix to return node data
	kvStore     map[string]string // For Get/Put operations
	shouldError bool
	errorMsg    string
}

func NewMockMetadataManager() *MockMetadataManager {
	return &MockMetadataManager{
		databases:   make(map[string]*metadata.Database),
		collections: make(map[string]map[string]*metadata.Collection),
		nodesData:   make(map[string]string),
		kvStore:     make(map[string]string),
	}
}

// AddStorageNode adds a mock storage node for testing
func (m *MockMetadataManager) AddStorageNode(nodeID, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodeInfo := `{"id":"` + nodeID + `","address":"` + address + `","status":"active"}`
	m.nodesData["/soltix/nodes/"+nodeID] = nodeInfo
}

func (m *MockMetadataManager) CreateDatabase(ctx context.Context, db *metadata.Database) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[db.Name]; exists {
		return errors.New("database " + db.Name + " already exists")
	}
	m.databases[db.Name] = db
	return nil
}

func (m *MockMetadataManager) GetDatabase(ctx context.Context, name string) (*metadata.Database, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	db, exists := m.databases[name]
	if !exists {
		return nil, errors.New("database " + name + " not found")
	}
	return db, nil
}

func (m *MockMetadataManager) ListDatabases(ctx context.Context) ([]*metadata.Database, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	dbs := make([]*metadata.Database, 0, len(m.databases))
	for _, db := range m.databases {
		dbs = append(dbs, db)
	}
	return dbs, nil
}

func (m *MockMetadataManager) DeleteDatabase(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	delete(m.databases, name)
	delete(m.collections, name)
	return nil
}

func (m *MockMetadataManager) DatabaseExists(ctx context.Context, name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return false, errors.New(m.errorMsg)
	}
	_, exists := m.databases[name]
	return exists, nil
}

func (m *MockMetadataManager) CreateCollection(ctx context.Context, dbName string, coll *metadata.Collection) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		m.collections[dbName] = make(map[string]*metadata.Collection)
	}
	if _, exists := m.collections[dbName][coll.Name]; exists {
		return errors.New("collection " + coll.Name + " already exists in database " + dbName)
	}
	m.collections[dbName][coll.Name] = coll
	return nil
}

func (m *MockMetadataManager) GetCollection(ctx context.Context, dbName, collName string) (*metadata.Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return nil, errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return nil, errors.New("collection " + collName + " not found in database " + dbName)
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return nil, errors.New("collection " + collName + " not found in database " + dbName)
	}
	return coll, nil
}

func (m *MockMetadataManager) ListCollections(ctx context.Context, dbName string) ([]*metadata.Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return nil, errors.New("database " + dbName + " not found")
	}
	colls := make([]*metadata.Collection, 0, len(m.collections[dbName]))
	for _, coll := range m.collections[dbName] {
		colls = append(colls, coll)
	}
	return colls, nil
}

func (m *MockMetadataManager) UpdateCollection(ctx context.Context, dbName string, coll *metadata.Collection) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return errors.New("collection " + coll.Name + " not found in database " + dbName)
	}
	if _, exists := m.collections[dbName][coll.Name]; !exists {
		return errors.New("collection " + coll.Name + " not found in database " + dbName)
	}
	m.collections[dbName][coll.Name] = coll
	return nil
}

func (m *MockMetadataManager) DeleteCollection(ctx context.Context, dbName, collName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if m.collections[dbName] != nil {
		delete(m.collections[dbName], collName)
	}
	return nil
}

func (m *MockMetadataManager) CollectionExists(ctx context.Context, dbName, collName string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return false, errors.New(m.errorMsg)
	}
	if m.collections[dbName] == nil {
		return false, nil
	}
	_, exists := m.collections[dbName][collName]
	return exists, nil
}

func (m *MockMetadataManager) ValidateCollection(ctx context.Context, dbName, collName string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	if _, exists := m.collections[dbName][collName]; !exists {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	return nil
}

func (m *MockMetadataManager) Get(ctx context.Context, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return "", errors.New(m.errorMsg)
	}
	if val, exists := m.kvStore[key]; exists {
		return val, nil
	}
	return "", nil
}

func (m *MockMetadataManager) Put(ctx context.Context, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	m.kvStore[key] = value
	return nil
}

func (m *MockMetadataManager) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	delete(m.kvStore, key)
	return nil
}

func (m *MockMetadataManager) GetPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	// Return nodes data if prefix matches
	if prefix == "/soltix/nodes/" {
		return m.nodesData, nil
	}
	return make(map[string]string), nil
}

func (m *MockMetadataManager) Close() error {
	return nil
}

// TrackDeviceID tracks a device ID for a collection
func (m *MockMetadataManager) TrackDeviceID(ctx context.Context, dbName, collName, deviceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	if coll.DeviceIDs == nil {
		coll.DeviceIDs = make(map[string]bool)
	}
	coll.DeviceIDs[deviceID] = true
	return nil
}

// TrackField tracks a field schema for a collection
func (m *MockMetadataManager) TrackField(ctx context.Context, dbName, collName, fieldName, fieldType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	if coll.FieldSchemas == nil {
		coll.FieldSchemas = make(map[string]string)
	}
	coll.FieldSchemas[fieldName] = fieldType
	return nil
}

// TrackFields adds or updates multiple fields at once
func (m *MockMetadataManager) TrackFields(ctx context.Context, dbName, collName string, fieldSchemas map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return errors.New("collection " + collName + " not found in database " + dbName)
	}
	if coll.FieldSchemas == nil {
		coll.FieldSchemas = make(map[string]string)
	}
	for fieldName, fieldType := range fieldSchemas {
		coll.FieldSchemas[fieldName] = fieldType
	}
	return nil
}

// GetDeviceIDs returns all tracked device IDs for a collection
func (m *MockMetadataManager) GetDeviceIDs(ctx context.Context, dbName, collName string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return nil, errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return nil, errors.New("collection " + collName + " not found in database " + dbName)
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return nil, errors.New("collection " + collName + " not found in database " + dbName)
	}
	deviceIDs := make([]string, 0, len(coll.DeviceIDs))
	for id := range coll.DeviceIDs {
		deviceIDs = append(deviceIDs, id)
	}
	return deviceIDs, nil
}

// GetFieldSchemas returns all tracked field schemas for a collection
func (m *MockMetadataManager) GetFieldSchemas(ctx context.Context, dbName, collName string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shouldError {
		return nil, errors.New(m.errorMsg)
	}
	if _, exists := m.databases[dbName]; !exists {
		return nil, errors.New("database " + dbName + " not found")
	}
	if m.collections[dbName] == nil {
		return nil, errors.New("collection " + collName + " not found in database " + dbName)
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return nil, errors.New("collection " + collName + " not found in database " + dbName)
	}
	return coll.FieldSchemas, nil
}

func TestHandler_CreateDatabase(t *testing.T) {
	tests := []struct {
		name           string
		requestBody    interface{}
		expectedStatus int
		expectedCode   string
		setupMock      func(*MockMetadataManager)
	}{
		{
			name: "valid_database_creation",
			requestBody: models.CreateDatabaseRequest{
				Name:        "testdb",
				Description: "Test database",
				Metadata:    map[string]string{"key": "value"},
			},
			expectedStatus: fiber.StatusCreated,
		},
		{
			name: "invalid_database_name_special_chars",
			requestBody: models.CreateDatabaseRequest{
				Name: "test@db",
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_NAME",
		},
		{
			name: "database_name_too_long",
			requestBody: models.CreateDatabaseRequest{
				Name: "this_is_a_very_long_database_name_that_exceeds_the_maximum_length_limit",
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_NAME",
		},
		{
			name: "reserved_database_name",
			requestBody: models.CreateDatabaseRequest{
				Name: "system",
			},
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "INVALID_NAME",
		},
		{
			name: "database_already_exists",
			requestBody: models.CreateDatabaseRequest{
				Name: "existing_db",
			},
			expectedStatus: fiber.StatusConflict,
			expectedCode:   "DATABASE_EXISTS",
			setupMock: func(m *MockMetadataManager) {
				m.databases["existing_db"] = &metadata.Database{
					Name:      "existing_db",
					CreatedAt: time.Now(),
				}
			},
		},
		{
			name:           "invalid_json",
			requestBody:    "invalid json",
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

			handler := &Handler{
				logger:          logger,
				metadataManager: mockMeta,
			}

			app := fiber.New()
			app.Post("/v1/databases", handler.CreateDatabase)

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

			req := httptest.NewRequest("POST", "/v1/databases", bytes.NewReader(body))
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

func TestHandler_ListDatabases(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCount  int
		shouldError    bool
	}{
		{
			name: "list_empty_databases",
			setupMock: func(m *MockMetadataManager) {
				// No databases
			},
			expectedStatus: fiber.StatusOK,
			expectedCount:  0,
		},
		{
			name: "list_multiple_databases",
			setupMock: func(m *MockMetadataManager) {
				m.databases["db1"] = &metadata.Database{
					Name:      "db1",
					CreatedAt: time.Now(),
				}
				m.databases["db2"] = &metadata.Database{
					Name:      "db2",
					CreatedAt: time.Now(),
				}
			},
			expectedStatus: fiber.StatusOK,
			expectedCount:  2,
		},
		{
			name: "internal_error",
			setupMock: func(m *MockMetadataManager) {
				m.shouldError = true
				m.errorMsg = "internal error"
			},
			expectedStatus: fiber.StatusInternalServerError,
			shouldError:    true,
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
			app.Get("/v1/databases", handler.ListDatabases)

			req := httptest.NewRequest("GET", "/v1/databases", nil)

			// Test
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			if !tt.shouldError {
				bodyBytes, _ := io.ReadAll(resp.Body)
				var listResp models.DatabaseListResponse
				if err := json.Unmarshal(bodyBytes, &listResp); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}
				if len(listResp.Databases) != tt.expectedCount {
					t.Errorf("Expected %d databases, got %d", tt.expectedCount, len(listResp.Databases))
				}
			}
		})
	}
}

func TestHandler_GetDatabase(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:   "get_existing_database",
			dbName: "testdb",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:        "testdb",
					Description: "Test database",
					CreatedAt:   time.Now(),
				}
			},
			expectedStatus: fiber.StatusOK,
		},
		{
			name:           "get_nonexistent_database",
			dbName:         "nonexistent",
			setupMock:      func(m *MockMetadataManager) {},
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "DATABASE_NOT_FOUND",
		},
		{
			name:   "internal_error",
			dbName: "testdb",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{Name: "testdb"}
				m.shouldError = true
				m.errorMsg = "internal error"
			},
			expectedStatus: fiber.StatusInternalServerError,
			expectedCode:   "INTERNAL_ERROR",
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
			app.Get("/v1/databases/:database", handler.GetDatabase)

			req := httptest.NewRequest("GET", "/v1/databases/"+tt.dbName, nil)

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

func TestHandler_DeleteDatabase(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		setupMock      func(*MockMetadataManager)
		expectedStatus int
		expectedCode   string
	}{
		{
			name:   "delete_existing_database",
			dbName: "testdb",
			setupMock: func(m *MockMetadataManager) {
				m.databases["testdb"] = &metadata.Database{
					Name:      "testdb",
					CreatedAt: time.Now(),
				}
			},
			expectedStatus: fiber.StatusNoContent,
		},
		{
			name:           "delete_nonexistent_database",
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
			app.Delete("/v1/databases/:database", handler.DeleteDatabase)

			req := httptest.NewRequest("DELETE", "/v1/databases/"+tt.dbName, nil)

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
