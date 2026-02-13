package services

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// MockMetadataManager is a mock implementation of metadata.Manager for testing
type MockMetadataManager struct {
	mu          sync.RWMutex
	databases   map[string]*metadata.Database
	collections map[string]map[string]*metadata.Collection
	kvStore     map[string]string
	nodesData   map[string]string
	shouldError bool
	errorMsg    string
}

func NewMockMetadataManager() *MockMetadataManager {
	return &MockMetadataManager{
		databases:   make(map[string]*metadata.Database),
		collections: make(map[string]map[string]*metadata.Collection),
		kvStore:     make(map[string]string),
		nodesData:   make(map[string]string),
	}
}

func (m *MockMetadataManager) CreateDatabase(ctx context.Context, db *metadata.Database) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldError {
		return errors.New(m.errorMsg)
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
	return nil, nil
}

func (m *MockMetadataManager) DeleteDatabase(ctx context.Context, name string) error {
	return nil
}

func (m *MockMetadataManager) DatabaseExists(ctx context.Context, name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.databases[name]
	return exists, nil
}

func (m *MockMetadataManager) CreateCollection(ctx context.Context, dbName string, coll *metadata.Collection) error {
	return nil
}

func (m *MockMetadataManager) GetCollection(ctx context.Context, dbName, collName string) (*metadata.Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.collections[dbName] == nil {
		return nil, errors.New("collection " + collName + " not found")
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return nil, errors.New("collection " + collName + " not found")
	}
	return coll, nil
}

func (m *MockMetadataManager) ListCollections(ctx context.Context, dbName string) ([]*metadata.Collection, error) {
	return nil, nil
}

func (m *MockMetadataManager) UpdateCollection(ctx context.Context, dbName string, coll *metadata.Collection) error {
	return nil
}

func (m *MockMetadataManager) DeleteCollection(ctx context.Context, dbName, collName string) error {
	return nil
}

func (m *MockMetadataManager) CollectionExists(ctx context.Context, dbName, collName string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
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
		return errors.New("collection " + collName + " not found")
	}
	if _, exists := m.collections[dbName][collName]; !exists {
		return errors.New("collection " + collName + " not found")
	}
	return nil
}

func (m *MockMetadataManager) TrackDeviceID(ctx context.Context, dbName, collName, deviceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.collections[dbName] == nil {
		return errors.New("collection not found")
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return errors.New("collection not found")
	}
	if coll.DeviceIDs == nil {
		coll.DeviceIDs = make(map[string]bool)
	}
	coll.DeviceIDs[deviceID] = true
	return nil
}

func (m *MockMetadataManager) TrackField(ctx context.Context, dbName, collName, fieldName, fieldType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.collections[dbName] == nil {
		return errors.New("collection not found")
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return errors.New("collection not found")
	}
	if coll.FieldSchemas == nil {
		coll.FieldSchemas = make(map[string]string)
	}
	coll.FieldSchemas[fieldName] = fieldType
	return nil
}

func (m *MockMetadataManager) TrackFields(ctx context.Context, dbName, collName string, fieldSchemas map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.collections[dbName] == nil {
		return errors.New("collection not found")
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return errors.New("collection not found")
	}
	if coll.FieldSchemas == nil {
		coll.FieldSchemas = make(map[string]string)
	}
	for fieldName, fieldType := range fieldSchemas {
		coll.FieldSchemas[fieldName] = fieldType
	}
	return nil
}

func (m *MockMetadataManager) GetDeviceIDs(ctx context.Context, dbName, collName string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.collections[dbName] == nil {
		return nil, errors.New("collection not found")
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return nil, errors.New("collection not found")
	}
	deviceIDs := make([]string, 0, len(coll.DeviceIDs))
	for id := range coll.DeviceIDs {
		deviceIDs = append(deviceIDs, id)
	}
	return deviceIDs, nil
}

func (m *MockMetadataManager) GetFieldSchemas(ctx context.Context, dbName, collName string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.collections[dbName] == nil {
		return nil, errors.New("collection not found")
	}
	coll, exists := m.collections[dbName][collName]
	if !exists {
		return nil, errors.New("collection not found")
	}
	return coll.FieldSchemas, nil
}

func (m *MockMetadataManager) Get(ctx context.Context, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, exists := m.kvStore[key]; exists {
		return val, nil
	}
	return "", nil
}

func (m *MockMetadataManager) Put(ctx context.Context, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kvStore[key] = value
	return nil
}

func (m *MockMetadataManager) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.kvStore, key)
	return nil
}

func (m *MockMetadataManager) GetPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if prefix == "/soltix/nodes/" {
		return m.nodesData, nil
	}
	return make(map[string]string), nil
}

func (m *MockMetadataManager) Close() error {
	return nil
}

// Helper to setup mock with database and collection
func (m *MockMetadataManager) SetupDatabaseAndCollection(dbName, collName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.databases[dbName] = &metadata.Database{Name: dbName, CreatedAt: time.Now()}
	if m.collections[dbName] == nil {
		m.collections[dbName] = make(map[string]*metadata.Collection)
	}
	m.collections[dbName][collName] = &metadata.Collection{Name: collName, CreatedAt: time.Now()}
}

// createTestQueryService creates a QueryService for testing
func createTestQueryService(mockMeta *MockMetadataManager) *QueryService {
	logger := logging.NewDevelopment()
	cfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  2,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, cfg)
	queryCoord := coordinator.NewQueryCoordinator(logger, mockMeta, shardRouter)
	processor := processing.NewProcessor(logger)

	return NewQueryService(logger, mockMeta, queryCoord, processor)
}

func TestNewQueryService(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestQueryService(mockMeta)

	if service == nil {
		t.Fatal("Expected non-nil QueryService")
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
	if service.processor == nil {
		t.Error("Expected non-nil processor")
	}
}

func TestQueryService_Execute_CollectionNotFound(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	// Only add database, not collection
	mockMeta.databases["testdb"] = &metadata.Database{Name: "testdb", CreatedAt: time.Now()}

	service := createTestQueryService(mockMeta)

	ctx := context.Background()
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()

	req := &models.QueryRequest{
		Database:        "testdb",
		Collection:      "nonexistent",
		StartTimeParsed: startTime,
		EndTimeParsed:   endTime,
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

func TestQueryService_Execute_DatabaseNotFound(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	// Don't add any database
	service := createTestQueryService(mockMeta)

	ctx := context.Background()
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()

	req := &models.QueryRequest{
		Database:        "nonexistent",
		Collection:      "testcoll",
		StartTimeParsed: startTime,
		EndTimeParsed:   endTime,
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

func TestQueryService_Execute_Success(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")

	service := createTestQueryService(mockMeta)

	ctx := context.Background()
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()

	req := &models.QueryRequest{
		Database:        "testdb",
		Collection:      "testcoll",
		IDs:             []string{"device1"},
		StartTimeParsed: startTime,
		EndTimeParsed:   endTime,
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
}

func TestQueryService_Execute_WithAnomalyDetection(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")

	service := createTestQueryService(mockMeta)

	ctx := context.Background()
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()

	req := &models.QueryRequest{
		Database:         "testdb",
		Collection:       "testcoll",
		IDs:              []string{"device1"},
		StartTimeParsed:  startTime,
		EndTimeParsed:    endTime,
		AnomalyDetection: "zscore",
		AnomalyThreshold: 2.0,
		AnomalyField:     "temperature",
	}

	result, err := service.Execute(ctx, req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	// Anomalies may be empty if no data, but should not error
}

func TestQueryService_Execute_WithDownsampling(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")

	service := createTestQueryService(mockMeta)

	ctx := context.Background()
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()

	req := &models.QueryRequest{
		Database:              "testdb",
		Collection:            "testcoll",
		IDs:                   []string{"device1"},
		StartTimeParsed:       startTime,
		EndTimeParsed:         endTime,
		Downsampling:          "lttb",
		DownsamplingThreshold: 100,
	}

	result, err := service.Execute(ctx, req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

func TestQueryService_coordinatorToProcessing(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestQueryService(mockMeta)

	input := []coordinator.FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"},
			Fields: map[string][]interface{}{
				"temperature": {25.0, 26.0},
			},
		},
		{
			DeviceID: "device2",
			Times:    []string{"2024-01-01T00:00:00Z"},
			Fields: map[string][]interface{}{
				"humidity": {60.0},
			},
		},
	}

	result := service.coordinatorToProcessing(input)

	if len(result) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(result))
	}
	if result[0].DeviceID != "device1" {
		t.Errorf("Expected device1, got %s", result[0].DeviceID)
	}
	if len(result[0].Times) != 2 {
		t.Errorf("Expected 2 times, got %d", len(result[0].Times))
	}
}

func TestQueryService_processingToResponse(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestQueryService(mockMeta)

	input := []processing.QueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z"},
			Fields: map[string][]interface{}{
				"temperature": {25.0},
			},
		},
	}

	result := service.processingToResponse(input)

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}
	if result[0].DeviceID != "device1" {
		t.Errorf("Expected device1, got %s", result[0].DeviceID)
	}
}
