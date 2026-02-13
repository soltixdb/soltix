package metadata

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

// setupTestEtcd creates an embedded etcd server for testing
func setupTestEtcd(t *testing.T) (*embed.Etcd, []string, func()) {
	// Create temp directory for etcd data
	tmpDir, err := os.MkdirTemp("", "etcd-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Configure embedded etcd
	cfg := embed.NewConfig()
	cfg.Dir = tmpDir

	// Use random available ports
	clientURL, _ := url.Parse("http://127.0.0.1:0")
	peerURL, _ := url.Parse("http://127.0.0.1:0")

	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}

	// Disable logging
	cfg.LogLevel = "error"
	cfg.Logger = "zap"

	// Start etcd server
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("Failed to start etcd: %v", err)
	}

	// Wait for etcd to be ready
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		e.Close()
		_ = os.RemoveAll(tmpDir)
		t.Fatal("Etcd server took too long to start")
	}

	endpoints := []string{e.Clients[0].Addr().String()}

	cleanup := func() {
		e.Close()
		_ = os.RemoveAll(tmpDir)
	}

	return e, endpoints, cleanup
}

func TestNewEtcdManager(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	if manager == nil {
		t.Fatal("Expected manager to be created")
	}

	if manager.client == nil {
		t.Error("Expected client to be initialized")
	}

	if manager.cache == nil {
		t.Error("Expected cache to be initialized")
	}
}

func TestNewEtcdManager_InvalidEndpoints(t *testing.T) {
	// Note: etcd client v3 may not fail immediately on invalid endpoints
	// It will fail when trying to perform operations
	manager, err := NewEtcdManager([]string{"invalid-endpoint:12345"})
	if err != nil {
		// Connection failed immediately - expected behavior
		return
	}

	if manager != nil {
		defer func() { _ = manager.Close() }()

		// Try an operation to trigger connection error
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err = manager.ListDatabases(ctx)
		if err == nil {
			t.Error("Expected error when performing operation with invalid endpoints")
		}
	}
}

func TestEtcdManager_DatabaseOperations(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	t.Run("CreateDatabase", func(t *testing.T) {
		db := &Database{
			Name:        "testdb",
			Description: "Test database",
			Metadata: map[string]string{
				"owner": "test-team",
			},
		}

		err := manager.CreateDatabase(ctx, db)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// Verify created_at was set
		if db.CreatedAt.IsZero() {
			t.Error("Expected created_at to be set")
		}
	})

	t.Run("CreateDatabase_AlreadyExists", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
		}

		err := manager.CreateDatabase(ctx, db)
		if err == nil {
			t.Error("Expected error when creating duplicate database")
		}
		if err != nil && !contains(err.Error(), "already exists") {
			t.Errorf("Expected 'already exists' error, got: %v", err)
		}
	})

	t.Run("DatabaseExists", func(t *testing.T) {
		exists, err := manager.DatabaseExists(ctx, "testdb")
		if err != nil {
			t.Fatalf("Failed to check database existence: %v", err)
		}
		if !exists {
			t.Error("Expected database to exist")
		}

		exists, err = manager.DatabaseExists(ctx, "nonexistent")
		if err != nil {
			t.Fatalf("Failed to check database existence: %v", err)
		}
		if exists {
			t.Error("Expected database to not exist")
		}
	})

	t.Run("GetDatabase", func(t *testing.T) {
		db, err := manager.GetDatabase(ctx, "testdb")
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		if db.Name != "testdb" {
			t.Errorf("Expected name 'testdb', got %q", db.Name)
		}
		if db.Description != "Test database" {
			t.Errorf("Expected description 'Test database', got %q", db.Description)
		}
		if db.Metadata["owner"] != "test-team" {
			t.Errorf("Expected metadata owner 'test-team', got %q", db.Metadata["owner"])
		}
	})

	t.Run("GetDatabase_NotFound", func(t *testing.T) {
		_, err := manager.GetDatabase(ctx, "nonexistent")
		if err == nil {
			t.Error("Expected error when getting nonexistent database")
		}
		if err != nil && !contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' error, got: %v", err)
		}
	})

	t.Run("ListDatabases", func(t *testing.T) {
		// Create another database
		db2 := &Database{
			Name: "testdb2",
		}
		err := manager.CreateDatabase(ctx, db2)
		if err != nil {
			t.Fatalf("Failed to create second database: %v", err)
		}

		databases, err := manager.ListDatabases(ctx)
		if err != nil {
			t.Fatalf("Failed to list databases: %v", err)
		}

		if len(databases) < 2 {
			t.Errorf("Expected at least 2 databases, got %d", len(databases))
		}

		// Verify databases are in the list
		found := make(map[string]bool)
		for _, db := range databases {
			found[db.Name] = true
		}
		if !found["testdb"] {
			t.Error("Expected 'testdb' in list")
		}
		if !found["testdb2"] {
			t.Error("Expected 'testdb2' in list")
		}
	})

	t.Run("DeleteDatabase", func(t *testing.T) {
		err := manager.DeleteDatabase(ctx, "testdb2")
		if err != nil {
			t.Fatalf("Failed to delete database: %v", err)
		}

		// Verify it's gone
		exists, err := manager.DatabaseExists(ctx, "testdb2")
		if err != nil {
			t.Fatalf("Failed to check database existence: %v", err)
		}
		if exists {
			t.Error("Expected database to be deleted")
		}
	})
}

func TestEtcdManager_CollectionOperations(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	// Create a database first
	db := &Database{Name: "testdb"}
	if err := manager.CreateDatabase(ctx, db); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Run("CreateCollection", func(t *testing.T) {
		coll := &Collection{
			Name:        "testcoll",
			Description: "Test collection",
			SchemaHints: map[string]string{
				"temperature": "float",
				"humidity":    "float",
			},
		}

		err := manager.CreateCollection(ctx, "testdb", coll)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}

		if coll.CreatedAt.IsZero() {
			t.Error("Expected created_at to be set")
		}
	})

	t.Run("CreateCollection_DatabaseNotFound", func(t *testing.T) {
		coll := &Collection{Name: "testcoll"}
		err := manager.CreateCollection(ctx, "nonexistent", coll)
		if err == nil {
			t.Error("Expected error when database doesn't exist")
		}
		if err != nil && !contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' error, got: %v", err)
		}
	})

	t.Run("CreateCollection_AlreadyExists", func(t *testing.T) {
		coll := &Collection{Name: "testcoll"}
		err := manager.CreateCollection(ctx, "testdb", coll)
		if err == nil {
			t.Error("Expected error when creating duplicate collection")
		}
		if err != nil && !contains(err.Error(), "already exists") {
			t.Errorf("Expected 'already exists' error, got: %v", err)
		}
	})

	t.Run("CollectionExists", func(t *testing.T) {
		exists, err := manager.CollectionExists(ctx, "testdb", "testcoll")
		if err != nil {
			t.Fatalf("Failed to check collection existence: %v", err)
		}
		if !exists {
			t.Error("Expected collection to exist")
		}

		exists, err = manager.CollectionExists(ctx, "testdb", "nonexistent")
		if err != nil {
			t.Fatalf("Failed to check collection existence: %v", err)
		}
		if exists {
			t.Error("Expected collection to not exist")
		}
	})

	t.Run("GetCollection", func(t *testing.T) {
		coll, err := manager.GetCollection(ctx, "testdb", "testcoll")
		if err != nil {
			t.Fatalf("Failed to get collection: %v", err)
		}

		if coll.Name != "testcoll" {
			t.Errorf("Expected name 'testcoll', got %q", coll.Name)
		}
		if coll.Description != "Test collection" {
			t.Errorf("Expected description 'Test collection', got %q", coll.Description)
		}
		if coll.SchemaHints["temperature"] != "float" {
			t.Errorf("Expected schema hint temperature='float', got %q", coll.SchemaHints["temperature"])
		}
	})

	t.Run("GetCollection_WithCache", func(t *testing.T) {
		// First call - should cache
		_, err := manager.GetCollection(ctx, "testdb", "testcoll")
		if err != nil {
			t.Fatalf("Failed to get collection: %v", err)
		}

		// Second call - should use cache
		coll, err := manager.GetCollection(ctx, "testdb", "testcoll")
		if err != nil {
			t.Fatalf("Failed to get collection from cache: %v", err)
		}
		if coll.Name != "testcoll" {
			t.Errorf("Expected cached collection name 'testcoll', got %q", coll.Name)
		}
	})

	t.Run("GetCollection_NotFound", func(t *testing.T) {
		_, err := manager.GetCollection(ctx, "testdb", "nonexistent")
		if err == nil {
			t.Error("Expected error when getting nonexistent collection")
		}
		if err != nil && !contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' error, got: %v", err)
		}
	})

	t.Run("ListCollections", func(t *testing.T) {
		// Create another collection
		coll2 := &Collection{Name: "testcoll2"}
		err := manager.CreateCollection(ctx, "testdb", coll2)
		if err != nil {
			t.Fatalf("Failed to create second collection: %v", err)
		}

		collections, err := manager.ListCollections(ctx, "testdb")
		if err != nil {
			t.Fatalf("Failed to list collections: %v", err)
		}

		if len(collections) < 2 {
			t.Errorf("Expected at least 2 collections, got %d", len(collections))
		}

		found := make(map[string]bool)
		for _, coll := range collections {
			found[coll.Name] = true
		}
		if !found["testcoll"] {
			t.Error("Expected 'testcoll' in list")
		}
		if !found["testcoll2"] {
			t.Error("Expected 'testcoll2' in list")
		}
	})

	t.Run("ListCollections_DatabaseNotFound", func(t *testing.T) {
		_, err := manager.ListCollections(ctx, "nonexistent")
		if err == nil {
			t.Error("Expected error when database doesn't exist")
		}
		if err != nil && !contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' error, got: %v", err)
		}
	})

	t.Run("ValidateCollection", func(t *testing.T) {
		err := manager.ValidateCollection(ctx, "testdb", "testcoll")
		if err != nil {
			t.Errorf("Expected validation to pass, got error: %v", err)
		}

		err = manager.ValidateCollection(ctx, "testdb", "nonexistent")
		if err == nil {
			t.Error("Expected validation to fail for nonexistent collection")
		}
	})

	t.Run("DeleteCollection", func(t *testing.T) {
		err := manager.DeleteCollection(ctx, "testdb", "testcoll2")
		if err != nil {
			t.Fatalf("Failed to delete collection: %v", err)
		}

		exists, err := manager.CollectionExists(ctx, "testdb", "testcoll2")
		if err != nil {
			t.Fatalf("Failed to check collection existence: %v", err)
		}
		if exists {
			t.Error("Expected collection to be deleted")
		}
	})
}

func TestEtcdManager_KeyValueOperations(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	t.Run("Put_and_Get", func(t *testing.T) {
		err := manager.Put(ctx, "/test/key1", "value1")
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		value, err := manager.Get(ctx, "/test/key1")
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}

		if value != "value1" {
			t.Errorf("Expected value 'value1', got %q", value)
		}
	})

	t.Run("Get_WithCache", func(t *testing.T) {
		// First call - should cache
		value, err := manager.Get(ctx, "/test/key1")
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}
		if value != "value1" {
			t.Errorf("Expected value 'value1', got %q", value)
		}

		// Second call - should use cache
		value, err = manager.Get(ctx, "/test/key1")
		if err != nil {
			t.Fatalf("Failed to get key from cache: %v", err)
		}
		if value != "value1" {
			t.Errorf("Expected cached value 'value1', got %q", value)
		}
	})

	t.Run("Get_NonExistent", func(t *testing.T) {
		value, err := manager.Get(ctx, "/test/nonexistent")
		if err != nil {
			t.Fatalf("Failed to get nonexistent key: %v", err)
		}
		if value != "" {
			t.Errorf("Expected empty value for nonexistent key, got %q", value)
		}
	})

	t.Run("GetPrefix", func(t *testing.T) {
		// Put multiple keys with same prefix
		_ = manager.Put(ctx, "/test/prefix/key1", "value1")
		_ = manager.Put(ctx, "/test/prefix/key2", "value2")
		_ = manager.Put(ctx, "/test/other/key3", "value3")

		result, err := manager.GetPrefix(ctx, "/test/prefix")
		if err != nil {
			t.Fatalf("Failed to get prefix: %v", err)
		}

		if len(result) < 2 {
			t.Errorf("Expected at least 2 keys with prefix, got %d", len(result))
		}

		if result["/test/prefix/key1"] != "value1" {
			t.Errorf("Expected value 'value1', got %q", result["/test/prefix/key1"])
		}
		if result["/test/prefix/key2"] != "value2" {
			t.Errorf("Expected value 'value2', got %q", result["/test/prefix/key2"])
		}

		// Should not include keys with different prefix
		if _, ok := result["/test/other/key3"]; ok {
			t.Error("Expected /test/other/key3 to not be in result")
		}
	})
}

func TestEtcdManager_CacheInvalidation(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	// Create database and collection
	db := &Database{Name: "cachedb"}
	_ = manager.CreateDatabase(ctx, db)

	coll := &Collection{Name: "cachecoll"}
	_ = manager.CreateCollection(ctx, "cachedb", coll)

	// Get collection to cache it
	_, _ = manager.GetCollection(ctx, "cachedb", "cachecoll")

	t.Run("DeleteDatabase_InvalidatesCache", func(t *testing.T) {
		err := manager.DeleteDatabase(ctx, "cachedb")
		if err != nil {
			t.Fatalf("Failed to delete database: %v", err)
		}

		// Cache should be invalidated
		_, err = manager.GetCollection(ctx, "cachedb", "cachecoll")
		if err == nil {
			t.Error("Expected error after database deletion, but got collection from cache")
		}
	})
}

func TestEtcdManager_Close(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}

	err = manager.Close()
	if err != nil {
		t.Errorf("Failed to close manager: %v", err)
	}
}

func TestEtcdManager_TrackDeviceID(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	// Create database and collection
	db := &Database{
		Name:      "testdb",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateDatabase(ctx, db); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	coll := &Collection{
		Name:      "testcoll",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateCollection(ctx, "testdb", coll); err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Track device IDs
	deviceIDs := []string{"device_001", "device_002", "device_003"}
	for _, deviceID := range deviceIDs {
		if err := manager.TrackDeviceID(ctx, "testdb", "testcoll", deviceID); err != nil {
			t.Fatalf("Failed to track device ID %s: %v", deviceID, err)
		}
	}

	// Get tracked device IDs
	trackedIDs, err := manager.GetDeviceIDs(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get device IDs: %v", err)
	}

	if len(trackedIDs) != len(deviceIDs) {
		t.Errorf("Expected %d device IDs, got %d", len(deviceIDs), len(trackedIDs))
	}

	// Verify all device IDs are present
	deviceIDMap := make(map[string]bool)
	for _, id := range trackedIDs {
		deviceIDMap[id] = true
	}

	for _, expectedID := range deviceIDs {
		if !deviceIDMap[expectedID] {
			t.Errorf("Expected device ID %s not found in tracked IDs", expectedID)
		}
	}

	// Track duplicate device ID (should not cause error)
	if err := manager.TrackDeviceID(ctx, "testdb", "testcoll", "device_001"); err != nil {
		t.Fatalf("Failed to track duplicate device ID: %v", err)
	}

	// Verify count hasn't changed
	trackedIDs, err = manager.GetDeviceIDs(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get device IDs: %v", err)
	}

	if len(trackedIDs) != len(deviceIDs) {
		t.Errorf("Expected %d device IDs after duplicate, got %d", len(deviceIDs), len(trackedIDs))
	}
}

func TestEtcdManager_TrackField(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	// Create database and collection
	db := &Database{
		Name:      "testdb",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateDatabase(ctx, db); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	coll := &Collection{
		Name:      "testcoll",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateCollection(ctx, "testdb", coll); err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Track fields
	fields := map[string]string{
		"temperature": "float",
		"humidity":    "float",
		"location":    "string",
		"active":      "bool",
	}

	for fieldName, fieldType := range fields {
		if err := manager.TrackField(ctx, "testdb", "testcoll", fieldName, fieldType); err != nil {
			t.Fatalf("Failed to track field %s: %v", fieldName, err)
		}
	}

	// Get tracked field schemas
	trackedSchemas, err := manager.GetFieldSchemas(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get field schemas: %v", err)
	}

	if len(trackedSchemas) != len(fields) {
		t.Errorf("Expected %d fields, got %d", len(fields), len(trackedSchemas))
	}

	// Verify all fields are present with correct types
	for fieldName, expectedType := range fields {
		actualType, exists := trackedSchemas[fieldName]
		if !exists {
			t.Errorf("Expected field %s not found in tracked schemas", fieldName)
		} else if actualType != expectedType {
			t.Errorf("Field %s: expected type %s, got %s", fieldName, expectedType, actualType)
		}
	}

	// Track duplicate field with same type (should not cause error)
	if err := manager.TrackField(ctx, "testdb", "testcoll", "temperature", "float"); err != nil {
		t.Fatalf("Failed to track duplicate field: %v", err)
	}

	// Track field with different type (should update)
	if err := manager.TrackField(ctx, "testdb", "testcoll", "temperature", "int"); err != nil {
		t.Fatalf("Failed to update field type: %v", err)
	}

	trackedSchemas, err = manager.GetFieldSchemas(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get field schemas: %v", err)
	}

	if trackedSchemas["temperature"] != "int" {
		t.Errorf("Expected temperature type to be updated to int, got %s", trackedSchemas["temperature"])
	}
}

func TestEtcdManager_TrackingWithCache(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	// Create database and collection
	db := &Database{
		Name:      "testdb",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateDatabase(ctx, db); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	coll := &Collection{
		Name:      "testcoll",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateCollection(ctx, "testdb", coll); err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Track device ID
	if err := manager.TrackDeviceID(ctx, "testdb", "testcoll", "device_001"); err != nil {
		t.Fatalf("Failed to track device ID: %v", err)
	}

	// Get device IDs (should use cache)
	deviceIDs1, err := manager.GetDeviceIDs(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get device IDs: %v", err)
	}

	// Get device IDs again (should use cache)
	deviceIDs2, err := manager.GetDeviceIDs(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get device IDs second time: %v", err)
	}

	if len(deviceIDs1) != len(deviceIDs2) {
		t.Error("Cache returned different results")
	}

	// Track field
	if err := manager.TrackField(ctx, "testdb", "testcoll", "temperature", "float"); err != nil {
		t.Fatalf("Failed to track field: %v", err)
	}

	// Get field schemas (should use cache)
	schemas1, err := manager.GetFieldSchemas(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get field schemas: %v", err)
	}

	// Get field schemas again (should use cache)
	schemas2, err := manager.GetFieldSchemas(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get field schemas second time: %v", err)
	}

	if len(schemas1) != len(schemas2) {
		t.Error("Cache returned different results for field schemas")
	}
}

func TestEtcdManager_TrackFields(t *testing.T) {
	_, endpoints, cleanup := setupTestEtcd(t)
	defer cleanup()

	manager, err := NewEtcdManager(endpoints)
	if err != nil {
		t.Fatalf("Failed to create EtcdManager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	ctx := context.Background()

	// Create database and collection
	db := &Database{
		Name:      "testdb",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateDatabase(ctx, db); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	coll := &Collection{
		Name:      "testcoll",
		CreatedAt: time.Now(),
	}
	if err := manager.CreateCollection(ctx, "testdb", coll); err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Track multiple fields at once
	fieldSchemas := map[string]string{
		"temperature": "float",
		"humidity":    "float",
		"pressure":    "float",
		"location":    "string",
		"active":      "bool",
		"count":       "int",
		"metadata":    "object",
		"tags":        "array",
	}

	if err := manager.TrackFields(ctx, "testdb", "testcoll", fieldSchemas); err != nil {
		t.Fatalf("Failed to track fields: %v", err)
	}

	// Get tracked field schemas
	trackedSchemas, err := manager.GetFieldSchemas(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get field schemas: %v", err)
	}

	if len(trackedSchemas) != len(fieldSchemas) {
		t.Errorf("Expected %d fields, got %d", len(fieldSchemas), len(trackedSchemas))
	}

	// Verify all fields are present with correct types
	for fieldName, expectedType := range fieldSchemas {
		actualType, exists := trackedSchemas[fieldName]
		if !exists {
			t.Errorf("Expected field %s not found in tracked schemas", fieldName)
		} else if actualType != expectedType {
			t.Errorf("Field %s: expected type %s, got %s", fieldName, expectedType, actualType)
		}
	}

	// Track batch with duplicate fields (should not cause error)
	duplicateSchemas := map[string]string{
		"temperature": "float", // Same type
		"humidity":    "float", // Same type
	}
	if err := manager.TrackFields(ctx, "testdb", "testcoll", duplicateSchemas); err != nil {
		t.Fatalf("Failed to track duplicate fields: %v", err)
	}

	// Track batch with updated types
	updatedSchemas := map[string]string{
		"temperature": "int",    // Changed type
		"status":      "string", // New field
	}
	if err := manager.TrackFields(ctx, "testdb", "testcoll", updatedSchemas); err != nil {
		t.Fatalf("Failed to update field types: %v", err)
	}

	// Verify updates
	trackedSchemas, err = manager.GetFieldSchemas(ctx, "testdb", "testcoll")
	if err != nil {
		t.Fatalf("Failed to get field schemas after update: %v", err)
	}

	if trackedSchemas["temperature"] != "int" {
		t.Errorf("Expected temperature type to be updated to int, got %s", trackedSchemas["temperature"])
	}

	if trackedSchemas["status"] != "string" {
		t.Errorf("Expected status field to be added, got %v", trackedSchemas["status"])
	}

	// Original fields should still be present (except updated ones)
	expectedFields := len(fieldSchemas) + 1 // Original fields + 1 new field (status)
	if len(trackedSchemas) != expectedFields {
		t.Errorf("Expected %d total fields after update, got %d", expectedFields, len(trackedSchemas))
	}

	// Test empty batch (should not error)
	emptySchemas := map[string]string{}
	if err := manager.TrackFields(ctx, "testdb", "testcoll", emptySchemas); err != nil {
		t.Fatalf("Failed to track empty batch: %v", err)
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(filepath.Base(s) == substr || len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
			len(s) >= len(substr) && s[:len(substr)] == substr ||
			len(s) > len(substr) && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
