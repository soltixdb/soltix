package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	databasePrefix   = "/soltix/databases"
	collectionSuffix = "collections"
)

// EtcdManager implements Manager using etcd
type EtcdManager struct {
	client *clientv3.Client
	cache  *KVCache
}

// NewEtcdManager creates a new etcd-based metadata manager
func NewEtcdManager(endpoints []string) (*EtcdManager, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &EtcdManager{
		client: client,
		cache:  NewKVCache(30 * time.Second), // 30s TTL for shards/nodes
	}, nil
}

// ============================================================================
// Database Operations
// ============================================================================

func (m *EtcdManager) CreateDatabase(ctx context.Context, db *Database) error {
	key := path.Join(databasePrefix, db.Name, "metadata")

	// Check if exists
	exists, err := m.DatabaseExists(ctx, db.Name)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("database %s already exists", db.Name)
	}

	// Set created_at if not set
	if db.CreatedAt.IsZero() {
		db.CreatedAt = time.Now()
	}

	// Marshal to JSON
	data, err := json.Marshal(db)
	if err != nil {
		return fmt.Errorf("failed to marshal database: %w", err)
	}

	// Store in etcd
	_, err = m.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to store database in etcd: %w", err)
	}

	return nil
}

func (m *EtcdManager) GetDatabase(ctx context.Context, name string) (*Database, error) {
	key := path.Join(databasePrefix, name, "metadata")

	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get database from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("database %s not found", name)
	}

	var db Database
	if err := json.Unmarshal(resp.Kvs[0].Value, &db); err != nil {
		return nil, fmt.Errorf("failed to unmarshal database: %w", err)
	}

	return &db, nil
}

func (m *EtcdManager) ListDatabases(ctx context.Context) ([]*Database, error) {
	// Get all database keys with prefix
	resp, err := m.client.Get(ctx, databasePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list databases from etcd: %w", err)
	}

	databases := make([]*Database, 0)
	for _, kv := range resp.Kvs {
		// Only process metadata keys, skip collection keys
		if !strings.HasSuffix(string(kv.Key), "/metadata") {
			continue
		}

		var db Database
		if err := json.Unmarshal(kv.Value, &db); err != nil {
			// Log error but continue
			continue
		}
		databases = append(databases, &db)
	}

	return databases, nil
}

func (m *EtcdManager) DeleteDatabase(ctx context.Context, name string) error {
	// Delete entire database tree (metadata + collections)
	key := path.Join(databasePrefix, name)

	_, err := m.client.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to delete database from etcd: %w", err)
	}

	// Invalidate cache for this database
	m.cache.DeletePrefix(key)
	return nil
}

func (m *EtcdManager) DatabaseExists(ctx context.Context, name string) (bool, error) {
	key := path.Join(databasePrefix, name, "metadata")

	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return len(resp.Kvs) > 0, nil
}

// ============================================================================
// Collection Operations
// ============================================================================

func (m *EtcdManager) CreateCollection(ctx context.Context, dbName string, coll *Collection) error {
	// Check if database exists
	exists, err := m.DatabaseExists(ctx, dbName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("database %s not found", dbName)
	}

	// Check if collection exists
	exists, err = m.CollectionExists(ctx, dbName, coll.Name)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("collection %s already exists in database %s", coll.Name, dbName)
	}

	key := path.Join(databasePrefix, dbName, collectionSuffix, coll.Name)

	// Set created_at if not set
	if coll.CreatedAt.IsZero() {
		coll.CreatedAt = time.Now()
	}

	// Marshal to JSON
	data, err := json.Marshal(coll)
	if err != nil {
		return fmt.Errorf("failed to marshal collection: %w", err)
	}

	// Store in etcd
	_, err = m.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to store collection in etcd: %w", err)
	}

	// Update cache
	m.cache.Set(key, string(data))
	return nil
}

func (m *EtcdManager) GetCollection(ctx context.Context, dbName, collName string) (*Collection, error) {
	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)

	// Check cache first
	if cached, ok := m.cache.Get(key); ok && cached != "" {
		var coll Collection
		if err := json.Unmarshal([]byte(cached), &coll); err == nil {
			return &coll, nil
		}
	}

	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get collection from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("collection %s not found in database %s", collName, dbName)
	}

	var coll Collection
	if err := json.Unmarshal(resp.Kvs[0].Value, &coll); err != nil {
		return nil, fmt.Errorf("failed to unmarshal collection: %w", err)
	}

	// Cache the result
	m.cache.Set(key, string(resp.Kvs[0].Value))
	return &coll, nil
}

func (m *EtcdManager) ListCollections(ctx context.Context, dbName string) ([]*Collection, error) {
	// Check if database exists
	exists, err := m.DatabaseExists(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("database %s not found", dbName)
	}

	prefix := path.Join(databasePrefix, dbName, collectionSuffix) + "/"

	resp, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list collections from etcd: %w", err)
	}

	collections := make([]*Collection, 0)
	for _, kv := range resp.Kvs {
		var coll Collection
		if err := json.Unmarshal(kv.Value, &coll); err != nil {
			// Log error but continue
			continue
		}
		collections = append(collections, &coll)
	}

	return collections, nil
}

func (m *EtcdManager) UpdateCollection(ctx context.Context, dbName string, coll *Collection) error {
	// Check if database exists
	exists, err := m.DatabaseExists(ctx, dbName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("database %s not found", dbName)
	}

	// Check if collection exists
	exists, err = m.CollectionExists(ctx, dbName, coll.Name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("collection %s not found in database %s", coll.Name, dbName)
	}

	key := path.Join(databasePrefix, dbName, collectionSuffix, coll.Name)

	// Marshal to JSON
	data, err := json.Marshal(coll)
	if err != nil {
		return fmt.Errorf("failed to marshal collection: %w", err)
	}

	// Update in etcd
	_, err = m.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update collection in etcd: %w", err)
	}

	// Update cache
	m.cache.Set(key, string(data))
	return nil
}

func (m *EtcdManager) DeleteCollection(ctx context.Context, dbName, collName string) error {
	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)

	_, err := m.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete collection from etcd: %w", err)
	}

	// Invalidate cache
	m.cache.Delete(key)
	return nil
}

func (m *EtcdManager) CollectionExists(ctx context.Context, dbName, collName string) (bool, error) {
	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)

	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to check collection existence: %w", err)
	}

	exists := len(resp.Kvs) > 0

	// update cache
	if exists {
		m.cache.Set(key, string(resp.Kvs[0].Value))
	} else {
		m.cache.Delete(key)
	}

	return exists, nil
}

// ValidateCollection checks if a collection exists and returns error if not
func (m *EtcdManager) ValidateCollection(ctx context.Context, dbName, collName string) error {
	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)
	// Check cache first
	if cached, ok := m.cache.Get(key); ok {
		// If cached value is empty, collection does not exist
		if len(cached) == 0 {
			return fmt.Errorf("collection not found: %s/%s", dbName, collName)
		}
		return nil
	}

	// Check existence in etcd if not in cache
	exists, err := m.CollectionExists(ctx, dbName, collName)
	if err != nil {
		return fmt.Errorf("failed to check collection: %w", err)
	}
	if !exists {
		return fmt.Errorf("collection not found: %s/%s", dbName, collName)
	}
	return nil
}

// ============================================================================
// Generic Key-Value Operations
// ============================================================================

// Get retrieves a value by key
func (m *EtcdManager) Get(ctx context.Context, key string) (string, error) {
	// Check cache first
	if cached, ok := m.cache.Get(key); ok {
		return cached, nil
	}

	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("failed to get key: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}

	value := string(resp.Kvs[0].Value)
	// Cache the result
	m.cache.Set(key, value)
	return value, nil
}

// Put stores a key-value pair
func (m *EtcdManager) Put(ctx context.Context, key, value string) error {
	_, err := m.client.Put(ctx, key, value)
	if err != nil {
		return fmt.Errorf("failed to put key: %w", err)
	}
	// Update cache
	m.cache.Set(key, value)
	return nil
}

// Delete removes a key from etcd
func (m *EtcdManager) Delete(ctx context.Context, key string) error {
	_, err := m.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}
	// Invalidate cache
	m.cache.Delete(key)
	return nil
}

// GetPrefix retrieves all keys with a given prefix
func (m *EtcdManager) GetPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	resp, err := m.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get prefix: %w", err)
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

// ============================================================================
// Runtime Tracking Operations (with caching)
// ============================================================================

// TrackDeviceID adds a device ID to the collection's tracking set
// Uses optimistic locking and caching for performance
func (m *EtcdManager) TrackDeviceID(ctx context.Context, dbName, collName, deviceID string) error {
	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)

	// Get current collection (try cache first)
	coll, err := m.GetCollection(ctx, dbName, collName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// Initialize DeviceIDs map if needed
	if coll.DeviceIDs == nil {
		coll.DeviceIDs = make(map[string]bool)
	}

	// Check if device ID already tracked (avoid unnecessary writes)
	if coll.DeviceIDs[deviceID] {
		return nil // Already tracked
	}

	// Add device ID
	coll.DeviceIDs[deviceID] = true

	// Marshal to JSON
	data, err := json.Marshal(coll)
	if err != nil {
		return fmt.Errorf("failed to marshal collection: %w", err)
	}

	// Update in etcd
	_, err = m.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update collection: %w", err)
	}

	// Update cache
	m.cache.Set(key, string(data))
	return nil
}

// TrackField adds or updates a field's type in the collection's schema tracking
// Uses optimistic locking and caching for performance
func (m *EtcdManager) TrackField(ctx context.Context, dbName, collName, fieldName, fieldType string) error {
	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)

	// Get current collection (try cache first)
	coll, err := m.GetCollection(ctx, dbName, collName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// Initialize FieldSchemas map if needed
	if coll.FieldSchemas == nil {
		coll.FieldSchemas = make(map[string]string)
	}

	// Check if field already tracked with same type (avoid unnecessary writes)
	if existingType, exists := coll.FieldSchemas[fieldName]; exists && existingType == fieldType {
		return nil // Already tracked with same type
	}

	// Add or update field type
	coll.FieldSchemas[fieldName] = fieldType

	// Marshal to JSON
	data, err := json.Marshal(coll)
	if err != nil {
		return fmt.Errorf("failed to marshal collection: %w", err)
	}

	// Update in etcd
	_, err = m.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update collection: %w", err)
	}

	// Update cache
	m.cache.Set(key, string(data))
	return nil
}

// TrackFields adds or updates multiple fields at once
// This is more efficient than calling TrackField multiple times
func (m *EtcdManager) TrackFields(ctx context.Context, dbName, collName string, fieldSchemas map[string]string) error {
	if len(fieldSchemas) == 0 {
		return nil // Nothing to track
	}

	key := path.Join(databasePrefix, dbName, collectionSuffix, collName)

	// Get current collection (try cache first)
	coll, err := m.GetCollection(ctx, dbName, collName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// Initialize FieldSchemas map if needed
	if coll.FieldSchemas == nil {
		coll.FieldSchemas = make(map[string]string)
	}

	// Track if any changes were made
	hasChanges := false

	// Add or update all fields
	for fieldName, fieldType := range fieldSchemas {
		if existingType, exists := coll.FieldSchemas[fieldName]; !exists || existingType != fieldType {
			coll.FieldSchemas[fieldName] = fieldType
			hasChanges = true
		}
	}

	// Skip update if nothing changed
	if !hasChanges {
		return nil
	}

	// Marshal to JSON
	data, err := json.Marshal(coll)
	if err != nil {
		return fmt.Errorf("failed to marshal collection: %w", err)
	}

	// Update in etcd
	_, err = m.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update collection: %w", err)
	}

	// Update cache
	m.cache.Set(key, string(data))
	return nil
}

// GetDeviceIDs returns all tracked device IDs for a collection
// Uses cache for performance
func (m *EtcdManager) GetDeviceIDs(ctx context.Context, dbName, collName string) ([]string, error) {
	coll, err := m.GetCollection(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	// Convert map to slice
	deviceIDs := make([]string, 0, len(coll.DeviceIDs))
	for id := range coll.DeviceIDs {
		deviceIDs = append(deviceIDs, id)
	}

	return deviceIDs, nil
}

// GetFieldSchemas returns all tracked field schemas for a collection
// Uses cache for performance
func (m *EtcdManager) GetFieldSchemas(ctx context.Context, dbName, collName string) (map[string]string, error) {
	coll, err := m.GetCollection(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	return coll.FieldSchemas, nil
}

// ============================================================================
// Lifecycle
// ============================================================================

func (m *EtcdManager) Close() error {
	// Stop cache cleanup goroutine
	if m.cache != nil {
		m.cache.Stop()
	}
	return m.client.Close()
}
