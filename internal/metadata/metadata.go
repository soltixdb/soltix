package metadata

import (
	"context"
	"time"
)

// Manager manages database and collection metadata
type Manager interface {
	// Database operations
	CreateDatabase(ctx context.Context, db *Database) error
	GetDatabase(ctx context.Context, name string) (*Database, error)
	ListDatabases(ctx context.Context) ([]*Database, error)
	DeleteDatabase(ctx context.Context, name string) error
	DatabaseExists(ctx context.Context, name string) (bool, error)

	// Collection operations
	CreateCollection(ctx context.Context, dbName string, coll *Collection) error
	GetCollection(ctx context.Context, dbName, collName string) (*Collection, error)
	ListCollections(ctx context.Context, dbName string) ([]*Collection, error)
	UpdateCollection(ctx context.Context, dbName string, coll *Collection) error
	DeleteCollection(ctx context.Context, dbName, collName string) error
	CollectionExists(ctx context.Context, dbName, collName string) (bool, error)
	ValidateCollection(ctx context.Context, dbName, collName string) error

	// Runtime tracking operations (with caching)
	TrackDeviceID(ctx context.Context, dbName, collName, deviceID string) error
	TrackField(ctx context.Context, dbName, collName, fieldName, fieldType string) error
	TrackFields(ctx context.Context, dbName, collName string, fieldSchemas map[string]string) error
	GetDeviceIDs(ctx context.Context, dbName, collName string) ([]string, error)
	GetFieldSchemas(ctx context.Context, dbName, collName string) (map[string]string, error)

	// Generic key-value operations (for shard management)
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	GetPrefix(ctx context.Context, prefix string) (map[string]string, error)

	// Lifecycle
	Close() error
}

// Database represents database metadata
type Database struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

// FieldInfo represents a field definition in a collection (metadata layer)
type FieldInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

// Collection represents collection metadata
type Collection struct {
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	SchemaHints   map[string]string `json:"schema_hints,omitempty"`
	Fields        []FieldInfo       `json:"fields,omitempty"`
	FirstDataTime *time.Time        `json:"first_data_time,omitempty"` // Earliest data timestamp (date only)
	// Runtime tracking (populated from actual data)
	DeviceIDs    map[string]bool   `json:"device_ids,omitempty"`    // Set of unique device IDs
	FieldSchemas map[string]string `json:"field_schemas,omitempty"` // Field name -> inferred type
	CreatedAt    time.Time         `json:"created_at"`
}
