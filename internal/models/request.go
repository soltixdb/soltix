package models

// CreateDatabaseRequest represents create database request
type CreateDatabaseRequest struct {
	Name        string            `json:"name" validate:"required,min=1,max=64"`
	Description string            `json:"description,omitempty" validate:"max=256"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// FieldDefinition represents a field definition in a collection
type FieldDefinition struct {
	Name     string `json:"name" validate:"required"`
	Type     string `json:"type" validate:"required,oneof=timestamp string number boolean"`
	Required bool   `json:"required"`
}

// CreateCollectionRequest represents create collection request
type CreateCollectionRequest struct {
	Name        string            `json:"name" validate:"required,min=1,max=64"`
	Description string            `json:"description,omitempty" validate:"max=256"`
	SchemaHints map[string]string `json:"schema_hints,omitempty"`
	Fields      []FieldDefinition `json:"fields,omitempty"`
}

// UpdateCollectionFieldsRequest represents update collection fields request
type UpdateCollectionFieldsRequest struct {
	Description *string           `json:"description,omitempty" validate:"omitempty,max=256"`
	Fields      []FieldDefinition `json:"fields" validate:"required"`
}

// WriteRequest represents a single data point write request
type WriteRequest struct {
	Time   string                 `json:"time" validate:"required"`
	ID     string                 `json:"id" validate:"required"`
	Fields map[string]interface{} `json:"-"` // Dynamic fields
}

// WriteBatchRequest represents a batch write request
type WriteBatchRequest struct {
	Points []map[string]interface{} `json:"points" validate:"required,min=1"`
}

// TimeRange represents a time range filter
type TimeRange struct {
	Start string `json:"start" validate:"required"`
	End   string `json:"end" validate:"required"`
}

// Filter represents a field filter
type Filter struct {
	Field    string      `json:"field" validate:"required"`
	Operator string      `json:"operator" validate:"required,oneof== != > >= < <= IS_NULL IS_NOT_NULL"`
	Value    interface{} `json:"value"`
}
