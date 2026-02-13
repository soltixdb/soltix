package models

// HealthResponse represents health check response
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Version   string `json:"version"`
}

// DatabaseResponse represents database metadata response
type DatabaseResponse struct {
	Name        string               `json:"name"`
	Description string               `json:"description,omitempty"`
	Metadata    map[string]string    `json:"metadata,omitempty"`
	CreatedAt   string               `json:"created_at"`
	Collections []CollectionResponse `json:"collections,omitempty"`
}

// DatabaseListResponse represents list databases response
type DatabaseListResponse struct {
	Databases []DatabaseResponse `json:"databases"`
}

// CollectionResponse represents collection metadata response
type CollectionResponse struct {
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	SchemaHints   map[string]string `json:"schema_hints,omitempty"`
	FirstDataTime *string           `json:"first_data_time,omitempty"` // Format: YYYY-MM-DD
	DeviceIDs     []string          `json:"device_ids,omitempty"`      // List of tracked device IDs
	Fields        map[string]string `json:"fields,omitempty"`          // Inferred field types from actual data
	CreatedAt     string            `json:"created_at"`
}

// CollectionListResponse represents list collections response
type CollectionListResponse struct {
	Collections []CollectionResponse `json:"collections"`
}

// WriteResponse represents write response
type WriteResponse struct {
	Accepted  bool   `json:"accepted"`
	RequestID string `json:"request_id"`
}

// WriteBatchResponse represents batch write response
type WriteBatchResponse struct {
	Accepted  int    `json:"accepted"`
	Rejected  int    `json:"rejected"`
	RequestID string `json:"request_id"`
}

// QueryResponse represents query response
type QueryResponse struct {
	Database   string                   `json:"database"`
	Collection string                   `json:"collection"`
	Query      interface{}              `json:"query"`
	Points     []map[string]interface{} `json:"points"`
	Count      int                      `json:"count"`
}

// DataPointView represents a single data point in query results
type DataPointView struct {
	Time   string                 `json:"time"`
	ID     string                 `json:"id"`
	Fields map[string]interface{} `json:"fields"`
}

// ErrorResponse represents error response
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail represents error details
type ErrorDetail struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Path    string                 `json:"path,omitempty"`
	Details map[string]interface{} `json:"details,omitempty"`
}
