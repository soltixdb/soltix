package storage

import (
	"time"
)

// MainStorage is the interface for main storage backends
type MainStorage interface {
	// WriteBatch writes a batch of data points to storage
	WriteBatch(entries []*DataPoint) error

	// Query retrieves data points matching the criteria
	// deviceIDs: list of device IDs to query (empty = all devices)
	// startTime, endTime: time range
	// fields: list of fields to return (empty = all fields)
	Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error)

	// SetTimezone sets the timezone for date-based file organization
	SetTimezone(tz *time.Location)
}

// StorageAdapter wraps Storage to implement MainStorage interface
type StorageAdapter struct {
	*Storage
}

// NewStorageAdapter creates a new adapter
func NewStorageAdapter(ms *Storage) *StorageAdapter {
	return &StorageAdapter{Storage: ms}
}
