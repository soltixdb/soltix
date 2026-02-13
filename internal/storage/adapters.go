package storage

import (
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
)

// RawDataReaderAdapter wraps MainStorage to implement aggregation.RawDataReader
type RawDataReaderAdapter struct {
	storage MainStorage
}

// NewRawDataReaderAdapter creates a new adapter
func NewRawDataReaderAdapter(storage MainStorage) *RawDataReaderAdapter {
	return &RawDataReaderAdapter{storage: storage}
}

// Query implements aggregation.RawDataReader
func (a *RawDataReaderAdapter) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]aggregation.DataPointInterface, error) {
	points, err := a.storage.Query(database, collection, deviceIDs, startTime, endTime, fields)
	if err != nil {
		return nil, err
	}

	// Convert []*DataPoint to []aggregation.DataPointInterface
	result := make([]aggregation.DataPointInterface, len(points))
	for i, p := range points {
		result[i] = p
	}
	return result, nil
}
