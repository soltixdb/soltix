package aggregation

import (
	"time"
)

// =============================================================================
// Interfaces for avoiding circular dependencies with storage package
// =============================================================================

// RawDataReader reads raw data points from storage
// Implemented by storage.MainStorage
type RawDataReader interface {
	// Query retrieves data points matching the criteria
	// deviceIDs: list of device IDs to query (empty = all devices)
	// startTime, endTime: time range
	// fields: list of fields to return (empty = all fields)
	Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error)
}

// DataPointInterface represents a raw data point from storage
type DataPointInterface interface {
	GetID() string
	GetDatabase() string
	GetCollection() string
	GetTime() time.Time
	GetFields() map[string]interface{}
}

// AggregationStorage defines the interface for aggregation storage
// Both the old row-based Storage and new ColumnarStorage implement this
type AggregationStorage interface {
	// Write methods
	WriteHourly(database, collection string, points []*AggregatedPoint) error
	WriteDaily(database, collection string, points []*AggregatedPoint) error
	WriteMonthly(database, collection string, points []*AggregatedPoint) error
	WriteYearly(database, collection string, points []*AggregatedPoint) error

	// Read methods
	ReadHourly(database, collection, date string) ([]*AggregatedPoint, error)
	ReadHourlyForDay(database, collection string, day time.Time) ([]*AggregatedPoint, error)
	ReadDailyForMonth(database, collection string, month time.Time) ([]*AggregatedPoint, error)
	ReadMonthlyForYear(database, collection string, year time.Time) ([]*AggregatedPoint, error)
	ReadYearly(database, collection string) ([]*AggregatedPoint, error)

	// Configuration
	SetTimezone(tz *time.Location)
	GetTimezone() *time.Location
}
