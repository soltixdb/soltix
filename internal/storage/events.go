package storage

import "time"

// =============================================================================
// Event types for communication between storage components
// =============================================================================

// WriteNotification is sent from WriteWorker to FlushWorkerPool
// when data is written to WAL partition
type WriteNotification struct {
	Database   string
	Date       time.Time
	Collection string
	EntryCount int
	Immediate  bool // If true, flush immediately (for cold/historical data)
}

// PartitionKey generates a partition key from database and date
func PartitionKey(database string, date time.Time) string {
	return database + ":" + date.Format("2006-01-02")
}

// CollectionPartitionKey generates a key for collection-level partitioning
func CollectionPartitionKey(database, collection string, date time.Time) string {
	return database + ":" + collection + ":" + date.Format("2006-01-02")
}

// HourPartitionKey generates a key for hour-level partitioning
func HourPartitionKey(database, collection string, hour time.Time) string {
	return database + ":" + collection + ":" + hour.Format("2006-01-02T15")
}
