package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	"github.com/soltixdb/soltix/internal/wal"
)

// LocalStorageAdapter adapts storage.Storage to LocalStorage interface
type LocalStorageAdapter struct {
	columnarStorage *storage.Storage
	memoryStore     *storage.MemoryStore
	writeWorkerPool *storage.WriteWorkerPool
	logger          *logging.Logger
}

// NewLocalStorageAdapter creates a new local storage adapter
func NewLocalStorageAdapter(
	columnarStorage *storage.Storage,
	memoryStore *storage.MemoryStore,
	writeWorkerPool *storage.WriteWorkerPool,
	logger *logging.Logger,
) *LocalStorageAdapter {
	return &LocalStorageAdapter{
		columnarStorage: columnarStorage,
		memoryStore:     memoryStore,
		writeWorkerPool: writeWorkerPool,
		logger:          logger,
	}
}

// GetLastTimestamp returns the last timestamp stored for a shard
func (a *LocalStorageAdapter) GetLastTimestamp(database, collection string, startTime, endTime time.Time) (time.Time, error) {
	// Query from both memory store and file storage
	var lastTS time.Time

	// Check memory store first (hot data)
	deviceIDs := a.memoryStore.GetDeviceIDs(database, collection)
	for _, deviceID := range deviceIDs {
		points, err := a.memoryStore.Query(database, collection, deviceID, startTime, endTime)
		if err != nil {
			continue
		}
		for _, p := range points {
			if p.Time.After(lastTS) {
				lastTS = p.Time
			}
		}
	}

	// Check file storage (cold data)
	if a.columnarStorage != nil {
		filePoints, err := a.columnarStorage.Query(database, collection, nil, startTime, endTime, nil)
		if err == nil {
			for _, p := range filePoints {
				if p.Time.After(lastTS) {
					lastTS = p.Time
				}
			}
		}
	}

	if lastTS.IsZero() {
		return time.Time{}, fmt.Errorf("no data found for %s/%s", database, collection)
	}

	return lastTS, nil
}

// WriteToWAL writes data through WAL for proper durability and replication.
// This follows the same flow as normal push data:
// WAL Entry -> WriteWorkerPool -> WAL -> MemoryStore -> Flush to Storage
func (a *LocalStorageAdapter) WriteToWAL(entries []*wal.Entry) error {
	if a.writeWorkerPool == nil {
		return fmt.Errorf("write worker pool is not available")
	}

	for _, entry := range entries {
		// Convert WAL entry to WriteMessage
		msg := storage.WriteMessage{
			Database:   entry.Database,
			Collection: entry.Collection,
			ShardID:    entry.ShardID,
			Time:       entry.Time,
			ID:         entry.ID,
			Fields:     entry.Fields,
		}

		// Submit to write worker pool (async write through WAL)
		if err := a.writeWorkerPool.Submit(msg); err != nil {
			return fmt.Errorf("failed to submit to write worker pool: %w", err)
		}
	}

	return nil
}

// WriteToWALSync writes data through WAL synchronously (waits for each write to complete)
func (a *LocalStorageAdapter) WriteToWALSync(entries []*wal.Entry) error {
	if a.writeWorkerPool == nil {
		return fmt.Errorf("write worker pool is not available")
	}

	for _, entry := range entries {
		// Convert WAL entry to WriteMessage
		msg := storage.WriteMessage{
			Database:   entry.Database,
			Collection: entry.Collection,
			ShardID:    entry.ShardID,
			Time:       entry.Time,
			ID:         entry.ID,
			Fields:     entry.Fields,
		}

		// Submit synchronously (waits for completion)
		if err := a.writeWorkerPool.SubmitSync(msg); err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}
	}

	return nil
}

// Query reads data points from storage
func (a *LocalStorageAdapter) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*storage.DataPoint, error) {
	var allPoints []*storage.DataPoint

	// Query memory store
	if len(deviceIDs) == 0 {
		deviceIDs = a.memoryStore.GetDeviceIDs(database, collection)
	}

	for _, deviceID := range deviceIDs {
		points, err := a.memoryStore.Query(database, collection, deviceID, startTime, endTime)
		if err != nil {
			a.logger.Warn("Failed to query memory store", "device", deviceID, "error", err)
			continue
		}
		allPoints = append(allPoints, points...)
	}

	// Query file storage
	if a.columnarStorage != nil {
		filePoints, err := a.columnarStorage.Query(database, collection, deviceIDs, startTime, endTime, fields)
		if err != nil {
			a.logger.Warn("Failed to query file storage", "error", err)
		} else {
			allPoints = append(allPoints, filePoints...)
		}
	}

	// Deduplicate
	allPoints = deduplicatePoints(allPoints)

	return allPoints, nil
}

// GetChecksum calculates checksum for data in a time range
func (a *LocalStorageAdapter) GetChecksum(database, collection string, startTime, endTime time.Time) (string, int64, error) {
	points, err := a.Query(database, collection, nil, startTime, endTime, nil)
	if err != nil {
		return "", 0, err
	}

	if len(points) == 0 {
		return "empty", 0, nil
	}

	// Sort points for consistent checksum
	sort.Slice(points, func(i, j int) bool {
		if points[i].ID != points[j].ID {
			return points[i].ID < points[j].ID
		}
		return points[i].Time.Before(points[j].Time)
	})

	// Calculate checksum
	h := sha256.New()
	for _, p := range points {
		h.Write([]byte(p.ID))
		h.Write([]byte(p.Time.Format(time.RFC3339Nano)))
		// Include field names (sorted)
		var fieldNames []string
		for k := range p.Fields {
			fieldNames = append(fieldNames, k)
		}
		sort.Strings(fieldNames)
		for _, name := range fieldNames {
			h.Write([]byte(name))
			_, _ = fmt.Fprintf(h, "%v", p.Fields[name])
		}
	}

	checksum := hex.EncodeToString(h.Sum(nil))[:16] // Use first 16 chars
	return checksum, int64(len(points)), nil
}

// deduplicatePoints removes duplicate data points
func deduplicatePoints(points []*storage.DataPoint) []*storage.DataPoint {
	if len(points) == 0 {
		return points
	}

	seen := make(map[string]*storage.DataPoint)
	for _, p := range points {
		key := fmt.Sprintf("%s:%s:%s:%d", p.Database, p.Collection, p.ID, p.Time.UnixNano())
		existing, exists := seen[key]
		if !exists || p.InsertedAt.After(existing.InsertedAt) {
			seen[key] = p
		}
	}

	result := make([]*storage.DataPoint, 0, len(seen))
	for _, p := range seen {
		result = append(result, p)
	}

	// Sort by time
	sort.Slice(result, func(i, j int) bool {
		return result[i].Time.Before(result[j].Time)
	})

	return result
}
