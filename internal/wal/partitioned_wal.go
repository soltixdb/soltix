package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PartitionedWriter is a WAL writer that partitions data by database, collection, and date
// Directory structure: baseDir/<database>/<collection>/<YYYY-MM-DD>/wal-xxx.log
type PartitionedWriter interface {
	// WritePartitioned writes an entry to the appropriate partition
	WritePartitioned(entry *Entry, database string, date time.Time) (WriteResult, error)

	// WriteSyncPartitioned writes an entry synchronously to the partition
	WriteSyncPartitioned(entry *Entry, database string, date time.Time) error

	// FlushPartition flushes a specific partition
	FlushPartition(database, collection string, date time.Time) error

	// SetCheckpointPartition sets a checkpoint on a specific partition and rotates to new segment
	// New writes after this will go to a new segment file
	SetCheckpointPartition(database, collection string, date time.Time) error

	// TruncatePartitionBeforeCheckpoint removes old segments from a partition that were before the checkpoint
	// This safely removes flushed data without affecting new writes
	TruncatePartitionBeforeCheckpoint(database, collection string, date time.Time) error

	// FlushAll flushes all partitions
	FlushAll() error

	// Close closes all partition writers
	Close() error

	// GetPartitionWriter gets or creates a writer for a partition
	GetPartitionWriter(database, collection string, date time.Time) (Writer, error)

	// ReadPartition reads all entries from a specific partition
	ReadPartition(database, collection string, date time.Time) ([]*Entry, error)

	// ReadAllPartitions reads all entries from all partitions
	ReadAllPartitions() ([]*Entry, error)

	// ListPartitions returns all database/collection/date partitions
	ListPartitions() ([]PartitionInfo, error)

	// RemovePartition removes a partition (for cleanup)
	RemovePartition(database, collection string, date time.Time) error

	// GetStats returns statistics for all partitions
	GetStats() PartitionedStats

	// SetCheckpointAll sets checkpoint on all partitions
	SetCheckpointAll() error

	// TruncateBeforeCheckpointAll truncates all partitions before checkpoint
	TruncateBeforeCheckpointAll() error

	// GetTotalSegmentCount returns total number of segments across all partitions
	GetTotalSegmentCount() (int, error)

	// RotateAll rotates all partition segments (for cold WAL)
	RotateAll() error

	// ProcessPartitions processes all partitions and returns entries, then removes them
	// This is used by ColdFlusher to read and cleanup in one operation
	ProcessPartitions(handler func(partition PartitionInfo, entries []*Entry) error) error

	// PrepareFlushPartition flushes buffer, rotates to new segment, returns old segment files to process
	// This ensures new writes go to a new segment while flush processes old segments
	PrepareFlushPartition(database, collection string, date time.Time) ([]string, error)

	// ReadPartitionSegmentFile reads all entries from a specific segment file in a partition
	ReadPartitionSegmentFile(database, collection string, date time.Time, filename string) ([]*Entry, error)

	// RemovePartitionSegmentFiles removes specified segment files from a partition
	RemovePartitionSegmentFiles(database, collection string, date time.Time, files []string) error

	// HasPartitionData checks if a partition has any WAL data to flush (non-empty segment files)
	HasPartitionData(database, collection string, date time.Time) bool
}

// PartitionInfo contains information about a partition
type PartitionInfo struct {
	Database   string
	Collection string
	Date       time.Time
	Dir        string
}

// WriteResult contains information about a write operation
type WriteResult struct {
	// IsNewSegment indicates if the write caused a new segment file to be created
	// This is useful to determine if a flush notification is needed
	IsNewSegment bool
}

// PartitionedStats contains statistics for partitioned WAL
type PartitionedStats struct {
	PartitionCount int
	TotalPending   int
	Partitions     map[string]Stats // key: "database:collection:YYYY-MM-DD"
}

// partitionedWriter implements PartitionedWriter
type partitionedWriter struct {
	baseDir     string
	config      Config
	writers     map[string]Writer // key: "database:collection:YYYY-MM-DD"
	notified    map[string]bool   // key: "database:collection:YYYY-MM-DD", true if current segment was notified
	mu          sync.RWMutex
	idleTimeout time.Duration
	lastAccess  map[string]time.Time
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// PartitionedConfig contains configuration for partitioned WAL
type PartitionedConfig struct {
	// BaseDir is the base directory for all partitions
	BaseDir string

	// Config is the configuration for each partition writer
	Config Config

	// IdleTimeout is how long to keep idle partition writers open
	IdleTimeout time.Duration
}

// DefaultPartitionedConfig returns default partitioned config
func DefaultPartitionedConfig(baseDir string) PartitionedConfig {
	return PartitionedConfig{
		BaseDir:     baseDir,
		Config:      DefaultConfig(baseDir), // Will be overridden per partition
		IdleTimeout: 10 * time.Minute,
	}
}

// NewPartitionedWriter creates a new partitioned WAL writer
func NewPartitionedWriter(config PartitionedConfig) (PartitionedWriter, error) {
	if err := os.MkdirAll(config.BaseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create base WAL directory: %w", err)
	}

	pw := &partitionedWriter{
		baseDir:     config.BaseDir,
		config:      config.Config,
		writers:     make(map[string]Writer),
		notified:    make(map[string]bool),
		lastAccess:  make(map[string]time.Time),
		idleTimeout: config.IdleTimeout,
		stopCh:      make(chan struct{}),
	}

	// Start cleanup goroutine
	pw.wg.Add(1)
	go pw.cleanupIdleWriters()

	return pw, nil
}

// partitionKey generates a key for the partition map
func partitionKey(database, collection string, date time.Time) string {
	return fmt.Sprintf("%s:%s:%s", database, collection, date.Format("2006-01-02"))
}

// partitionDir returns the directory path for a partition
func (pw *partitionedWriter) partitionDir(database, collection string, date time.Time) string {
	dateStr := date.Format("2006-01-02")
	return filepath.Join(pw.baseDir, database, collection, dateStr)
}

// GetPartitionWriter gets or creates a writer for a partition
func (pw *partitionedWriter) GetPartitionWriter(database, collection string, date time.Time) (Writer, error) {
	key := partitionKey(database, collection, date)

	// Fast path: read lock
	pw.mu.RLock()
	writer, exists := pw.writers[key]
	pw.mu.RUnlock()

	if exists {
		// Update last access with write lock (separate from read)
		pw.mu.Lock()
		pw.lastAccess[key] = time.Now()
		pw.mu.Unlock()
		return writer, nil
	}

	// Slow path: write lock
	pw.mu.Lock()
	defer pw.mu.Unlock()

	// Double check
	if writer, exists = pw.writers[key]; exists {
		pw.lastAccess[key] = time.Now()
		return writer, nil
	}

	// Create partition directory
	partDir := pw.partitionDir(database, collection, date)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	// Create writer for this partition
	partConfig := Config{
		Dir:            partDir,
		MaxSegmentSize: pw.config.MaxSegmentSize,
		MaxBatchSize:   pw.config.MaxBatchSize,
		FlushInterval:  pw.config.FlushInterval,
	}

	writer, err := NewWriterWithConfig(partConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition writer: %w", err)
	}

	pw.writers[key] = writer
	pw.lastAccess[key] = time.Now()

	return writer, nil
}

// WritePartitioned writes an entry to the appropriate partition
func (pw *partitionedWriter) WritePartitioned(entry *Entry, database string, date time.Time) (WriteResult, error) {
	key := partitionKey(database, entry.Collection, date)

	writer, err := pw.GetPartitionWriter(database, entry.Collection, date)
	if err != nil {
		return WriteResult{}, err
	}

	if err := writer.Write(entry); err != nil {
		return WriteResult{}, err
	}

	// Check if this partition has been notified already
	pw.mu.Lock()
	wasNotified := pw.notified[key]
	if !wasNotified {
		pw.notified[key] = true // Mark as notified
	}
	pw.mu.Unlock()

	return WriteResult{
		IsNewSegment: !wasNotified, // True if this is first write after partition creation or PrepareFlush
	}, nil
}

// WriteSyncPartitioned writes an entry synchronously
func (pw *partitionedWriter) WriteSyncPartitioned(entry *Entry, database string, date time.Time) error {
	writer, err := pw.GetPartitionWriter(database, entry.Collection, date)
	if err != nil {
		return err
	}
	return writer.WriteSync(entry)
}

// FlushPartition flushes a specific partition
func (pw *partitionedWriter) FlushPartition(database, collection string, date time.Time) error {
	key := partitionKey(database, collection, date)

	pw.mu.RLock()
	writer, exists := pw.writers[key]
	pw.mu.RUnlock()

	if !exists {
		return nil // Partition doesn't exist, nothing to flush
	}

	return writer.Flush()
}

// SetCheckpointPartition sets a checkpoint on a specific partition
// This marks the current position and rotates to a new segment for new writes
func (pw *partitionedWriter) SetCheckpointPartition(database, collection string, date time.Time) error {
	key := partitionKey(database, collection, date)

	pw.mu.RLock()
	writer, exists := pw.writers[key]
	pw.mu.RUnlock()

	if !exists {
		return nil // Partition doesn't exist
	}

	return writer.SetCheckpoint()
}

// TruncatePartitionBeforeCheckpoint removes segments before the checkpoint for a specific partition
// This safely removes flushed data without affecting new writes that came after the checkpoint
func (pw *partitionedWriter) TruncatePartitionBeforeCheckpoint(database, collection string, date time.Time) error {
	key := partitionKey(database, collection, date)

	pw.mu.RLock()
	writer, exists := pw.writers[key]
	pw.mu.RUnlock()

	if !exists {
		return nil // Partition doesn't exist
	}

	return writer.TruncateBeforeCheckpoint()
}

// FlushAll flushes all partitions
func (pw *partitionedWriter) FlushAll() error {
	pw.mu.RLock()
	writers := make([]Writer, 0, len(pw.writers))
	for _, w := range pw.writers {
		writers = append(writers, w)
	}
	pw.mu.RUnlock()

	var lastErr error
	for _, writer := range writers {
		if err := writer.Flush(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Close closes all partition writers
func (pw *partitionedWriter) Close() error {
	// Stop cleanup goroutine
	close(pw.stopCh)
	pw.wg.Wait()

	pw.mu.Lock()
	defer pw.mu.Unlock()

	var lastErr error
	for key, writer := range pw.writers {
		if err := writer.Close(); err != nil {
			lastErr = err
		}
		delete(pw.writers, key)
	}

	return lastErr
}

// ReadPartition reads all entries from a specific partition
func (pw *partitionedWriter) ReadPartition(database, collection string, date time.Time) ([]*Entry, error) {
	writer, err := pw.GetPartitionWriter(database, collection, date)
	if err != nil {
		// Check if directory exists but writer not opened yet
		partDir := pw.partitionDir(database, collection, date)
		if _, statErr := os.Stat(partDir); os.IsNotExist(statErr) {
			return nil, nil // Partition doesn't exist
		}
		return nil, err
	}
	return writer.ReadAll()
}

// ListPartitions returns all database/collection/date partitions
func (pw *partitionedWriter) ListPartitions() ([]PartitionInfo, error) {
	var partitions []PartitionInfo

	// Walk the base directory
	databases, err := os.ReadDir(pw.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return partitions, nil
		}
		return nil, fmt.Errorf("failed to read base directory: %w", err)
	}

	for _, dbEntry := range databases {
		if !dbEntry.IsDir() {
			continue
		}

		database := dbEntry.Name()
		dbDir := filepath.Join(pw.baseDir, database)

		// Read collections
		collections, err := os.ReadDir(dbDir)
		if err != nil {
			continue
		}

		for _, collEntry := range collections {
			if !collEntry.IsDir() {
				continue
			}

			collection := collEntry.Name()
			collDir := filepath.Join(dbDir, collection)

			// Read dates
			dates, err := os.ReadDir(collDir)
			if err != nil {
				continue
			}

			for _, dateEntry := range dates {
				if !dateEntry.IsDir() {
					continue
				}

				dateStr := dateEntry.Name()
				date, err := time.Parse("2006-01-02", dateStr)
				if err != nil {
					continue // Skip invalid date directories
				}

				partitions = append(partitions, PartitionInfo{
					Database:   database,
					Collection: collection,
					Date:       date,
					Dir:        filepath.Join(collDir, dateStr),
				})
			}
		}
	}

	return partitions, nil
}

// RemovePartition removes a partition
func (pw *partitionedWriter) RemovePartition(database, collection string, date time.Time) error {
	key := partitionKey(database, collection, date)

	pw.mu.Lock()
	if writer, exists := pw.writers[key]; exists {
		_ = writer.Close()
		delete(pw.writers, key)
		delete(pw.lastAccess, key)
	}
	pw.mu.Unlock()

	// Remove the directory
	partDir := pw.partitionDir(database, collection, date)
	return os.RemoveAll(partDir)
}

// GetStats returns statistics for all partitions
func (pw *partitionedWriter) GetStats() PartitionedStats {
	pw.mu.RLock()
	defer pw.mu.RUnlock()

	stats := PartitionedStats{
		PartitionCount: len(pw.writers),
		Partitions:     make(map[string]Stats),
	}

	for key, writer := range pw.writers {
		writerStats := writer.GetStats()
		stats.Partitions[key] = writerStats
		stats.TotalPending += writerStats.PendingEntries
	}

	return stats
}

// cleanupIdleWriters periodically closes idle partition writers
func (pw *partitionedWriter) cleanupIdleWriters() {
	defer pw.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-pw.stopCh:
			return
		case <-ticker.C:
			pw.doCleanup()
		}
	}
}

// doCleanup closes writers that have been idle too long and removes empty directories
func (pw *partitionedWriter) doCleanup() {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	now := time.Now()
	for key, lastAccess := range pw.lastAccess {
		if now.Sub(lastAccess) > pw.idleTimeout {
			if writer, exists := pw.writers[key]; exists {
				_ = writer.Close() // Close will cleanup empty segment files
				delete(pw.writers, key)
				delete(pw.lastAccess, key)
				delete(pw.notified, key) // Also cleanup notified state
			}
		}
	}

	// Cleanup empty date/collection/database directories
	pw.cleanupEmptyDirectories()
}

// cleanupEmptyDirectories removes empty directories in the WAL structure
func (pw *partitionedWriter) cleanupEmptyDirectories() {
	// Walk baseDir to find empty directories
	databases, err := os.ReadDir(pw.baseDir)
	if err != nil {
		return
	}

	for _, db := range databases {
		if !db.IsDir() {
			continue
		}
		dbPath := filepath.Join(pw.baseDir, db.Name())

		collections, err := os.ReadDir(dbPath)
		if err != nil {
			continue
		}

		for _, coll := range collections {
			if !coll.IsDir() {
				continue
			}
			collPath := filepath.Join(dbPath, coll.Name())

			dates, err := os.ReadDir(collPath)
			if err != nil {
				continue
			}

			for _, date := range dates {
				if !date.IsDir() {
					continue
				}
				datePath := filepath.Join(collPath, date.Name())

				// Check if date directory is empty
				files, err := os.ReadDir(datePath)
				if err == nil && len(files) == 0 {
					_ = os.Remove(datePath)
				}
			}

			// Check if collection directory is empty
			remaining, err := os.ReadDir(collPath)
			if err == nil && len(remaining) == 0 {
				_ = os.Remove(collPath)
			}
		}

		// Check if database directory is empty
		remaining, err := os.ReadDir(dbPath)
		if err == nil && len(remaining) == 0 {
			_ = os.Remove(dbPath)
		}
	}
}

// ReadAllPartitions reads all entries from all partitions
func (pw *partitionedWriter) ReadAllPartitions() ([]*Entry, error) {
	partitions, err := pw.ListPartitions()
	if err != nil {
		return nil, err
	}

	var allEntries []*Entry
	for _, partition := range partitions {
		entries, err := pw.ReadPartition(partition.Database, partition.Collection, partition.Date)
		if err != nil {
			return nil, fmt.Errorf("failed to read partition %s/%s/%s: %w",
				partition.Database, partition.Collection, partition.Date.Format("2006-01-02"), err)
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

// SetCheckpointAll sets checkpoint on all partitions
func (pw *partitionedWriter) SetCheckpointAll() error {
	pw.mu.RLock()
	writers := make([]Writer, 0, len(pw.writers))
	for _, w := range pw.writers {
		writers = append(writers, w)
	}
	pw.mu.RUnlock()

	var lastErr error
	for _, writer := range writers {
		if err := writer.SetCheckpoint(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// TruncateBeforeCheckpointAll truncates all partitions before checkpoint
func (pw *partitionedWriter) TruncateBeforeCheckpointAll() error {
	pw.mu.RLock()
	writers := make([]Writer, 0, len(pw.writers))
	for _, w := range pw.writers {
		writers = append(writers, w)
	}
	pw.mu.RUnlock()

	var lastErr error
	for _, writer := range writers {
		if err := writer.TruncateBeforeCheckpoint(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// GetTotalSegmentCount returns total number of segments across all partitions
func (pw *partitionedWriter) GetTotalSegmentCount() (int, error) {
	pw.mu.RLock()
	writers := make([]Writer, 0, len(pw.writers))
	for _, w := range pw.writers {
		writers = append(writers, w)
	}
	pw.mu.RUnlock()

	total := 0
	for _, writer := range writers {
		count, err := writer.GetSegmentCount()
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

// RotateAll rotates all partition segments
func (pw *partitionedWriter) RotateAll() error {
	pw.mu.RLock()
	writers := make([]Writer, 0, len(pw.writers))
	for _, w := range pw.writers {
		writers = append(writers, w)
	}
	pw.mu.RUnlock()

	var lastErr error
	for _, writer := range writers {
		if err := writer.SetCheckpoint(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ProcessPartitions processes all partitions and returns entries, then removes them
func (pw *partitionedWriter) ProcessPartitions(handler func(partition PartitionInfo, entries []*Entry) error) error {
	partitions, err := pw.ListPartitions()
	if err != nil {
		return fmt.Errorf("failed to list partitions: %w", err)
	}

	for _, partition := range partitions {
		// Flush before reading to ensure all data is on disk
		if err := pw.FlushPartition(partition.Database, partition.Collection, partition.Date); err != nil {
			continue // Skip this partition
		}

		entries, err := pw.ReadPartition(partition.Database, partition.Collection, partition.Date)
		if err != nil {
			continue // Skip this partition
		}

		if len(entries) == 0 {
			// Empty partition, remove it
			_ = pw.RemovePartition(partition.Database, partition.Collection, partition.Date)
			continue
		}

		// Call handler
		if err := handler(partition, entries); err != nil {
			continue // Skip but don't remove on error
		}

		// Remove processed partition
		_ = pw.RemovePartition(partition.Database, partition.Collection, partition.Date)
	}

	return nil
}

// PrepareFlushPartition flushes buffer, rotates to new segment, returns old segment files to process
func (pw *partitionedWriter) PrepareFlushPartition(database, collection string, date time.Time) ([]string, error) {
	key := partitionKey(database, collection, date)

	writer, err := pw.GetPartitionWriter(database, collection, date)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition writer: %w", err)
	}

	files, err := writer.PrepareFlush()
	if err != nil {
		return nil, err
	}

	// Reset notified flag - next write to this partition will trigger new notification
	pw.mu.Lock()
	pw.notified[key] = false
	pw.mu.Unlock()

	return files, nil
}

// ReadPartitionSegmentFile reads all entries from a specific segment file in a partition
func (pw *partitionedWriter) ReadPartitionSegmentFile(database, collection string, date time.Time, filename string) ([]*Entry, error) {
	writer, err := pw.GetPartitionWriter(database, collection, date)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition writer: %w", err)
	}

	return writer.ReadSegmentFile(filename)
}

// RemovePartitionSegmentFiles removes specified segment files from a partition
func (pw *partitionedWriter) RemovePartitionSegmentFiles(database, collection string, date time.Time, files []string) error {
	writer, err := pw.GetPartitionWriter(database, collection, date)
	if err != nil {
		return fmt.Errorf("failed to get partition writer: %w", err)
	}

	return writer.RemoveSegmentFiles(files)
}

// HasPartitionData checks if a partition has any WAL data to flush (non-empty segment files)
func (pw *partitionedWriter) HasPartitionData(database, collection string, date time.Time) bool {
	writer, err := pw.GetPartitionWriter(database, collection, date)
	if err != nil {
		return false
	}

	return writer.HasData()
}
