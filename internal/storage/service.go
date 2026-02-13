package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/subscriber"
	"github.com/soltixdb/soltix/internal/wal"
)

// WriteMessage represents a write message from the queue
type WriteMessage struct {
	Database   string                 `json:"database"`
	Collection string                 `json:"collection"`
	ShardID    string                 `json:"shard_id"`
	GroupID    int                    `json:"group_id"`
	Nodes      []string               `json:"nodes"`
	Time       string                 `json:"time"`
	ID         string                 `json:"id"`
	Fields     map[string]interface{} `json:"fields"`
}

// StorageConfig configuration for storage settings
type StorageConfig struct {
	Timezone *time.Location // Timezone for date-based file organization
	// Columnar storage config
	MaxRowsPerPart     int   // Max rows per part file (default: 10000)
	MaxPartSize        int64 // Max size per part file in bytes (default: 64MB)
	MinRowsPerPart     int   // Min rows before splitting (default: 1000)
	MaxDevicesPerGroup int   // Max devices per device group (default: 50)
}

// StorageService handles storage operations for write messages
type StorageService struct {
	nodeID         string
	logger         *logging.Logger
	dataDir        string
	subscriber     subscriber.Subscriber
	walPartitioned wal.PartitionedWriter // Partitioned WAL by database+date
	memStore       *MemoryStore

	// Storage config
	storageConfig StorageConfig // Storage configuration

	// Storage backend - V5 Columnar only
	columnarStorage *Storage                       // V5 columnar columnar storage
	tieredStorage   *TieredStorage                 // 4-tier group-aware storage (optional)
	aggStorage      aggregation.AggregationStorage // Aggregation storage interface

	// Parallel write worker pool
	writeWorkerPool *WriteWorkerPool // Partitioned workers for parallel writes

	// Event-driven flush and aggregation (new architecture)
	flushPool   *FlushWorkerPool      // Event-driven flush workers
	aggPipeline *aggregation.Pipeline // Aggregation pipeline

	// Background compaction
	compactionWorker *CompactionWorker // Background part compaction

	// Context for subscription handlers
	ctx    context.Context
	cancel context.CancelFunc
}

// NewStorageService creates a new storage service with the given subscriber
func NewStorageService(
	sub subscriber.Subscriber,
	nodeID, dataDir string,
	logger *logging.Logger,
	maxAge time.Duration,
	maxSize int,
	storageConfig StorageConfig,
) (*StorageService, error) {
	if sub == nil {
		return nil, fmt.Errorf("subscriber is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize partitioned WAL for parallel writes (database + collection + date partitioning)
	walDir := filepath.Join(dataDir, "wal")
	walPartitioned, err := wal.NewPartitionedWriter(wal.PartitionedConfig{
		BaseDir:     walDir,
		Config:      wal.DefaultConfig(walDir),
		IdleTimeout: 10 * time.Minute,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize partitioned WAL: %w", err)
	}
	logger.Info("Partitioned WAL initialized", "dir", walDir)

	// Initialize in-memory store
	memStore := NewMemoryStore(maxAge, maxSize, logger)
	logger.Info("Memory store initialized",
		"max_age", maxAge,
		"max_size", maxSize)

	// Initialize V3 columnar storage with config
	storageDir := filepath.Join(dataDir, "data")
	columnarStorage := NewStorageWithConfig(
		storageDir,
		compression.Snappy,
		logger,
		storageConfig.MaxRowsPerPart,
		storageConfig.MaxPartSize,
		storageConfig.MinRowsPerPart,
		storageConfig.MaxDevicesPerGroup,
	)

	// Set timezone for storage if provided
	if storageConfig.Timezone != nil {
		columnarStorage.SetTimezone(storageConfig.Timezone)
		logger.Info("Columnar storage timezone configured", "timezone", storageConfig.Timezone.String())
	}

	logger.Info("Columnar storage V5 initialized", "dir", storageDir)

	// Initialize 4-tier group-aware storage
	tieredConfig := GroupStorageConfig{
		DataDir:            storageDir,
		Compression:        compression.Snappy,
		MaxRowsPerPart:     storageConfig.MaxRowsPerPart,
		MaxPartSize:        storageConfig.MaxPartSize,
		MinRowsPerPart:     storageConfig.MinRowsPerPart,
		MaxDevicesPerGroup: storageConfig.MaxDevicesPerGroup,
		Timezone:           storageConfig.Timezone,
	}
	tieredStorage := NewTieredStorage(tieredConfig, logger)

	// Initialize aggregate storage for aggregation pipeline
	aggStorageDir := filepath.Join(dataDir, "agg")
	aggStorage := aggregation.NewStorage(aggStorageDir, logger)
	if storageConfig.Timezone != nil {
		aggStorage.SetTimezone(storageConfig.Timezone)
	}
	logger.Info("Columnar aggregate storage initialized", "dir", aggStorageDir)

	// Use TieredStorageAdapter as main storage backend for writes
	mainStorage := NewTieredStorageAdapter(tieredStorage)

	service := &StorageService{
		nodeID:          nodeID,
		logger:          logger,
		dataDir:         dataDir,
		subscriber:      sub,
		walPartitioned:  walPartitioned,
		memStore:        memStore,
		storageConfig:   storageConfig,
		columnarStorage: columnarStorage,
		tieredStorage:   tieredStorage,
		aggStorage:      aggStorage,
		ctx:             ctx,
		cancel:          cancel,
	}

	// Create raw data reader adapter for aggregation pipeline
	// Use TieredStorageAdapter (mainStorage) so aggregation reads from group-aware paths
	rawDataReader := NewRawDataReaderAdapter(mainStorage)

	// Initialize aggregation pipeline (pool-based)
	aggPipelineConfig := aggregation.DefaultPipelineConfig()
	if storageConfig.Timezone != nil {
		aggPipelineConfig.Timezone = storageConfig.Timezone
	}
	aggPipeline := aggregation.NewPipeline(
		aggPipelineConfig,
		logger,
		rawDataReader,
		aggStorage,
	)
	service.aggPipeline = aggPipeline
	logger.Info("Aggregation pipeline initialized")

	// Initialize flush worker pool (event-driven)
	flushPool := NewFlushWorkerPool(
		FlushWorkerPoolConfig{
			MaxBatchSize:      100,
			FlushDelay:        time.Second,
			MaxPendingEntries: 10000,
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         1000,
		},
		logger,
		walPartitioned,
		memStore,
		mainStorage, // Use TieredStorageAdapter for group-aware writes
		aggPipeline,
	)
	service.flushPool = flushPool
	logger.Info("Flush worker pool initialized")

	// Initialize write worker pool for parallel writes with partitioned WAL
	writeWorkerPool := NewWriteWorkerPool(
		logger,
		walPartitioned,
		memStore,
		flushPool, // Pass flush pool for event-driven notifications
	)
	service.writeWorkerPool = writeWorkerPool
	logger.Info("Write worker pool initialized with partitioned WAL")

	// Initialize background compaction worker
	compactionWorker := NewCompactionWorker(DefaultCompactionWorkerConfig(), logger)
	service.compactionWorker = compactionWorker
	logger.Info("Compaction worker initialized")

	return service, nil
}

// Start begins the storage service
func (s *StorageService) Start() error {
	// Step 1: Replay WAL to recover unflushed data from previous run
	if err := s.replayWAL(); err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Step 2: Subscribe to node-specific subject for writes
	writeSubject := fmt.Sprintf("soltix.write.node.%s", s.nodeID)
	s.logger.Info("Subscribing to write subject", "subject", writeSubject)

	if err := s.subscriber.Subscribe(s.ctx, writeSubject, s.handleWriteMessage); err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", writeSubject, err)
	}
	s.logger.Info("Successfully subscribed to write subject", "subject", writeSubject)

	// Step 3: Subscribe to admin flush trigger
	flushSubject := "soltix.admin.flush.trigger"
	if err := s.subscriber.Subscribe(s.ctx, flushSubject, s.handleFlushTrigger); err != nil {
		return fmt.Errorf("failed to subscribe to flush trigger: %w", err)
	}
	s.logger.Info("Subscribed to flush trigger", "subject", flushSubject)

	// Step 4: Start write worker pool for parallel writes
	if s.writeWorkerPool != nil {
		s.writeWorkerPool.Start()
		s.logger.Info("Write worker pool started")
	}

	// Step 5: Start aggregation pipeline (event-driven)
	if s.aggPipeline != nil {
		s.aggPipeline.Start()
		s.logger.Info("Aggregation pipeline started")
	}

	// Step 6: Start flush worker pool (event-driven)
	if s.flushPool != nil {
		s.flushPool.Start()
		s.logger.Info("Flush worker pool started")
	}

	// Step 7: Start background compaction worker
	if s.compactionWorker != nil && s.tieredStorage != nil {
		// Set tiered storage for dynamic engine discovery
		s.compactionWorker.SetTieredStorage(s.tieredStorage)
		s.compactionWorker.Start()
		s.logger.Info("Background compaction worker started")
	}

	return nil
}

// handleWriteMessage processes incoming write messages
func (s *StorageService) handleWriteMessage(ctx context.Context, subject string, data []byte) error {
	// Check context first
	if ctx.Err() != nil {
		s.logger.Error("handleWriteMessage: context cancelled",
			"error", ctx.Err(),
			"data_len", len(data))
		return ctx.Err()
	}

	// Parse message
	var writeMsg WriteMessage
	if err := json.Unmarshal(data, &writeMsg); err != nil {
		s.logger.Error("Failed to parse write message",
			"error", err,
			"data_preview", string(data[:min(200, len(data))]))
		return err
	}

	// Write to storage
	if err := s.writeToStorage(writeMsg); err != nil {
		s.logger.Error("Failed to write to storage",
			"error", err,
			"id", writeMsg.ID,
			"time", writeMsg.Time,
			"database", writeMsg.Database,
			"collection", writeMsg.Collection)
		return err
	}

	return nil
}

// handleFlushTrigger processes admin flush triggers
func (s *StorageService) handleFlushTrigger(ctx context.Context, subject string, data []byte) error {
	s.logger.Info("Received flush trigger", "subject", subject)

	// Trigger flush all partitions via flush pool
	go func() {
		if s.flushPool != nil {
			s.flushPool.FlushAll()
			s.logger.Info("Manual flush completed")
		}
	}()

	return nil
}

// writeToStorage writes data to storage engine
// Uses WriteWorkerPool for parallel processing partitioned by database + date
func (s *StorageService) writeToStorage(msg WriteMessage) error {
	// Use worker pool for parallel writes if available
	if s.writeWorkerPool != nil {
		return s.writeWorkerPool.Submit(msg)
	}

	// Fallback to direct write (legacy path)
	return s.writeToStorageDirect(msg)
}

// writeToStorageDirect writes data directly without worker pool (legacy/fallback)
// Only data within memStore maxAge is stored in memory
// Notification is sent only when WAL writes to a new segment file
func (s *StorageService) writeToStorageDirect(msg WriteMessage) error {
	// Parse timestamp first
	timeParsed, err := time.Parse(time.RFC3339, msg.Time)
	if err != nil {
		return fmt.Errorf("failed to parse time: %w", err)
	}

	// Check if data is recent enough for memory store
	dataAge := time.Since(timeParsed)
	isRecentData := dataAge <= s.memStore.GetMaxAge()

	// Create WAL entry
	entry := &wal.Entry{
		Type:       wal.EntryTypeWrite,
		Database:   msg.Database,
		Collection: msg.Collection,
		ShardID:    msg.ShardID,
		GroupID:    msg.GroupID,
		Time:       msg.Time,
		ID:         msg.ID,
		Fields:     msg.Fields,
		Timestamp:  timeParsed.UnixNano(),
	}

	// Write to WAL (always, for durability)
	// WritePartitioned returns whether this write triggered a new segment
	result, err := s.walPartitioned.WritePartitioned(entry, msg.Database, timeParsed)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	if isRecentData {
		// Recent data: write to memory store for fast queries
		dataPoint := &DataPoint{
			Database:   msg.Database,
			Collection: msg.Collection,
			ShardID:    msg.ShardID,
			GroupID:    msg.GroupID,
			Time:       timeParsed,
			ID:         msg.ID,
			Fields:     msg.Fields,
			InsertedAt: time.Now(),
		}

		if err := s.memStore.Write(dataPoint); err != nil {
			return fmt.Errorf("failed to write to memory store: %w", err)
		}
	}

	// Only notify flush pool when WAL writes to a new segment file
	// This drastically reduces notifications - only 1 per segment instead of 1 per record
	if result.IsNewSegment && s.flushPool != nil {
		s.flushPool.Notify(WriteNotification{
			Database:   msg.Database,
			Date:       timeParsed,
			Collection: msg.Collection,
			EntryCount: 1,
			Immediate:  !isRecentData, // Immediate flush for historical data
		})
		s.logger.Debug("New WAL segment created, notified flush pool",
			"database", msg.Database,
			"collection", msg.Collection,
			"immediate", !isRecentData)
	}

	return nil
}

// GetWAL returns the partitioned WAL instance for use by other components
func (s *StorageService) GetWAL() wal.PartitionedWriter {
	return s.walPartitioned
}

// GetMemoryStore returns the memory store instance for use by other components (e.g., flusher)
func (s *StorageService) GetMemoryStore() *MemoryStore {
	return s.memStore
}

// GetWriteWorkerPool returns the write worker pool for monitoring
func (s *StorageService) GetWriteWorkerPool() *WriteWorkerPool {
	return s.writeWorkerPool
}

// GetStorage returns the V3 columnar storage instance
func (s *StorageService) GetStorage() *Storage {
	return s.columnarStorage
}

// GetTieredStorage returns the 4-tier group-aware storage engine
func (s *StorageService) GetTieredStorage() *TieredStorage {
	return s.tieredStorage
}

// Stop gracefully stops the storage service
func (s *StorageService) Stop() error {
	// Cancel context to stop handlers
	s.cancel()

	// Stop write worker pool first (drains pending writes)
	if s.writeWorkerPool != nil {
		s.writeWorkerPool.Stop()
		s.logger.Info("Write worker pool stopped")
	}

	// Stop flush worker pool (event-driven)
	if s.flushPool != nil {
		s.flushPool.Stop()
		s.logger.Info("Flush worker pool stopped")
	}

	// Stop aggregation pipeline
	if s.aggPipeline != nil {
		s.aggPipeline.Stop()
		s.logger.Info("Aggregation pipeline stopped")
	}

	// Stop background compaction worker
	if s.compactionWorker != nil {
		s.compactionWorker.Stop()
		s.logger.Info("Compaction worker stopped")
	}

	// Unsubscribe from subjects
	writeSubject := fmt.Sprintf("soltix.write.node.%s", s.nodeID)
	if s.subscriber != nil {
		if err := s.subscriber.Unsubscribe(writeSubject); err != nil {
			s.logger.Error("Failed to unsubscribe write subject", "error", err)
		}
		if err := s.subscriber.Unsubscribe("soltix.admin.flush.trigger"); err != nil {
			s.logger.Error("Failed to unsubscribe flush trigger", "error", err)
		}
	}

	if s.memStore != nil {
		if err := s.memStore.Close(); err != nil {
			s.logger.Error("Failed to close memory store", "error", err)
		} else {
			s.logger.Info("Memory store closed")
		}
	}

	// Close partitioned WAL
	if s.walPartitioned != nil {
		if err := s.walPartitioned.Close(); err != nil {
			s.logger.Error("Failed to close partitioned WAL", "error", err)
		} else {
			s.logger.Info("Partitioned WAL closed")
		}
	}

	// Close subscriber
	if s.subscriber != nil {
		if err := s.subscriber.Close(); err != nil {
			s.logger.Error("Failed to close subscriber", "error", err)
		} else {
			s.logger.Info("Subscriber closed")
		}
	}

	return nil
}

// replayWAL replays WAL entries on startup
// Strategy: Process segment-by-segment to avoid loading all WAL data into memory.
// - Recent data (within maxAge): replay to memory store for fast queries
// - All data: flush directly to storage (since flush pool isn't started yet)
// - Processed segments are removed after successful flush
func (s *StorageService) replayWAL() error {
	s.logger.Info("Starting WAL replay")

	// List all partitions
	partitions, err := s.walPartitioned.ListPartitions()
	if err != nil {
		return fmt.Errorf("failed to list WAL partitions: %w", err)
	}

	if len(partitions) == 0 {
		s.logger.Info("No WAL partitions to replay")
		return nil
	}

	s.logger.Info("Found WAL partitions to replay", "count", len(partitions))

	maxAge := s.memStore.GetMaxAge()
	totalReplayed := 0
	totalFlushed := 0
	totalSegments := 0

	// Process each partition
	for _, partition := range partitions {
		// PrepareFlushPartition rotates to a new segment and returns all old segment files.
		// This ensures any new writes during replay go to a separate segment.
		segmentFiles, err := s.walPartitioned.PrepareFlushPartition(partition.Database, partition.Collection, partition.Date)
		if err != nil {
			s.logger.Warn("Failed to prepare flush for partition, skipping",
				"database", partition.Database,
				"collection", partition.Collection,
				"date", partition.Date.Format("2006-01-02"),
				"error", err)
			continue
		}

		if len(segmentFiles) == 0 {
			continue
		}

		s.logger.Info("Replaying WAL partition",
			"database", partition.Database,
			"collection", partition.Collection,
			"date", partition.Date.Format("2006-01-02"),
			"segments", len(segmentFiles))

		partitionReplayed := 0
		partitionFlushed := 0

		// Process each segment file individually to keep memory usage bounded
		for segIdx, segFile := range segmentFiles {
			s.logger.Info("Replaying WAL segment",
				"segment", segFile,
				"progress", fmt.Sprintf("%d/%d", segIdx+1, len(segmentFiles)),
				"database", partition.Database,
				"collection", partition.Collection,
				"date", partition.Date.Format("2006-01-02"))

			entries, err := s.walPartitioned.ReadPartitionSegmentFile(
				partition.Database, partition.Collection, partition.Date, segFile,
			)
			if err != nil {
				s.logger.Warn("Failed to read WAL segment, skipping",
					"segment", segFile,
					"database", partition.Database,
					"error", err)
				continue
			}

			if len(entries) == 0 {
				// Remove empty segment
				_ = s.walPartitioned.RemovePartitionSegmentFiles(
					partition.Database, partition.Collection, partition.Date, []string{segFile},
				)
				continue
			}

			// Replay recent entries to memory store
			for _, entry := range entries {
				timestamp, parseErr := time.Parse(time.RFC3339Nano, entry.Time)
				if parseErr != nil {
					// Fallback: try parsing from Timestamp field
					if entry.Timestamp > 0 {
						timestamp = time.Unix(0, entry.Timestamp)
					} else {
						continue
					}
				}

				if time.Since(timestamp) <= maxAge {
					dp := &DataPoint{
						Database:    entry.Database,
						Collection:  entry.Collection,
						ShardID:     entry.ShardID,
						GroupID:     entry.GroupID,
						Time:        timestamp,
						ID:          entry.ID,
						Fields:      entry.Fields,
						InsertedAt:  time.Now(),
						FlushStatus: FlushStatusNew,
					}
					if writeErr := s.memStore.Write(dp); writeErr != nil {
						s.logger.Error("Failed to replay entry to memory store",
							"error", writeErr)
					} else {
						partitionReplayed++
					}
				}
			}

			// Flush entries directly to storage (flush pool not started yet)
			dataPoints := make([]*DataPoint, 0, len(entries))
			for _, entry := range entries {
				dp := walEntryToDataPoint(entry)
				if dp != nil {
					dataPoints = append(dataPoints, dp)
				}
			}

			if len(dataPoints) > 0 {
				if flushErr := s.tieredStorage.WriteBatch(dataPoints); flushErr != nil {
					s.logger.Error("Failed to flush WAL segment to storage",
						"segment", segFile,
						"points", len(dataPoints),
						"error", flushErr)
					// Don't remove segment on failure — will be retried next startup
					continue
				}
				partitionFlushed += len(dataPoints)
			}

			// Segment successfully processed — remove it
			if rmErr := s.walPartitioned.RemovePartitionSegmentFiles(
				partition.Database, partition.Collection, partition.Date, []string{segFile},
			); rmErr != nil {
				s.logger.Warn("Failed to remove processed WAL segment",
					"segment", segFile, "error", rmErr)
			}
		}

		totalReplayed += partitionReplayed
		totalFlushed += partitionFlushed
		totalSegments += len(segmentFiles)

		s.logger.Info("Partition replay completed",
			"database", partition.Database,
			"collection", partition.Collection,
			"date", partition.Date.Format("2006-01-02"),
			"segments", len(segmentFiles),
			"replayed_to_memory", partitionReplayed,
			"flushed_to_storage", partitionFlushed)
	}

	s.logger.Info("WAL replay completed",
		"total_partitions", len(partitions),
		"total_segments", totalSegments,
		"replayed_to_memory", totalReplayed,
		"flushed_to_storage", totalFlushed)

	return nil
}
