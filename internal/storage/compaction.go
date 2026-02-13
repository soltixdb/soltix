package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// ============================================================================
// Background Compaction
// ============================================================================
//
// With append-only writes, each WriteBatch adds new part files to existing DGs
// without reading or rewriting existing data. Over time, this creates many small
// part files per CG directory.
//
// The compaction worker periodically scans for DGs that have too many parts
// (above compactionThreshold) and merges them: read all parts → deduplicate →
// sort by time → rewrite as minimal parts.
//
// This is essentially the old read-merge-sort-rewrite logic, but executed
// asynchronously in the background when resources are available.
// ============================================================================

// CompactionWorkerConfig configures the background compaction worker
type CompactionWorkerConfig struct {
	Interval  time.Duration // How often to scan for compaction candidates
	Threshold int           // Max parts per CG before triggering compaction
}

// DefaultCompactionWorkerConfig returns default compaction worker settings
func DefaultCompactionWorkerConfig() CompactionWorkerConfig {
	return CompactionWorkerConfig{
		Interval:  time.Duration(DefaultCompactionInterval) * time.Second,
		Threshold: DefaultCompactionThreshold,
	}
}

// CompactionWorker runs background compaction on storage engines
type CompactionWorker struct {
	mu            sync.Mutex
	config        CompactionWorkerConfig
	logger        *logging.Logger
	storages      []*Storage     // Statically registered storage engines
	tieredStorage *TieredStorage // For dynamically discovering group engines
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	running       bool
}

// NewCompactionWorker creates a new background compaction worker
func NewCompactionWorker(config CompactionWorkerConfig, logger *logging.Logger) *CompactionWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &CompactionWorker{
		config: config,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
}

// AddStorage registers a storage engine for background compaction
func (cw *CompactionWorker) AddStorage(s *Storage) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.storages = append(cw.storages, s)
}

// SetTieredStorage sets the tiered storage for dynamic engine discovery
func (cw *CompactionWorker) SetTieredStorage(ts *TieredStorage) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.tieredStorage = ts
}

// Start begins the background compaction loop
func (cw *CompactionWorker) Start() {
	cw.mu.Lock()
	if cw.running {
		cw.mu.Unlock()
		return
	}
	cw.running = true
	cw.mu.Unlock()

	cw.wg.Add(1)
	go cw.loop()

	cw.logger.Info("Compaction worker started",
		"interval", cw.config.Interval,
		"threshold", cw.config.Threshold,
		"storage_count", len(cw.storages))
}

// Stop gracefully stops the compaction worker
func (cw *CompactionWorker) Stop() {
	cw.cancel()
	cw.wg.Wait()

	cw.mu.Lock()
	cw.running = false
	cw.mu.Unlock()

	cw.logger.Info("Compaction worker stopped")
}

func (cw *CompactionWorker) loop() {
	defer cw.wg.Done()

	ticker := time.NewTicker(cw.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-cw.ctx.Done():
			return
		case <-ticker.C:
			cw.runCompactionCycle()
		}
	}
}

func (cw *CompactionWorker) runCompactionCycle() {
	cw.mu.Lock()
	staticStorages := make([]*Storage, len(cw.storages))
	copy(staticStorages, cw.storages)
	ts := cw.tieredStorage
	cw.mu.Unlock()

	// Collect all engines: static + dynamic from tiered storage
	engineSet := make(map[*Storage]bool)
	for _, s := range staticStorages {
		engineSet[s] = true
	}
	if ts != nil {
		for _, s := range ts.GetAllEngines() {
			engineSet[s] = true
		}
	}

	totalCompacted := 0
	for s := range engineSet {
		n, err := s.CompactAll(cw.ctx, cw.config.Threshold)
		if err != nil {
			cw.logger.Warn("Compaction cycle error", "error", err)
		}
		totalCompacted += n
	}

	if totalCompacted > 0 {
		cw.logger.Info("Compaction cycle completed", "compacted_dgs", totalCompacted)
	}
}

// ============================================================================
// Storage-level compaction methods
// ============================================================================

// CompactAll scans all date directories and compacts DGs that have too many parts.
// Returns the number of DGs compacted.
func (ms *Storage) CompactAll(ctx context.Context, threshold int) (int, error) {
	if threshold <= 0 {
		threshold = ms.compactionThreshold
	}
	if threshold <= 0 {
		threshold = DefaultCompactionThreshold
	}

	// Walk the data directory to find all date dirs
	dateDirs, err := ms.findAllDateDirs()
	if err != nil {
		return 0, err
	}

	totalCompacted := 0
	for _, dateDir := range dateDirs {
		select {
		case <-ctx.Done():
			return totalCompacted, ctx.Err()
		default:
		}

		n, err := ms.compactDateDir(ctx, dateDir, threshold)
		if err != nil {
			ms.logger.Warn("Failed to compact date dir", "dir", dateDir, "error", err)
			continue
		}
		totalCompacted += n
	}

	return totalCompacted, nil
}

// CompactDateDir compacts a specific date directory
func (ms *Storage) CompactDateDir(ctx context.Context, dateDir string, threshold int) (int, error) {
	return ms.compactDateDir(ctx, dateDir, threshold)
}

// compactDateDir compacts DGs in a date directory that exceed the part threshold.
// For each qualifying DG: reads ALL data from ALL parts → deduplicates → sorts → rewrites as minimal parts.
func (ms *Storage) compactDateDir(ctx context.Context, dateDir string, threshold int) (int, error) {
	// Acquire per-dateDir lock to prevent racing with concurrent writes
	dateDirMu := ms.getDateDirLock(dateDir)
	dateDirMu.Lock()
	defer dateDirMu.Unlock()

	metadataPath := filepath.Join(dateDir, "_metadata.idx")
	metadata, err := ms.readGlobalMetadata(metadataPath)
	if err != nil {
		return 0, nil // No metadata = nothing to compact
	}

	compacted := 0
	metadataChanged := false

	for dgIdx, dg := range metadata.DeviceGroups {
		select {
		case <-ctx.Done():
			return compacted, ctx.Err()
		default:
		}

		if int(dg.PartCount) <= threshold {
			continue // This DG doesn't need compaction
		}

		dgDir := filepath.Join(dateDir, dg.DirName)

		// Check if this DG was already compacted to its minimum possible parts.
		// If compaction cannot reduce the part count (data is too large to fit
		// within threshold parts at maxRowsPerPart), skip to avoid infinite loops.
		maxRowsPerPart := ms.maxRowsPerPart
		if maxRowsPerPart <= 0 {
			maxRowsPerPart = DefaultMaxRowsPerPart
		}
		totalDeviceRows := uint64(dg.RowCount)
		// Estimate minimum parts needed from row count: total rows / max rows per part (ceiling)
		minPartsFromRows := int((totalDeviceRows + uint64(maxRowsPerPart) - 1) / uint64(maxRowsPerPart))
		if minPartsFromRows < 1 {
			minPartsFromRows = 1
		}
		// Also estimate minimum parts from file size (if we know actual total size)
		minPartsFromSize := 0
		if ms.maxPartSize > 0 {
			// Sum actual file sizes for this DG
			var totalDGSize int64
			dgPartDir := filepath.Join(dateDir, dg.DirName)
			for i := 0; i < int(dg.PartCount); i++ {
				partPath := filepath.Join(dgPartDir, fmt.Sprintf("part_%04d.bin", i))
				if info, err := os.Stat(partPath); err == nil {
					totalDGSize += info.Size()
				}
			}
			if totalDGSize > 0 {
				minPartsFromSize = int((totalDGSize + ms.maxPartSize - 1) / ms.maxPartSize)
			}
		}
		minPossibleParts := minPartsFromRows
		if minPartsFromSize > minPossibleParts {
			minPossibleParts = minPartsFromSize
		}
		if int(dg.PartCount) <= minPossibleParts {
			// Already at minimum — compaction won't reduce parts further
			continue
		}

		ms.logger.Info("Compacting device group",
			"dg", dg.DirName,
			"parts", dg.PartCount,
			"threshold", threshold,
			"min_possible_parts", minPossibleParts,
			"rows", dg.RowCount,
			"devices", dg.DeviceCount)

		// Read DG metadata
		dgMetaPath := filepath.Join(dgDir, "_metadata.idx")
		dgMeta, err := ms.readDeviceGroupMetadata(dgMetaPath)
		if err != nil {
			ms.logger.Warn("Failed to read DG metadata for compaction", "dg", dg.DirName, "error", err)
			continue
		}

		// Read ALL data from this DG
		existingPoints := ms.readAllV6PartsInDG(dgDir, dgMeta)
		if len(existingPoints) == 0 {
			continue
		}

		// Deduplicate and sort
		existingPoints = ms.deduplicatePoints(existingPoints)

		// Group by device
		deviceGroups := ms.groupByDevice(existingPoints)

		// Collect field names and types from compacted data
		fieldNames, fieldTypes := collectFieldsFromData(deviceGroups)

		// Rewrite the entire DG with compacted data
		dgManifest, _, tmpInfos, err := ms.writeV6DeviceGroupDir(dateDir, dgIdx, deviceGroups, fieldNames, fieldTypes)
		if err != nil {
			ms.logger.Warn("Failed to compact DG", "dg", dg.DirName, "error", err)
			ms.cleanupTmpFiles(tmpInfos)
			continue
		}

		// Atomic rename
		renameErr := ms.atomicRenameAll(tmpInfos)
		if renameErr != nil {
			ms.logger.Warn("Failed to rename compacted files", "error", renameErr)
			ms.cleanupTmpFiles(tmpInfos)
			continue
		}

		// Clean up stale part files that are no longer referenced after compaction.
		// writeV6DeviceGroupDir creates new part files but does NOT
		// remove old ones from before compaction.
		ms.cleanupV6StaleParts(dgDir, dgManifest)

		// Update manifest
		metadata.DeviceGroups[dgIdx] = dgManifest
		metadataChanged = true
		compacted++

		ms.logger.Info("Device group compacted",
			"dg", dg.DirName,
			"old_parts", dg.PartCount,
			"new_parts", dgManifest.PartCount,
			"old_rows", dg.RowCount,
			"new_rows", dgManifest.RowCount)
	}

	// If metadata changed, rewrite global metadata
	if metadataChanged {
		// Rebuild global stats
		var totalRows uint64
		var minTs, maxTs int64
		deviceSet := make(map[string]bool)

		for _, dg := range metadata.DeviceGroups {
			totalRows += uint64(dg.RowCount)
			for _, d := range dg.DeviceNames {
				deviceSet[d] = true
			}
			if minTs == 0 || dg.MinTimestamp < minTs {
				minTs = dg.MinTimestamp
			}
			if dg.MaxTimestamp > maxTs {
				maxTs = dg.MaxTimestamp
			}
		}

		allDevices := make([]string, 0, len(deviceSet))
		for d := range deviceSet {
			allDevices = append(allDevices, d)
		}
		sort.Strings(allDevices)

		metadata.TotalRows = totalRows
		metadata.MinTimestamp = minTs
		metadata.MaxTimestamp = maxTs
		metadata.DeviceCount = uint32(len(allDevices))
		metadata.DeviceNames = allDevices

		// Write updated global metadata
		metadataTmpInfo, err := ms.writeGlobalMetadata(metadataPath, metadata)
		if err != nil {
			return compacted, fmt.Errorf("failed to write compacted metadata: %w", err)
		}

		if err := ms.atomicRenameAll([]*TmpFileInfo{metadataTmpInfo}); err != nil {
			return compacted, fmt.Errorf("failed to rename compacted metadata: %w", err)
		}

		// Clear metadata cache
		ms.metadataCacheLock.Lock()
		delete(ms.metadataCache, dateDir)
		ms.metadataCacheLock.Unlock()
	}

	return compacted, nil
}

// findAllDateDirs finds all date directories across all databases and collections
func (ms *Storage) findAllDateDirs() ([]string, error) {
	var dateDirs []string

	// Walk: dataDir/{db}/{collection}/{year}/{month}/{date}
	dbEntries, err := os.ReadDir(ms.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	for _, dbEntry := range dbEntries {
		if !dbEntry.IsDir() {
			continue
		}
		dbDir := filepath.Join(ms.dataDir, dbEntry.Name())

		colEntries, err := os.ReadDir(dbDir)
		if err != nil {
			continue
		}
		for _, colEntry := range colEntries {
			if !colEntry.IsDir() {
				continue
			}
			colDir := filepath.Join(dbDir, colEntry.Name())

			yearEntries, err := os.ReadDir(colDir)
			if err != nil {
				continue
			}
			for _, yearEntry := range yearEntries {
				if !yearEntry.IsDir() {
					continue
				}
				yearDir := filepath.Join(colDir, yearEntry.Name())

				monthEntries, err := os.ReadDir(yearDir)
				if err != nil {
					continue
				}
				for _, monthEntry := range monthEntries {
					if !monthEntry.IsDir() {
						continue
					}
					monthDir := filepath.Join(yearDir, monthEntry.Name())

					dateEntries, err := os.ReadDir(monthDir)
					if err != nil {
						continue
					}
					for _, dateEntry := range dateEntries {
						if !dateEntry.IsDir() {
							continue
						}
						dateDir := filepath.Join(monthDir, dateEntry.Name())
						// Verify it has _metadata.idx
						if _, err := os.Stat(filepath.Join(dateDir, "_metadata.idx")); err == nil {
							dateDirs = append(dateDirs, dateDir)
						}
					}
				}
			}
		}
	}

	return dateDirs, nil
}

// NeedsCompaction checks if any DG in the given date directory needs compaction
func (ms *Storage) NeedsCompaction(dateDir string, threshold int) bool {
	metadataPath := filepath.Join(dateDir, "_metadata.idx")
	metadata, err := ms.readGlobalMetadata(metadataPath)
	if err != nil {
		return false
	}

	for _, dg := range metadata.DeviceGroups {
		if int(dg.PartCount) > threshold {
			return true
		}
	}
	return false
}
