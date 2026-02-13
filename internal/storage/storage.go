package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// V6 Columnar Storage with Device Groups + Single-File Parts with Footer Index
// 2-level hierarchy:
//   - Device Groups (dg_XXXX/): group devices by count (max maxDevicesPerGroup)
//   - Parts (part_XXXX.bin): single file with ALL columns + footer-based column index
//
// Directory structure: dateDir/dg_XXXX/part_XXXX.bin
// Two-tier metadata:
//   - Global _metadata.idx in dateDir: field list, DG manifests, device→DG map
//   - DG _metadata.idx in dg_XXXX/: part file names, part manifests, device→part map
const (
	// Metadata index magic: "SIDX" (Soltix InDeX)
	MetadataIndexMagic = 0x58444953

	// Device group metadata magic: "SDGX" (Soltix Device Group indeX)
	DeviceGroupMetadataMagic = 0x58474453

	// Version
	StorageVersion = 6

	// Default split thresholds (can be overridden by config)
	DefaultMaxRowsPerPart     = 100_000          // Primary: split when rows >= 100K
	DefaultMaxPartSize        = 64 * 1024 * 1024 // Safety: max 64MB per part
	DefaultMinRowsPerPart     = 1000             // Don't split if less than 1K rows
	DefaultMaxDevicesPerGroup = 50               // Max devices per device group

	// Default row group size for columnar encoding
	DefaultRowGroupSize = 10_000

	// Default compaction settings
	DefaultCompactionThreshold = 4  // Trigger compaction when parts per DG > this
	DefaultCompactionInterval  = 30 // Seconds between compaction scans

	// File sizes
	MetadataHeaderSize = 256
)

// MetadataIndex is the global metadata file in dateDir (_metadata.idx)
// V6: No column group definitions — fields are stored in single part files with footer index
type MetadataIndex struct {
	Magic       uint32 // "SIDX"
	Version     uint32 // 6
	Compression compression.Algorithm

	// Global stats
	TotalRows    uint64
	GroupCount   uint32 // Number of device groups
	DeviceCount  uint32
	FieldCount   uint32
	MinTimestamp int64
	MaxTimestamp int64

	// All field names and types
	FieldNames []string
	FieldTypes []compression.ColumnType

	// Device list
	DeviceNames []string

	// Device group manifests (lightweight - no part details)
	DeviceGroups []DeviceGroupManifest

	// Device to device-group mapping for quick routing
	DeviceGroupMap map[string]int // device → DG index
}

// DeviceGroupManifest describes a device group directory (lightweight, stored in global metadata)
type DeviceGroupManifest struct {
	DirName      string // e.g. "dg_0000"
	GroupIndex   uint32
	DeviceCount  uint32
	PartCount    uint32 // Number of part files in this DG
	RowCount     uint32
	MinTimestamp int64
	MaxTimestamp int64
	DeviceNames  []string // Devices in this DG
}

// DeviceGroupMetadata is the per-DG metadata file in dg_XXXX/_metadata.idx
// V6: Contains part file names, part manifests, and device→part mapping within this DG
type DeviceGroupMetadata struct {
	Magic         uint32 // "SDGX"
	Version       uint32 // 6
	GroupIndex    uint32
	DeviceCount   uint32
	PartCount     uint32 // Number of part files in this DG
	RowCount      uint32
	DeviceNames   []string
	PartFileNames []string         // e.g. ["part_0000.bin", "part_0001.bin"]
	Parts         []PartManifest   // Per-part manifests
	DevicePartMap map[string][]int // device → part indices within this DG
}

// PartManifest describes a single part file within a column group directory
type PartManifest struct {
	FileName     string // e.g. "part_0000.bin"
	PartIndex    uint32
	RowCount     uint32
	DeviceCount  uint32
	MinTimestamp int64
	MaxTimestamp int64
	DeviceNames  []string // Devices in this part
	RowCounts    []uint32 // Row count per device in this part
	IsSorted     bool     // Whether data within this part is sorted by time (append parts may not be)
}

// Storage implements V6 columnar storage with Device Groups + Single-File Parts
type Storage struct {
	mu           sync.RWMutex
	dataDir      string
	compressor   compression.Compressor
	logger       *logging.Logger
	timezone     *time.Location
	rowGroupSize int

	// Configurable split thresholds
	maxRowsPerPart     int
	maxPartSize        int64
	minRowsPerPart     int
	maxDevicesPerGroup int

	// Compaction settings
	compactionThreshold int // Max parts per DG before triggering compaction

	// Cache for metadata index (per date directory)
	metadataCache     map[string]*MetadataIndex
	metadataCacheLock sync.RWMutex

	// Per-dateDir write locks to serialize writes to the same date directory
	dateDirLocks     map[string]*sync.Mutex
	dateDirLocksLock sync.Mutex
}

// NewStorage creates a new columnar storage with default settings
func NewStorage(dataDir string, algo compression.Algorithm, logger *logging.Logger) *Storage {
	return NewStorageWithConfig(dataDir, algo, logger, DefaultMaxRowsPerPart, DefaultMaxPartSize, DefaultMinRowsPerPart, DefaultMaxDevicesPerGroup)
}

// NewStorageWithConfig creates a new columnar storage with custom config
func NewStorageWithConfig(dataDir string, algo compression.Algorithm, logger *logging.Logger, maxRowsPerPart int, maxPartSize int64, minRowsPerPart int, maxDevicesPerGroup int) *Storage {
	_ = os.MkdirAll(dataDir, 0o755)

	compressor, _ := compression.GetCompressor(algo)
	if compressor == nil {
		compressor = compression.NewSnappyCompressor()
	}

	// Apply defaults if zero values
	if maxRowsPerPart <= 0 {
		maxRowsPerPart = DefaultMaxRowsPerPart
	}
	if maxPartSize <= 0 {
		maxPartSize = DefaultMaxPartSize
	}
	if minRowsPerPart <= 0 {
		minRowsPerPart = DefaultMinRowsPerPart
	}
	if maxDevicesPerGroup <= 0 {
		maxDevicesPerGroup = DefaultMaxDevicesPerGroup
	}

	logger.Info("Columnar storage initialized (V6 DG + Single-File Parts)",
		"max_rows_per_part", maxRowsPerPart,
		"max_part_size_mb", maxPartSize/(1024*1024),
		"min_rows_per_part", minRowsPerPart,
		"max_devices_per_group", maxDevicesPerGroup)

	return &Storage{
		dataDir:             dataDir,
		compressor:          compressor,
		logger:              logger,
		timezone:            time.UTC,
		rowGroupSize:        DefaultRowGroupSize,
		maxRowsPerPart:      maxRowsPerPart,
		maxPartSize:         maxPartSize,
		minRowsPerPart:      minRowsPerPart,
		maxDevicesPerGroup:  maxDevicesPerGroup,
		compactionThreshold: DefaultCompactionThreshold,
		metadataCache:       make(map[string]*MetadataIndex),
		dateDirLocks:        make(map[string]*sync.Mutex),
	}
}

// SetTimezone sets the timezone for date-based file organization
func (ms *Storage) SetTimezone(tz *time.Location) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if tz != nil {
		ms.timezone = tz
	}
}

// SetRowGroupSize sets the number of rows per row group
func (ms *Storage) SetRowGroupSize(size int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if size > 0 {
		ms.rowGroupSize = size
	}
}

// SetMaxDevicesPerGroup sets the max number of devices per device group
func (ms *Storage) SetMaxDevicesPerGroup(size int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if size > 0 {
		ms.maxDevicesPerGroup = size
	}
}

// storageFileKey represents a unique directory identifier
type storageFileKey struct {
	database   string
	collection string
	date       string // YYYYMMDD
}

// getDateDir returns the directory path for a date
func (ms *Storage) getDateDir(key storageFileKey) string {
	year := key.date[:4]
	month := key.date[4:6]
	return filepath.Join(ms.dataDir, key.database, key.collection, year, month, key.date)
}

// ============================================================
// Write operations
// ============================================================

// WriteBatch writes data points with automatic part splitting and column groups
func (ms *Storage) WriteBatch(entries []*DataPoint) error {
	if len(entries) == 0 {
		return nil
	}

	ms.logger.Info("Writing batch to columnar storage (V5)", "count", len(entries))

	// Group by database/collection/date
	grouped := ms.groupByFile(entries)

	// Write each group
	for key, points := range grouped {
		if err := ms.writeData(key, points); err != nil {
			return fmt.Errorf("failed to write columnar: %w", err)
		}
	}

	ms.logger.Info("Multi-part batch write completed", "dirs", len(grouped))
	return nil
}

// groupByFile groups data points by file key
func (ms *Storage) groupByFile(entries []*DataPoint) map[storageFileKey][]*DataPoint {
	result := make(map[storageFileKey][]*DataPoint)

	for _, entry := range entries {
		date := entry.Time.In(ms.timezone).Format("20060102")

		key := storageFileKey{
			database:   entry.Database,
			collection: entry.Collection,
			date:       date,
		}

		result[key] = append(result[key], entry)
	}

	return result
}

// getDateDirLock returns a per-dateDir mutex, creating one if needed.
// This serializes all writes (and compaction) to the same date directory.
func (ms *Storage) getDateDirLock(dateDir string) *sync.Mutex {
	ms.dateDirLocksLock.Lock()
	defer ms.dateDirLocksLock.Unlock()
	if mu, ok := ms.dateDirLocks[dateDir]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	ms.dateDirLocks[dateDir] = mu
	return mu
}

// writeData writes data to columnar storage using append-only strategy.
// Instead of reading existing data, merging, and rewriting, it appends new
// data as additional part files in existing DGs. Background compaction will
// later merge and sort these parts when resources are available.
func (ms *Storage) writeData(key storageFileKey, points []*DataPoint) error {
	dateDir := ms.getDateDir(key)
	metadataPath := filepath.Join(dateDir, "_metadata.idx")

	// Serialize writes to the same date directory to prevent race conditions
	// on metadata reads/writes and part index allocation
	dateDirMu := ms.getDateDirLock(dateDir)
	dateDirMu.Lock()
	defer dateDirMu.Unlock()

	// Create directory
	if err := os.MkdirAll(dateDir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Read existing metadata
	metadata, err := ms.readGlobalMetadata(metadataPath)
	if err != nil {
		// First write — no existing data, use full write path
		return ms.writeDataFull(key, points, dateDir, metadataPath)
	}

	// Group incoming points by device
	incomingByDevice := ms.groupByDevice(points)

	// Collect new field names from incoming data
	newFieldNames := ms.collectFieldNames(points)
	newFieldTypes := ms.inferFieldTypes(points, newFieldNames)

	// Merge field definitions: existing + new
	fieldNames, fieldTypes := ms.mergeFieldDefinitionsV6(metadata, newFieldNames, newFieldTypes)

	// Identify affected device groups
	maxDevices := ms.maxDevicesPerGroup
	if maxDevices <= 0 {
		maxDevices = DefaultMaxDevicesPerGroup
	}

	deviceToDGIdx := make(map[string]int) // device → assigned DG index
	var newDevices []string               // devices not yet in any DG
	affectedDGs := make(map[int]bool)

	for deviceID := range incomingByDevice {
		if dgIdx, ok := metadata.DeviceGroupMap[deviceID]; ok {
			deviceToDGIdx[deviceID] = dgIdx
			affectedDGs[dgIdx] = true
		} else {
			newDevices = append(newDevices, deviceID)
		}
	}
	sort.Strings(newDevices)

	// Assign new devices to existing DGs with available capacity, or create new DGs.
	// Track effective device count per DG (existing count + newly assigned in this loop)
	// to avoid overfilling a single DG when many new devices arrive in one batch.
	dgEffectiveCount := make(map[int]int)
	for dgIdx, dg := range metadata.DeviceGroups {
		dgEffectiveCount[dgIdx] = int(dg.DeviceCount)
	}

	nextDGIdx := len(metadata.DeviceGroups)
	for _, deviceID := range newDevices {
		assigned := false
		for dgIdx := range metadata.DeviceGroups {
			if dgEffectiveCount[dgIdx] < maxDevices {
				deviceToDGIdx[deviceID] = dgIdx
				affectedDGs[dgIdx] = true
				dgEffectiveCount[dgIdx]++
				assigned = true
				break
			}
		}
		if !assigned {
			deviceToDGIdx[deviceID] = nextDGIdx
			affectedDGs[nextDGIdx] = true
			dgEffectiveCount[nextDGIdx]++
			if dgEffectiveCount[nextDGIdx] >= maxDevices {
				nextDGIdx++
			}
		}
	}

	ms.logger.Debug("Append-only write (V6)",
		"incoming_devices", len(incomingByDevice),
		"affected_dgs", len(affectedDGs),
		"total_dgs", len(metadata.DeviceGroups),
		"new_devices", len(newDevices))

	// Group incoming devices by their DG index
	dgDeviceMap := make(map[int]map[string][]*DataPoint)
	for deviceID := range incomingByDevice {
		dgIdx := deviceToDGIdx[deviceID]
		if dgDeviceMap[dgIdx] == nil {
			dgDeviceMap[dgIdx] = make(map[string][]*DataPoint)
		}
		dgDeviceMap[dgIdx][deviceID] = incomingByDevice[deviceID]
	}

	// Prepare the new DeviceGroups slice
	totalDGs := nextDGIdx
	if totalDGs < len(metadata.DeviceGroups) {
		totalDGs = len(metadata.DeviceGroups)
	}
	newDGManifests := make([]DeviceGroupManifest, totalDGs)
	copy(newDGManifests, metadata.DeviceGroups)

	var pendingRenames []*TmpFileInfo
	newDeviceGroupMap := make(map[string]int)
	for deviceID, dgIdx := range metadata.DeviceGroupMap {
		newDeviceGroupMap[deviceID] = dgIdx
	}

	for dgIdx := range affectedDGs {
		incomingDGData := dgDeviceMap[dgIdx]

		if dgIdx >= len(metadata.DeviceGroups) {
			// Brand new DG — use full write (no existing parts)
			// Sort incoming data by time per device
			for deviceID, pts := range incomingDGData {
				sort.Slice(pts, func(i, j int) bool {
					return pts[i].Time.Before(pts[j].Time)
				})
				incomingDGData[deviceID] = pts
			}

			dgManifest, _, tmpInfos, err := ms.writeV6DeviceGroupDir(dateDir, dgIdx, incomingDGData, fieldNames, fieldTypes)
			if err != nil {
				ms.cleanupTmpFiles(pendingRenames)
				return fmt.Errorf("failed to write new DG %d: %w", dgIdx, err)
			}
			pendingRenames = append(pendingRenames, tmpInfos...)
			for len(newDGManifests) <= dgIdx {
				newDGManifests = append(newDGManifests, DeviceGroupManifest{})
			}
			newDGManifests[dgIdx] = dgManifest
			for _, d := range dgManifest.DeviceNames {
				newDeviceGroupMap[d] = dgIdx
			}
			continue
		}

		// Existing DG — append new parts (no read-back of existing data!)
		dg := metadata.DeviceGroups[dgIdx]
		dgDir := filepath.Join(dateDir, dg.DirName)

		// Sort incoming data per device by time
		for deviceID, pts := range incomingDGData {
			sort.Slice(pts, func(i, j int) bool {
				return pts[i].Time.Before(pts[j].Time)
			})
			incomingDGData[deviceID] = pts
		}

		// Append new parts to existing DG directory
		updatedManifest, tmpInfos, err := ms.appendV6PartsToExistingDG(dgDir, dgIdx, dg, incomingDGData, fieldNames, fieldTypes)
		if err != nil {
			ms.cleanupTmpFiles(pendingRenames)
			return fmt.Errorf("failed to append to DG %d: %w", dgIdx, err)
		}
		pendingRenames = append(pendingRenames, tmpInfos...)
		newDGManifests[dgIdx] = updatedManifest
		for _, d := range updatedManifest.DeviceNames {
			newDeviceGroupMap[d] = dgIdx
		}
	}

	// Compact: remove empty DGs and re-index
	compactedDGs, reindexMap := ms.compactDeviceGroups(newDGManifests)

	// Re-map device→DG with new indices
	finalDeviceGroupMap := make(map[string]int)
	for deviceID, oldIdx := range newDeviceGroupMap {
		if newIdx, ok := reindexMap[oldIdx]; ok {
			finalDeviceGroupMap[deviceID] = newIdx
		}
	}

	// Rebuild global stats
	var totalRows uint64
	var minTs, maxTs int64
	deviceSet := make(map[string]bool)

	for _, dg := range compactedDGs {
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

	newMetadata := &MetadataIndex{
		Magic:          MetadataIndexMagic,
		Version:        StorageVersion,
		Compression:    ms.compressor.Algorithm(),
		FieldNames:     fieldNames,
		FieldTypes:     fieldTypes,
		DeviceGroupMap: finalDeviceGroupMap,
		TotalRows:      totalRows,
		GroupCount:     uint32(len(compactedDGs)),
		DeviceCount:    uint32(len(allDevices)),
		FieldCount:     uint32(len(fieldNames)),
		MinTimestamp:   minTs,
		MaxTimestamp:   maxTs,
		DeviceNames:    allDevices,
		DeviceGroups:   compactedDGs,
	}

	// Write global metadata to temp file
	metadataTmpInfo, err := ms.writeGlobalMetadata(metadataPath, newMetadata)
	if err != nil {
		ms.cleanupTmpFiles(pendingRenames)
		return fmt.Errorf("failed to write global metadata: %w", err)
	}
	pendingRenames = append(pendingRenames, metadataTmpInfo)

	// ATOMIC BATCH RENAME
	if err := ms.atomicRenameAll(pendingRenames); err != nil {
		ms.cleanupTmpFiles(pendingRenames)
		return err
	}

	// Clear metadata cache
	ms.metadataCacheLock.Lock()
	delete(ms.metadataCache, dateDir)
	ms.metadataCacheLock.Unlock()

	ms.logger.Debug("Append-only write completed (V6)",
		"dir", dateDir,
		"affected_dgs", len(affectedDGs),
		"total_dgs", len(compactedDGs),
		"total_rows", totalRows,
		"devices", len(allDevices))

	return nil
}

// writeDataFull is the original full-write path used when no metadata exists (first write).
func (ms *Storage) writeDataFull(key storageFileKey, points []*DataPoint, dateDir string, metadataPath string) error {
	// Sort by device ID and time
	sort.Slice(points, func(i, j int) bool {
		if points[i].ID != points[j].ID {
			return points[i].ID < points[j].ID
		}
		return points[i].Time.Before(points[j].Time)
	})

	// Group points by device
	allDeviceGroups := ms.groupByDevice(points)

	// Collect all unique field names
	fieldNames := ms.collectFieldNames(points)

	// Infer field types
	fieldTypes := ms.inferFieldTypes(points, fieldNames)

	// Split into device groups based on device count
	dgGroups := ms.splitIntoDeviceGroups(allDeviceGroups)

	ms.logger.Debug("Initial full write (V6)",
		"total_points", len(points),
		"device_groups", len(dgGroups),
		"devices", len(allDeviceGroups),
		"fields", len(fieldNames))

	// Build new global metadata
	newMetadata := &MetadataIndex{
		Magic:          MetadataIndexMagic,
		Version:        StorageVersion,
		Compression:    ms.compressor.Algorithm(),
		FieldNames:     fieldNames,
		FieldTypes:     fieldTypes,
		DeviceGroupMap: make(map[string]int),
	}

	var pendingRenames []*TmpFileInfo

	for dgIdx, dgData := range dgGroups {
		dgManifest, _, tmpInfos, err := ms.writeV6DeviceGroupDir(dateDir, dgIdx, dgData, fieldNames, fieldTypes)
		if err != nil {
			ms.cleanupTmpFiles(pendingRenames)
			return fmt.Errorf("failed to write DG %d: %w", dgIdx, err)
		}

		pendingRenames = append(pendingRenames, tmpInfos...)
		newMetadata.DeviceGroups = append(newMetadata.DeviceGroups, dgManifest)
		newMetadata.TotalRows += uint64(dgManifest.RowCount)

		for _, deviceID := range dgManifest.DeviceNames {
			newMetadata.DeviceGroupMap[deviceID] = dgIdx
		}

		if newMetadata.MinTimestamp == 0 || dgManifest.MinTimestamp < newMetadata.MinTimestamp {
			newMetadata.MinTimestamp = dgManifest.MinTimestamp
		}
		if dgManifest.MaxTimestamp > newMetadata.MaxTimestamp {
			newMetadata.MaxTimestamp = dgManifest.MaxTimestamp
		}
	}

	allDevices := make([]string, 0, len(newMetadata.DeviceGroupMap))
	for deviceID := range newMetadata.DeviceGroupMap {
		allDevices = append(allDevices, deviceID)
	}
	sort.Strings(allDevices)
	newMetadata.DeviceNames = allDevices

	newMetadata.GroupCount = uint32(len(dgGroups))
	newMetadata.DeviceCount = uint32(len(allDevices))
	newMetadata.FieldCount = uint32(len(fieldNames))

	metadataTmpInfo, err := ms.writeGlobalMetadata(metadataPath, newMetadata)
	if err != nil {
		ms.cleanupTmpFiles(pendingRenames)
		return fmt.Errorf("failed to write global metadata: %w", err)
	}
	pendingRenames = append(pendingRenames, metadataTmpInfo)

	// ATOMIC BATCH RENAME
	if err := ms.atomicRenameAll(pendingRenames); err != nil {
		ms.cleanupTmpFiles(pendingRenames)
		return err
	}

	// Clear metadata cache
	ms.metadataCacheLock.Lock()
	delete(ms.metadataCache, dateDir)
	ms.metadataCacheLock.Unlock()

	ms.logger.Debug("Initial full write completed (V6)",
		"dir", dateDir,
		"device_groups", len(dgGroups),
		"total_rows", newMetadata.TotalRows,
		"devices", newMetadata.DeviceCount)

	return nil
}

// mergeFieldDefinitionsV6 merges existing and new field definitions.
// V6: no column groups, just merge field names/types.
func (ms *Storage) mergeFieldDefinitionsV6(metadata *MetadataIndex, newFieldNames []string, newFieldTypes []compression.ColumnType) ([]string, []compression.ColumnType) {
	// Merge: existing fields + new fields, sorted
	fieldSet := make(map[string]compression.ColumnType)
	for i, f := range metadata.FieldNames {
		if i < len(metadata.FieldTypes) {
			fieldSet[f] = metadata.FieldTypes[i]
		}
	}
	for i, f := range newFieldNames {
		if _, exists := fieldSet[f]; !exists {
			if i < len(newFieldTypes) {
				fieldSet[f] = newFieldTypes[i]
			}
		}
	}

	fieldNames := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		fieldNames = append(fieldNames, f)
	}
	sort.Strings(fieldNames)

	fieldTypes := make([]compression.ColumnType, len(fieldNames))
	for i, f := range fieldNames {
		fieldTypes[i] = fieldSet[f]
	}

	return fieldNames, fieldTypes
}

// compactDeviceGroups removes empty device groups and re-indexes them
func (ms *Storage) compactDeviceGroups(dgs []DeviceGroupManifest) ([]DeviceGroupManifest, map[int]int) {
	var compacted []DeviceGroupManifest
	reindexMap := make(map[int]int) // old index → new index

	for oldIdx, dg := range dgs {
		if dg.RowCount > 0 || len(dg.DeviceNames) > 0 {
			newIdx := len(compacted)
			reindexMap[oldIdx] = newIdx
			dg.GroupIndex = uint32(newIdx)
			dg.DirName = fmt.Sprintf("dg_%04d", newIdx)
			compacted = append(compacted, dg)
		}
	}

	return compacted, reindexMap
}

// cleanupTmpFiles removes all temp files on error
func (ms *Storage) cleanupTmpFiles(tmpFiles []*TmpFileInfo) {
	for _, info := range tmpFiles {
		_ = os.Remove(info.TmpPath)
	}
}

// atomicRenameAll renames all tmp files to their final paths under ms.mu lock.
// If any rename fails, already-renamed files are NOT rolled back (best-effort),
// and the error is returned. This avoids the double-unlock bug from inline rename loops.
func (ms *Storage) atomicRenameAll(tmpFiles []*TmpFileInfo) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, info := range tmpFiles {
		if err := os.Rename(info.TmpPath, info.FinalPath); err != nil {
			return fmt.Errorf("failed to rename %s to %s: %w", info.TmpPath, info.FinalPath, err)
		}
	}
	return nil
}

// groupByDevice groups points by device ID
func (ms *Storage) groupByDevice(points []*DataPoint) map[string][]*DataPoint {
	groups := make(map[string][]*DataPoint)
	for _, p := range points {
		groups[p.ID] = append(groups[p.ID], p)
	}
	return groups
}

// splitIntoDeviceGroups splits devices into groups of maxDevicesPerGroup.
// Each device group contains at most maxDevicesPerGroup devices.
func (ms *Storage) splitIntoDeviceGroups(deviceGroups map[string][]*DataPoint) []map[string][]*DataPoint {
	deviceIDs := make([]string, 0, len(deviceGroups))
	for id := range deviceGroups {
		deviceIDs = append(deviceIDs, id)
	}
	sort.Strings(deviceIDs)

	maxDevices := ms.maxDevicesPerGroup
	if maxDevices <= 0 {
		maxDevices = DefaultMaxDevicesPerGroup
	}

	var groups []map[string][]*DataPoint

	for i := 0; i < len(deviceIDs); i += maxDevices {
		end := i + maxDevices
		if end > len(deviceIDs) {
			end = len(deviceIDs)
		}

		group := make(map[string][]*DataPoint)
		for _, deviceID := range deviceIDs[i:end] {
			group[deviceID] = deviceGroups[deviceID]
		}
		groups = append(groups, group)
	}

	if len(groups) == 0 {
		groups = append(groups, make(map[string][]*DataPoint))
	}

	return groups
}

// splitIntoParts splits devices within a device group into parts based on row count
// and estimated part file size. Each part contains at most effectiveMaxRows total rows,
// where effectiveMaxRows = min(maxRowsPerPart, estimated rows that fit in maxPartSize).
func (ms *Storage) splitIntoParts(deviceGroups map[string][]*DataPoint) []map[string][]*DataPoint {
	deviceIDs := make([]string, 0, len(deviceGroups))
	for id := range deviceGroups {
		deviceIDs = append(deviceIDs, id)
	}
	sort.Strings(deviceIDs)

	// Compute effective max rows per part considering both row limit and size limit.
	// Estimate average compressed bytes per row from the data shape:
	//   ~8 bytes timestamp + ~8 bytes per field (avg) × compression ratio ~0.5
	// Then: effective_rows = maxPartSize / estimated_bytes_per_row
	effectiveMaxRows := ms.maxRowsPerPart
	if ms.maxPartSize > 0 {
		// Sample field count from first non-empty device
		fieldCount := 0
		for _, deviceID := range deviceIDs {
			points := deviceGroups[deviceID]
			if len(points) > 0 {
				fieldCount = len(points[0].Fields)
				break
			}
		}
		if fieldCount == 0 {
			fieldCount = 1
		}
		// Estimate: 8 bytes/timestamp + 10 bytes/field (value + overhead), compressed ~40%
		// Add footer/header overhead (~1KB per device)
		estimatedBytesPerRow := int64(8+fieldCount*10) * 4 / 10 // ~40% compression ratio
		if estimatedBytesPerRow < 4 {
			estimatedBytesPerRow = 4
		}
		sizeBasedMaxRows := int(ms.maxPartSize / estimatedBytesPerRow)
		if sizeBasedMaxRows < 1000 {
			sizeBasedMaxRows = 1000 // floor to avoid tiny parts
		}
		if sizeBasedMaxRows < effectiveMaxRows {
			effectiveMaxRows = sizeBasedMaxRows
		}
	}

	var parts []map[string][]*DataPoint
	currentPart := make(map[string][]*DataPoint)
	currentRows := 0

	for _, deviceID := range deviceIDs {
		devicePoints := deviceGroups[deviceID]
		deviceRows := len(devicePoints)

		if currentRows > 0 && currentRows+deviceRows > effectiveMaxRows {
			if len(currentPart) > 0 {
				parts = append(parts, currentPart)
			}
			currentPart = make(map[string][]*DataPoint)
			currentRows = 0
		}

		if deviceRows > effectiveMaxRows {
			// Single device exceeds max rows — split into chunks
			for i := 0; i < len(devicePoints); i += effectiveMaxRows {
				end := i + effectiveMaxRows
				if end > len(devicePoints) {
					end = len(devicePoints)
				}
				chunkPart := map[string][]*DataPoint{
					deviceID: devicePoints[i:end],
				}
				parts = append(parts, chunkPart)
			}
		} else {
			currentPart[deviceID] = devicePoints
			currentRows += deviceRows
		}
	}

	if len(currentPart) > 0 {
		parts = append(parts, currentPart)
	}

	if len(parts) == 0 {
		parts = append(parts, make(map[string][]*DataPoint))
	}

	return parts
}

// TmpFileInfo holds info about a temp file pending rename
type TmpFileInfo struct {
	TmpPath   string
	FinalPath string
}

// ============================================================
// Global Metadata (dateDir _metadata.idx) encode/decode
// ============================================================

func (ms *Storage) writeGlobalMetadata(path string, idx *MetadataIndex) (*TmpFileInfo, error) {
	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	data, err := ms.encodeGlobalMetadata(idx)
	if err != nil {
		return nil, err
	}

	if _, err := file.Write(data); err != nil {
		return nil, err
	}

	if err := file.Sync(); err != nil {
		return nil, err
	}

	return &TmpFileInfo{TmpPath: tmpPath, FinalPath: path}, nil
}

func (ms *Storage) encodeGlobalMetadata(idx *MetadataIndex) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	intBuf := make([]byte, 8)

	// Header
	binary.LittleEndian.PutUint32(intBuf, idx.Magic)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, idx.Version)
	buf = append(buf, intBuf[:4]...)
	buf = append(buf, byte(idx.Compression))

	// Global stats
	binary.LittleEndian.PutUint64(intBuf, idx.TotalRows)
	buf = append(buf, intBuf[:8]...)
	binary.LittleEndian.PutUint32(intBuf, idx.GroupCount)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, idx.DeviceCount)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, idx.FieldCount)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint64(intBuf, uint64(idx.MinTimestamp))
	buf = append(buf, intBuf[:8]...)
	binary.LittleEndian.PutUint64(intBuf, uint64(idx.MaxTimestamp))
	buf = append(buf, intBuf[:8]...)

	// Field names
	binary.LittleEndian.PutUint32(intBuf, uint32(len(idx.FieldNames)))
	buf = append(buf, intBuf[:4]...)
	for _, name := range idx.FieldNames {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, nameBytes...)
	}

	// Field types
	for _, t := range idx.FieldTypes {
		buf = append(buf, byte(t))
	}

	// Device names
	binary.LittleEndian.PutUint32(intBuf, uint32(len(idx.DeviceNames)))
	buf = append(buf, intBuf[:4]...)
	for _, name := range idx.DeviceNames {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, nameBytes...)
	}

	// Device group manifests (V6 — no CGCount)
	binary.LittleEndian.PutUint32(intBuf, uint32(len(idx.DeviceGroups)))
	buf = append(buf, intBuf[:4]...)
	for _, dg := range idx.DeviceGroups {
		// DirName
		dnBytes := []byte(dg.DirName)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(dnBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, dnBytes...)

		// Stats
		binary.LittleEndian.PutUint32(intBuf, dg.GroupIndex)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, dg.DeviceCount)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, dg.PartCount)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, dg.RowCount)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint64(intBuf, uint64(dg.MinTimestamp))
		buf = append(buf, intBuf[:8]...)
		binary.LittleEndian.PutUint64(intBuf, uint64(dg.MaxTimestamp))
		buf = append(buf, intBuf[:8]...)

		// Device names in this DG
		binary.LittleEndian.PutUint32(intBuf, uint32(len(dg.DeviceNames)))
		buf = append(buf, intBuf[:4]...)
		for _, name := range dg.DeviceNames {
			nameBytes := []byte(name)
			binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
			buf = append(buf, intBuf[:2]...)
			buf = append(buf, nameBytes...)
		}
	}

	// Device-to-device-group mapping
	binary.LittleEndian.PutUint32(intBuf, uint32(len(idx.DeviceGroupMap)))
	buf = append(buf, intBuf[:4]...)
	for deviceID, dgIdx := range idx.DeviceGroupMap {
		deviceBytes := []byte(deviceID)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(deviceBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, deviceBytes...)

		binary.LittleEndian.PutUint32(intBuf, uint32(dgIdx))
		buf = append(buf, intBuf[:4]...)
	}

	return buf, nil
}

func (ms *Storage) readGlobalMetadata(path string) (*MetadataIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ms.decodeGlobalMetadata(data)
}

func (ms *Storage) decodeGlobalMetadata(data []byte) (*MetadataIndex, error) {
	if len(data) < 50 {
		return nil, fmt.Errorf("global metadata too short")
	}

	offset := 0
	idx := &MetadataIndex{}

	// Header
	idx.Magic = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if idx.Magic != MetadataIndexMagic {
		return nil, fmt.Errorf("invalid global metadata magic: 0x%X", idx.Magic)
	}

	idx.Version = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	idx.Compression = compression.Algorithm(data[offset])
	offset++

	// Global stats
	idx.TotalRows = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	idx.GroupCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	idx.DeviceCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	idx.FieldCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	idx.MinTimestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	idx.MaxTimestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Field names
	fieldCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	idx.FieldNames = make([]string, fieldCount)
	for i := 0; i < fieldCount; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		idx.FieldNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Field types
	idx.FieldTypes = make([]compression.ColumnType, fieldCount)
	for i := 0; i < fieldCount; i++ {
		idx.FieldTypes[i] = compression.ColumnType(data[offset])
		offset++
	}

	// Device names
	deviceCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	idx.DeviceNames = make([]string, deviceCount)
	for i := 0; i < deviceCount; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		idx.DeviceNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Device group manifests (V6 — no CGCount)
	dgCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	idx.DeviceGroups = make([]DeviceGroupManifest, dgCount)
	for i := 0; i < dgCount; i++ {
		// DirName
		dnLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		idx.DeviceGroups[i].DirName = string(data[offset : offset+dnLen])
		offset += dnLen

		// Stats
		idx.DeviceGroups[i].GroupIndex = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		idx.DeviceGroups[i].DeviceCount = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		idx.DeviceGroups[i].PartCount = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		idx.DeviceGroups[i].RowCount = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		idx.DeviceGroups[i].MinTimestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		idx.DeviceGroups[i].MaxTimestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// Device names in this DG
		dgDeviceCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		idx.DeviceGroups[i].DeviceNames = make([]string, dgDeviceCount)
		for j := 0; j < dgDeviceCount; j++ {
			nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2
			idx.DeviceGroups[i].DeviceNames[j] = string(data[offset : offset+nameLen])
			offset += nameLen
		}
	}

	// Device-to-device-group mapping
	idx.DeviceGroupMap = make(map[string]int)
	mapCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	for i := 0; i < mapCount; i++ {
		deviceLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		deviceID := string(data[offset : offset+deviceLen])
		offset += deviceLen

		dgIdx := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		idx.DeviceGroupMap[deviceID] = dgIdx
	}

	return idx, nil
}

// ============================================================
// Device Group Metadata (dg_XXXX/_metadata.idx)
// ============================================================

func (ms *Storage) writeDeviceGroupMetadata(path string, dgMeta *DeviceGroupMetadata) (*TmpFileInfo, error) {
	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	data, err := ms.encodeDeviceGroupMetadata(dgMeta)
	if err != nil {
		return nil, err
	}

	if _, err := file.Write(data); err != nil {
		return nil, err
	}

	if err := file.Sync(); err != nil {
		return nil, err
	}

	return &TmpFileInfo{TmpPath: tmpPath, FinalPath: path}, nil
}

func (ms *Storage) encodeDeviceGroupMetadata(dgMeta *DeviceGroupMetadata) ([]byte, error) {
	buf := make([]byte, 0, 2048)
	intBuf := make([]byte, 8)

	// Header
	binary.LittleEndian.PutUint32(intBuf, dgMeta.Magic)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, dgMeta.Version)
	buf = append(buf, intBuf[:4]...)

	// Stats
	binary.LittleEndian.PutUint32(intBuf, dgMeta.GroupIndex)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, dgMeta.DeviceCount)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, dgMeta.PartCount)
	buf = append(buf, intBuf[:4]...)
	binary.LittleEndian.PutUint32(intBuf, dgMeta.RowCount)
	buf = append(buf, intBuf[:4]...)

	// Device names
	binary.LittleEndian.PutUint32(intBuf, uint32(len(dgMeta.DeviceNames)))
	buf = append(buf, intBuf[:4]...)
	for _, name := range dgMeta.DeviceNames {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, nameBytes...)
	}

	// Part file names (V6)
	binary.LittleEndian.PutUint32(intBuf, uint32(len(dgMeta.PartFileNames)))
	buf = append(buf, intBuf[:4]...)
	for _, name := range dgMeta.PartFileNames {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, nameBytes...)
	}

	// Part manifests (V6)
	binary.LittleEndian.PutUint32(intBuf, uint32(len(dgMeta.Parts)))
	buf = append(buf, intBuf[:4]...)
	for _, part := range dgMeta.Parts {
		// FileName
		fnBytes := []byte(part.FileName)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(fnBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, fnBytes...)

		// Stats
		binary.LittleEndian.PutUint32(intBuf, part.PartIndex)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, part.RowCount)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, part.DeviceCount)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint64(intBuf, uint64(part.MinTimestamp))
		buf = append(buf, intBuf[:8]...)
		binary.LittleEndian.PutUint64(intBuf, uint64(part.MaxTimestamp))
		buf = append(buf, intBuf[:8]...)

		// Device names in this part
		binary.LittleEndian.PutUint32(intBuf, uint32(len(part.DeviceNames)))
		buf = append(buf, intBuf[:4]...)
		for _, name := range part.DeviceNames {
			nameBytes := []byte(name)
			binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
			buf = append(buf, intBuf[:2]...)
			buf = append(buf, nameBytes...)
		}

		// Row counts per device
		for _, rc := range part.RowCounts {
			binary.LittleEndian.PutUint32(intBuf, rc)
			buf = append(buf, intBuf[:4]...)
		}

		// IsSorted flag
		if part.IsSorted {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}

	// Device-to-part mapping within this DG
	binary.LittleEndian.PutUint32(intBuf, uint32(len(dgMeta.DevicePartMap)))
	buf = append(buf, intBuf[:4]...)
	for deviceID, partIndices := range dgMeta.DevicePartMap {
		deviceBytes := []byte(deviceID)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(deviceBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, deviceBytes...)

		binary.LittleEndian.PutUint32(intBuf, uint32(len(partIndices)))
		buf = append(buf, intBuf[:4]...)
		for _, pi := range partIndices {
			binary.LittleEndian.PutUint32(intBuf, uint32(pi))
			buf = append(buf, intBuf[:4]...)
		}
	}

	return buf, nil
}

func (ms *Storage) readDeviceGroupMetadata(path string) (*DeviceGroupMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ms.decodeDeviceGroupMetadata(data)
}

func (ms *Storage) decodeDeviceGroupMetadata(data []byte) (*DeviceGroupMetadata, error) {
	if len(data) < 28 {
		return nil, fmt.Errorf("DG metadata too short")
	}

	offset := 0
	dgMeta := &DeviceGroupMetadata{}

	// Header
	dgMeta.Magic = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if dgMeta.Magic != DeviceGroupMetadataMagic {
		return nil, fmt.Errorf("invalid DG metadata magic: 0x%X", dgMeta.Magic)
	}

	dgMeta.Version = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Stats
	dgMeta.GroupIndex = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	dgMeta.DeviceCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	dgMeta.PartCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	dgMeta.RowCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Device names
	deviceCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	dgMeta.DeviceNames = make([]string, deviceCount)
	for i := 0; i < deviceCount; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		dgMeta.DeviceNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Part file names (V6)
	partFileCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	dgMeta.PartFileNames = make([]string, partFileCount)
	for i := 0; i < partFileCount; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		dgMeta.PartFileNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Part manifests (V6)
	manifestCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	dgMeta.Parts = make([]PartManifest, manifestCount)
	for i := 0; i < manifestCount; i++ {
		// FileName
		fnLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		dgMeta.Parts[i].FileName = string(data[offset : offset+fnLen])
		offset += fnLen

		// Stats
		dgMeta.Parts[i].PartIndex = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		dgMeta.Parts[i].RowCount = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		dgMeta.Parts[i].DeviceCount = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		dgMeta.Parts[i].MinTimestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		dgMeta.Parts[i].MaxTimestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// Device names
		partDeviceCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		dgMeta.Parts[i].DeviceNames = make([]string, partDeviceCount)
		for j := 0; j < partDeviceCount; j++ {
			nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2
			dgMeta.Parts[i].DeviceNames[j] = string(data[offset : offset+nameLen])
			offset += nameLen
		}

		// Row counts per device
		dgMeta.Parts[i].RowCounts = make([]uint32, partDeviceCount)
		for j := 0; j < partDeviceCount; j++ {
			dgMeta.Parts[i].RowCounts[j] = binary.LittleEndian.Uint32(data[offset:])
			offset += 4
		}

		// IsSorted flag
		if offset < len(data) {
			dgMeta.Parts[i].IsSorted = data[offset] == 1
			offset++
		}
	}

	// Device-to-part mapping
	dgMeta.DevicePartMap = make(map[string][]int)
	mapCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	for i := 0; i < mapCount; i++ {
		deviceLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		deviceID := string(data[offset : offset+deviceLen])
		offset += deviceLen

		partIdxCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		partIndices := make([]int, partIdxCount)
		for j := 0; j < partIdxCount; j++ {
			partIndices[j] = int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
		}
		dgMeta.DevicePartMap[deviceID] = partIndices
	}

	return dgMeta, nil
}

// ============================================================
// Read operations
// ============================================================

// ============================================================
// Query operations
// ============================================================

// Query queries data with optional field projection, reading only needed CGs
func (ms *Storage) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	dateDirs, err := ms.findDateDirs(database, collection, startTime, endTime)
	if err != nil {
		return nil, err
	}

	ms.logger.Debug("Multi-part query (V5)",
		"database", database,
		"collection", collection,
		"start_time", startTime.Format("2006-01-02 15:04:05"),
		"end_time", endTime.Format("2006-01-02 15:04:05"),
		"dirs_found", len(dateDirs),
		"devices", len(deviceIDs),
		"fields", len(fields))

	var allPoints []*DataPoint

	for _, dateDir := range dateDirs {
		points, err := ms.queryDateDir(dateDir, deviceIDs, startTime, endTime, fields)
		if err != nil {
			ms.logger.Warn("Failed to query date dir", "dir", dateDir, "error", err)
			continue
		}
		allPoints = append(allPoints, points...)
	}

	return allPoints, nil
}

// queryDateDir queries a single date directory using 3-level DG→CG→Part hierarchy
func (ms *Storage) queryDateDir(dateDir string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	metadataPath := filepath.Join(dateDir, "_metadata.idx")

	// Try cache
	ms.metadataCacheLock.RLock()
	metadata, cached := ms.metadataCache[dateDir]
	ms.metadataCacheLock.RUnlock()

	if !cached {
		var err error
		metadata, err = ms.readGlobalMetadata(metadataPath)
		if err != nil {
			return nil, nil // no data for this date
		}

		ms.metadataCacheLock.Lock()
		ms.metadataCache[dateDir] = metadata
		ms.metadataCacheLock.Unlock()
	}

	startNano := startTime.UnixNano()
	endNano := endTime.UnixNano()

	// Quick time range check
	if metadata.MaxTimestamp < startNano || metadata.MinTimestamp > endNano {
		return nil, nil
	}

	// Determine which DGs to query based on device filter
	dgsToQuery := make(map[int]bool)
	if len(deviceIDs) > 0 {
		for _, deviceID := range deviceIDs {
			if dgIdx, ok := metadata.DeviceGroupMap[deviceID]; ok {
				dgsToQuery[dgIdx] = true
			}
		}
	} else {
		for i := range metadata.DeviceGroups {
			dgsToQuery[i] = true
		}
	}

	if len(dgsToQuery) == 0 {
		return nil, nil
	}

	// Build filters
	deviceFilter := make(map[string]bool)
	for _, d := range deviceIDs {
		deviceFilter[d] = true
	}
	fieldFilter := make(map[string]bool)
	for _, f := range fields {
		fieldFilter[f] = true
	}

	// For each DG, query V6 part files directly
	type dgQuery struct {
		dgIdx int
		dgDir string
	}
	var allDGQueries []dgQuery

	for dgIdx := range dgsToQuery {
		dg := metadata.DeviceGroups[dgIdx]
		if dg.MaxTimestamp < startNano || dg.MinTimestamp > endNano {
			continue
		}
		dgDir := filepath.Join(dateDir, dg.DirName)
		allDGQueries = append(allDGQueries, dgQuery{dgIdx: dgIdx, dgDir: dgDir})
	}

	if len(allDGQueries) == 0 {
		return nil, nil
	}

	var allPoints []*DataPoint

	for _, dgq := range allDGQueries {
		dgMeta, err := ms.readDeviceGroupMetadata(filepath.Join(dgq.dgDir, "_metadata.idx"))
		if err != nil {
			ms.logger.Warn("Failed to read DG metadata for query", "dg", dgq.dgDir, "error", err)
			continue
		}

		points, err := ms.queryV6DG(dgq.dgDir, dgMeta, deviceFilter, fieldFilter, startNano, endNano)
		if err != nil {
			return nil, err
		}
		allPoints = append(allPoints, points...)
	}

	// Deduplicate: with append-based writes, duplicates can exist within a single DG
	// across different parts, so always deduplicate when we have results
	if len(allPoints) > 0 {
		allPoints = ms.deduplicatePoints(allPoints)
	}

	return allPoints, nil
}

// deduplicatePoints removes duplicates by (deviceID, time), keeping the one with latest InsertedAt
func (ms *Storage) deduplicatePoints(points []*DataPoint) []*DataPoint {
	pointMap := make(map[string]map[int64]*DataPoint)

	for _, p := range points {
		if pointMap[p.ID] == nil {
			pointMap[p.ID] = make(map[int64]*DataPoint)
		}
		existing := pointMap[p.ID][p.Time.UnixNano()]
		if existing == nil || p.InsertedAt.After(existing.InsertedAt) {
			pointMap[p.ID][p.Time.UnixNano()] = p
		}
	}

	result := make([]*DataPoint, 0, len(points))
	for _, timeMap := range pointMap {
		for _, p := range timeMap {
			result = append(result, p)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ID != result[j].ID {
			return result[i].ID < result[j].ID
		}
		return result[i].Time.Before(result[j].Time)
	})

	return result
}

// ============================================================
// Helper functions
// ============================================================

// findDateDirs finds date directories for a time range
func (ms *Storage) findDateDirs(database, collection string, startTime, endTime time.Time) ([]string, error) {
	var dirs []string

	baseDir := filepath.Join(ms.dataDir, database, collection)

	current := startTime.In(ms.timezone).Truncate(24 * time.Hour)
	end := endTime.In(ms.timezone).Truncate(24 * time.Hour)

	for !current.After(end) {
		date := current.Format("20060102")
		year := date[:4]
		month := date[4:6]

		dateDir := filepath.Join(baseDir, year, month, date)
		if _, err := os.Stat(dateDir); err == nil {
			dirs = append(dirs, dateDir)
		}

		current = current.Add(24 * time.Hour)
	}

	return dirs, nil
}

// collectFieldNames collects all unique field names from points
func (ms *Storage) collectFieldNames(points []*DataPoint) []string {
	fieldSet := make(map[string]bool)
	for _, p := range points {
		for fieldName := range p.Fields {
			if fieldName != "_inserted_at" {
				fieldSet[fieldName] = true
			}
		}
	}

	fields := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	return fields
}

// inferFieldTypes infers ColumnType for each field from data points
func (ms *Storage) inferFieldTypes(points []*DataPoint, fieldNames []string) []compression.ColumnType {
	types := make([]compression.ColumnType, len(fieldNames))
	found := make([]bool, len(fieldNames))

	for _, p := range points {
		allFound := true
		for i, fieldName := range fieldNames {
			if found[i] {
				continue
			}
			if val, ok := p.Fields[fieldName]; ok && val != nil {
				types[i] = inferType(val)
				found[i] = true
			}
			if !found[i] {
				allFound = false
			}
		}
		if allFound {
			break
		}
	}

	return types
}

// inferType infers ColumnType from a value
func inferType(val interface{}) compression.ColumnType {
	switch val.(type) {
	case float64, float32:
		return compression.ColumnTypeFloat64
	case int, int32, int64:
		return compression.ColumnTypeInt64
	case bool:
		return compression.ColumnTypeBool
	case string:
		return compression.ColumnTypeString
	default:
		return compression.ColumnTypeFloat64
	}
}

// ============================================================================
// V6 Part File Format — Single file with footer-based column index
// ============================================================================
//
// V6 removes Column Group directories. All columns for a part are stored in a
// single file with a footer index that enables seeking directly to any column.
//
// File layout:
//   [Header 64 bytes]
//   [Column chunks: device0._time, device0.field1, device0.field2, ...]
//   [Column chunks: device1._time, device1.field1, ...]
//   [Footer: ColumnIndex entries + FieldDictionary + DeviceIndex]
//   [FooterSize: 4 bytes]
//   [FooterOffset: 8 bytes] (last 8 bytes of file)
//
// This eliminates:
//   - CG directories (cg_XXXX/)
//   - CG metadata files (_metadata.idx in each CG dir)
//   - Duplicate _time columns across CG files
//   - O(fields/50) file opens per query
//
// Directory structure (V6):
//   dateDir/
//   ├── _metadata.idx           (Global metadata — simplified, no CG defs)
//   ├── dg_0000/
//   │   ├── _metadata.idx       (DG metadata — simplified, no CG refs)
//   │   ├── part_0000.bin       (Single file with ALL columns + footer)
//   │   └── part_0001.bin
//   └── dg_0001/
//       ├── _metadata.idx
//       └── part_0000.bin

const (
	// V6 part file magic: "SXPF" (Soltix Part File)
	V6PartFileMagic = 0x46505853

	// V6 header size
	V6HeaderSize = 64

	// V6 storage version
	V6StorageVersion = 6
)

// maxInt64 is the maximum int64 value for "no time filter" queries
const maxInt64 = int64(^uint64(0) >> 1)

// V6ColumnEntry describes a single column chunk in the footer index
type V6ColumnEntry struct {
	DeviceIdx  uint32 // Index into the device list
	FieldIdx   uint32 // Index into the field dictionary (0 = _time)
	Offset     int64  // Byte offset in the file
	Size       uint32 // Compressed size in bytes
	RowCount   uint32 // Number of rows in this chunk
	ColumnType uint8  // compression.ColumnType
}

// V6PartFooter is the in-memory representation of the footer
type V6PartFooter struct {
	DeviceNames    []string                 // Ordered device list
	FieldNames     []string                 // Field dictionary: index 0 = "_time", 1..N = sorted field names
	FieldTypes     []compression.ColumnType // Type per field (index 0 = INT64 for _time)
	Columns        []V6ColumnEntry          // All column entries
	RowCountPerDev []uint32                 // Row count per device
}

// writeV6PartFile writes a single V6 part file containing ALL columns with a footer index.
// The file contains: Header + column chunks (per device, per field) + Footer + FooterSize + FooterOffset.
func (ms *Storage) writeV6PartFile(
	partFilePath string,
	partIdx int,
	deviceIDs []string,
	deviceGroups map[string][]*DataPoint,
	fieldNames []string,
	fieldTypes []compression.ColumnType,
) (*V6PartFooter, *TmpFileInfo, error) {
	tmpPath := partFilePath + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create V6 part file: %w", err)
	}

	var writeErr error
	defer func() {
		_ = file.Close()
		if writeErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	// Reserve header space
	if writeErr = file.Truncate(V6HeaderSize); writeErr != nil {
		return nil, nil, writeErr
	}

	currentOffset := int64(V6HeaderSize)
	totalRows := uint32(0)

	// Build field dictionary: [_time, field0, field1, ...]
	dictFields := make([]string, 0, len(fieldNames)+2)
	dictFields = append(dictFields, "_time")
	dictFields = append(dictFields, fieldNames...)
	dictFields = append(dictFields, "_inserted_at")

	dictTypes := make([]compression.ColumnType, 0, len(fieldTypes)+2)
	dictTypes = append(dictTypes, compression.ColumnTypeInt64) // _time is always int64
	dictTypes = append(dictTypes, fieldTypes...)
	dictTypes = append(dictTypes, compression.ColumnTypeInt64) // _inserted_at is always int64 (UnixNano)

	var columns []V6ColumnEntry
	rowCountPerDev := make([]uint32, len(deviceIDs))

	for deviceIdx, deviceID := range deviceIDs {
		points := deviceGroups[deviceID]
		if len(points) == 0 {
			continue
		}

		rowCount := uint32(len(points))
		totalRows += rowCount
		rowCountPerDev[deviceIdx] = rowCount

		// Write _time column (field index 0)
		timestamps := make([]interface{}, len(points))
		for i, p := range points {
			timestamps[i] = p.Time.UnixNano()
		}

		tsEncoder := compression.NewDeltaEncoder()
		tsData, writeErr := tsEncoder.Encode(timestamps)
		if writeErr != nil {
			return nil, nil, writeErr
		}

		compressed, writeErr := ms.compressor.Compress(tsData)
		if writeErr != nil {
			return nil, nil, writeErr
		}

		if _, writeErr = file.WriteAt(compressed, currentOffset); writeErr != nil {
			return nil, nil, writeErr
		}

		columns = append(columns, V6ColumnEntry{
			DeviceIdx:  uint32(deviceIdx),
			FieldIdx:   0, // _time
			Offset:     currentOffset,
			Size:       uint32(len(compressed)),
			RowCount:   rowCount,
			ColumnType: uint8(compression.ColumnTypeInt64),
		})
		currentOffset += int64(len(compressed))

		// Write each field column
		for fIdx, fieldName := range fieldNames {
			values := make([]interface{}, len(points))
			hasNonNull := false
			for i, p := range points {
				values[i] = p.Fields[fieldName]
				if values[i] != nil {
					hasNonNull = true
				}
			}

			// Sparse optimization: skip columns that are entirely null
			if !hasNonNull {
				continue
			}

			// Use the caller-provided field type if available (already inferred
			// from ALL data points, not just this device's subset).  This avoids
			// picking the wrong encoder when a device's first non-nil value is nil
			// (InferColumnType defaults to Float64) and the actual values contain
			// strings or bools.
			var colType compression.ColumnType
			if fIdx < len(fieldTypes) && fieldTypes[fIdx] != compression.ColumnTypeNull {
				colType = fieldTypes[fIdx]
			} else {
				colType = compression.InferColumnType(values)
			}
			encoder := compression.GetEncoder(colType)

			colData, writeErr := encoder.Encode(values)
			if writeErr != nil {
				return nil, nil, fmt.Errorf("encode field %q (colType=%d, device=%s): %w",
					fieldName, colType, deviceIDs[deviceIdx], writeErr)
			}

			compressed, writeErr := ms.compressor.Compress(colData)
			if writeErr != nil {
				return nil, nil, writeErr
			}

			if _, writeErr = file.WriteAt(compressed, currentOffset); writeErr != nil {
				return nil, nil, writeErr
			}

			columns = append(columns, V6ColumnEntry{
				DeviceIdx:  uint32(deviceIdx),
				FieldIdx:   uint32(fIdx + 1), // +1 because 0 = _time
				Offset:     currentOffset,
				Size:       uint32(len(compressed)),
				RowCount:   rowCount,
				ColumnType: uint8(colType),
			})
			currentOffset += int64(len(compressed))
		}

		// Write _inserted_at column (last field index in dictFields)
		insertedAtFieldIdx := uint32(len(fieldNames) + 1) // +1 for _time at index 0
		insertedAtValues := make([]interface{}, len(points))
		for i, p := range points {
			if !p.InsertedAt.IsZero() {
				insertedAtValues[i] = p.InsertedAt.UnixNano()
			} else {
				insertedAtValues[i] = time.Now().UnixNano()
			}
		}

		iaEncoder := compression.NewDeltaEncoder()
		iaData, iaErr := iaEncoder.Encode(insertedAtValues)
		if iaErr != nil {
			writeErr = iaErr
			return nil, nil, writeErr
		}

		iaCompressed, iaErr := ms.compressor.Compress(iaData)
		if iaErr != nil {
			writeErr = iaErr
			return nil, nil, writeErr
		}

		if _, writeErr = file.WriteAt(iaCompressed, currentOffset); writeErr != nil {
			return nil, nil, writeErr
		}

		columns = append(columns, V6ColumnEntry{
			DeviceIdx:  uint32(deviceIdx),
			FieldIdx:   insertedAtFieldIdx,
			Offset:     currentOffset,
			Size:       uint32(len(iaCompressed)),
			RowCount:   rowCount,
			ColumnType: uint8(compression.ColumnTypeInt64),
		})
		currentOffset += int64(len(iaCompressed))
	}

	footer := &V6PartFooter{
		DeviceNames:    deviceIDs,
		FieldNames:     dictFields,
		FieldTypes:     dictTypes,
		Columns:        columns,
		RowCountPerDev: rowCountPerDev,
	}

	// Encode and write footer
	footerData := ms.encodeV6Footer(footer)
	footerOffset := currentOffset

	if _, writeErr = file.WriteAt(footerData, footerOffset); writeErr != nil {
		return nil, nil, writeErr
	}
	currentOffset += int64(len(footerData))

	// Write footer size (4 bytes)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(len(footerData)))
	if _, writeErr = file.WriteAt(sizeBuf, currentOffset); writeErr != nil {
		return nil, nil, writeErr
	}
	currentOffset += 4

	// Write footer offset (last 8 bytes)
	offsetBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBuf, uint64(footerOffset))
	if _, writeErr = file.WriteAt(offsetBuf, currentOffset); writeErr != nil {
		return nil, nil, writeErr
	}

	// Write header
	headerBuf := make([]byte, V6HeaderSize)
	binary.LittleEndian.PutUint32(headerBuf[0:], V6PartFileMagic)
	binary.LittleEndian.PutUint32(headerBuf[4:], V6StorageVersion)
	binary.LittleEndian.PutUint32(headerBuf[8:], uint32(partIdx))
	binary.LittleEndian.PutUint32(headerBuf[12:], totalRows)
	binary.LittleEndian.PutUint32(headerBuf[16:], uint32(len(deviceIDs)))
	binary.LittleEndian.PutUint32(headerBuf[20:], uint32(len(dictFields)))
	binary.LittleEndian.PutUint32(headerBuf[24:], uint32(len(columns)))

	if _, writeErr = file.WriteAt(headerBuf, 0); writeErr != nil {
		return nil, nil, writeErr
	}

	if writeErr = file.Sync(); writeErr != nil {
		return nil, nil, writeErr
	}
	_ = file.Close()

	tmpInfo := &TmpFileInfo{
		TmpPath:   tmpPath,
		FinalPath: partFilePath,
	}

	return footer, tmpInfo, nil
}

// encodeV6Footer serializes the footer to binary
func (ms *Storage) encodeV6Footer(footer *V6PartFooter) []byte {
	buf := make([]byte, 0, 4096)
	intBuf := make([]byte, 8)

	// Device names
	binary.LittleEndian.PutUint32(intBuf, uint32(len(footer.DeviceNames)))
	buf = append(buf, intBuf[:4]...)
	for _, name := range footer.DeviceNames {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, nameBytes...)
	}

	// Row count per device
	for _, rc := range footer.RowCountPerDev {
		binary.LittleEndian.PutUint32(intBuf, rc)
		buf = append(buf, intBuf[:4]...)
	}

	// Field dictionary
	binary.LittleEndian.PutUint32(intBuf, uint32(len(footer.FieldNames)))
	buf = append(buf, intBuf[:4]...)
	for _, name := range footer.FieldNames {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(intBuf, uint16(len(nameBytes)))
		buf = append(buf, intBuf[:2]...)
		buf = append(buf, nameBytes...)
	}

	// Field types
	for _, t := range footer.FieldTypes {
		buf = append(buf, byte(t))
	}

	// Column entries
	binary.LittleEndian.PutUint32(intBuf, uint32(len(footer.Columns)))
	buf = append(buf, intBuf[:4]...)
	for _, col := range footer.Columns {
		binary.LittleEndian.PutUint32(intBuf, col.DeviceIdx)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, col.FieldIdx)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint64(intBuf, uint64(col.Offset))
		buf = append(buf, intBuf[:8]...)
		binary.LittleEndian.PutUint32(intBuf, col.Size)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, col.RowCount)
		buf = append(buf, intBuf[:4]...)
		buf = append(buf, col.ColumnType)
	}

	return buf
}

// readV6PartFooter reads the footer from a V6 part file
func (ms *Storage) readV6PartFooter(filePath string) (*V6PartFooter, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	// Read file size
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	if fileSize < V6HeaderSize+12 {
		return nil, fmt.Errorf("V6 part file too small: %d bytes", fileSize)
	}

	// Read footer offset (last 8 bytes)
	offsetBuf := make([]byte, 8)
	if _, err := file.ReadAt(offsetBuf, fileSize-8); err != nil {
		return nil, fmt.Errorf("failed to read footer offset: %w", err)
	}
	footerOffset := int64(binary.LittleEndian.Uint64(offsetBuf))

	// Read footer size (4 bytes before footer offset)
	sizeBuf := make([]byte, 4)
	if _, err := file.ReadAt(sizeBuf, fileSize-12); err != nil {
		return nil, fmt.Errorf("failed to read footer size: %w", err)
	}
	footerSize := int(binary.LittleEndian.Uint32(sizeBuf))

	if footerOffset < V6HeaderSize || footerOffset+int64(footerSize) > fileSize-12 {
		return nil, fmt.Errorf("invalid footer offset/size: offset=%d size=%d fileSize=%d", footerOffset, footerSize, fileSize)
	}

	// Read footer data
	footerData := make([]byte, footerSize)
	if _, err := file.ReadAt(footerData, footerOffset); err != nil {
		return nil, fmt.Errorf("failed to read footer data: %w", err)
	}

	return ms.decodeV6Footer(footerData)
}

// decodeV6Footer deserializes footer from binary
func (ms *Storage) decodeV6Footer(data []byte) (*V6PartFooter, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("V6 footer too short")
	}

	offset := 0
	footer := &V6PartFooter{}

	// Device names
	deviceCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	footer.DeviceNames = make([]string, deviceCount)
	for i := 0; i < deviceCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("V6 footer truncated at device name %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("V6 footer truncated at device name %d data", i)
		}
		footer.DeviceNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Row count per device
	footer.RowCountPerDev = make([]uint32, deviceCount)
	for i := 0; i < deviceCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("V6 footer truncated at row count %d", i)
		}
		footer.RowCountPerDev[i] = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}

	// Field dictionary
	if offset+4 > len(data) {
		return nil, fmt.Errorf("V6 footer truncated at field count")
	}
	fieldCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	footer.FieldNames = make([]string, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("V6 footer truncated at field name %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("V6 footer truncated at field name %d data", i)
		}
		footer.FieldNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Field types
	footer.FieldTypes = make([]compression.ColumnType, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if offset >= len(data) {
			return nil, fmt.Errorf("V6 footer truncated at field type %d", i)
		}
		footer.FieldTypes[i] = compression.ColumnType(data[offset])
		offset++
	}

	// Column entries
	if offset+4 > len(data) {
		return nil, fmt.Errorf("V6 footer truncated at column count")
	}
	colCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	footer.Columns = make([]V6ColumnEntry, colCount)
	for i := 0; i < colCount; i++ {
		if offset+25 > len(data) {
			return nil, fmt.Errorf("V6 footer truncated at column entry %d", i)
		}
		footer.Columns[i] = V6ColumnEntry{
			DeviceIdx:  binary.LittleEndian.Uint32(data[offset:]),
			FieldIdx:   binary.LittleEndian.Uint32(data[offset+4:]),
			Offset:     int64(binary.LittleEndian.Uint64(data[offset+8:])),
			Size:       binary.LittleEndian.Uint32(data[offset+16:]),
			RowCount:   binary.LittleEndian.Uint32(data[offset+20:]),
			ColumnType: data[offset+24],
		}
		offset += 25
	}

	return footer, nil
}

// readV6PartFileData reads specific columns from a V6 part file using the footer index.
// It opens the file once and seeks to each needed column.
func (ms *Storage) readV6PartFileData(
	filePath string,
	footer *V6PartFooter,
	deviceFilter map[string]bool,
	fieldFilter map[string]bool,
	startNano, endNano int64,
) ([]*DataPoint, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	// Build device index map: name → index
	deviceIdxMap := make(map[string]int, len(footer.DeviceNames))
	for i, name := range footer.DeviceNames {
		deviceIdxMap[name] = i
	}

	// Build field index map: name → index in footer.FieldNames
	fieldIdxMap := make(map[string]int, len(footer.FieldNames))
	for i, name := range footer.FieldNames {
		fieldIdxMap[name] = i
	}

	// Build column lookup: (deviceIdx, fieldIdx) → ColumnEntry
	type colKey struct {
		deviceIdx uint32
		fieldIdx  uint32
	}
	colMap := make(map[colKey]*V6ColumnEntry, len(footer.Columns))
	for i := range footer.Columns {
		col := &footer.Columns[i]
		colMap[colKey{col.DeviceIdx, col.FieldIdx}] = col
	}

	// Determine which devices to read
	var targetDevices []int
	for devIdx, devName := range footer.DeviceNames {
		if len(deviceFilter) > 0 && !deviceFilter[devName] {
			continue
		}
		targetDevices = append(targetDevices, devIdx)
	}

	// Determine which fields to read (indices in footer.FieldNames)
	var targetFieldIdxs []int
	if len(fieldFilter) > 0 {
		// Always include _time (index 0)
		targetFieldIdxs = append(targetFieldIdxs, 0)
		for fieldName := range fieldFilter {
			if idx, ok := fieldIdxMap[fieldName]; ok && idx != 0 {
				targetFieldIdxs = append(targetFieldIdxs, idx)
			}
		}
	} else {
		// All fields
		for i := range footer.FieldNames {
			targetFieldIdxs = append(targetFieldIdxs, i)
		}
	}

	var allPoints []*DataPoint

	for _, devIdx := range targetDevices {
		deviceName := footer.DeviceNames[devIdx]
		rowCount := int(footer.RowCountPerDev[devIdx])
		if rowCount == 0 {
			continue
		}

		// Read _time column first
		timeCol := colMap[colKey{uint32(devIdx), 0}]
		if timeCol == nil || timeCol.Size == 0 {
			continue
		}

		timeTC, err := ms.readV6ColumnTyped(file, timeCol, rowCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read _time for device %s: %w", deviceName, err)
		}

		// Build time mask using typed int64 slice (no boxing)
		timeMask := make([]bool, timeTC.len())
		matchCount := 0
		for i, tsNano := range timeTC.int64s {
			if !timeTC.nulls[i] && tsNano >= startNano && tsNano <= endNano {
				timeMask[i] = true
				matchCount++
			}
		}

		if matchCount == 0 {
			continue
		}

		// Read other field columns using typed decode (avoids 1000 allocs per column)
		typedFieldData := make(map[string]*typedColumn)
		for _, fIdx := range targetFieldIdxs {
			if fIdx == 0 {
				continue // skip _time, already read
			}
			fieldName := footer.FieldNames[fIdx]

			col := colMap[colKey{uint32(devIdx), uint32(fIdx)}]
			if col == nil || col.Size == 0 {
				continue // sparse: this device doesn't have this field
			}

			tc, err := ms.readV6ColumnTyped(file, col, rowCount)
			if err != nil {
				return nil, fmt.Errorf("failed to read field %s for device %s: %w", fieldName, deviceName, err)
			}
			typedFieldData[fieldName] = tc
		}

		// Read _inserted_at column if present (for last-write-wins deduplication)
		var insertedAtTC *typedColumn
		if iaIdx, ok := fieldIdxMap["_inserted_at"]; ok {
			iaCol := colMap[colKey{uint32(devIdx), uint32(iaIdx)}]
			if iaCol != nil && iaCol.Size > 0 {
				iaTC, iaErr := ms.readV6ColumnTyped(file, iaCol, rowCount)
				if iaErr == nil {
					insertedAtTC = iaTC
				}
			}
		}

		// Construct DataPoints for matching timestamps only.
		// Boxing (interface{} alloc) only happens here for matched rows,
		// not for all N rows — reducing allocs from O(N*cols) to O(matchCount*cols).
		for i, include := range timeMask {
			if !include {
				continue
			}

			fields := make(map[string]interface{})
			for fieldName, tc := range typedFieldData {
				if i < tc.len() {
					v := tc.valueAt(i)
					if v != nil {
						fields[fieldName] = v
					}
				}
			}

			dp := &DataPoint{
				ID:     deviceName,
				Time:   time.Unix(0, timeTC.int64s[i]),
				Fields: fields,
			}

			// Restore InsertedAt from persisted _inserted_at column
			if insertedAtTC != nil && i < insertedAtTC.len() && !insertedAtTC.nulls[i] {
				dp.InsertedAt = time.Unix(0, insertedAtTC.int64s[i])
			}

			allPoints = append(allPoints, dp)
		}
	}

	return allPoints, nil
}

// typedColumn holds column data in its native type to avoid interface{} boxing allocations.
// Values are only boxed to interface{} on demand via valueAt(), typically for the subset
// of rows matching a time filter — reducing allocs from O(N) to O(matchCount).
type typedColumn struct {
	colType  compression.ColumnType
	float64s []float64
	int64s   []int64
	strings  []string
	boolVals []bool
	nulls    []bool
}

// valueAt returns the value at index i as interface{}, boxing only when called.
func (tc *typedColumn) valueAt(i int) interface{} {
	if tc.nulls != nil && i < len(tc.nulls) && tc.nulls[i] {
		return nil
	}
	switch tc.colType {
	case compression.ColumnTypeFloat64:
		if i < len(tc.float64s) {
			return tc.float64s[i]
		}
	case compression.ColumnTypeInt64:
		if i < len(tc.int64s) {
			return tc.int64s[i]
		}
	case compression.ColumnTypeString:
		if i < len(tc.strings) {
			return tc.strings[i]
		}
	case compression.ColumnTypeBool:
		if i < len(tc.boolVals) {
			return tc.boolVals[i]
		}
	}
	return nil
}

// len returns the number of values in the typed column.
func (tc *typedColumn) len() int {
	return len(tc.nulls)
}

// readV6Column reads and decodes a single column chunk from a V6 part file
func (ms *Storage) readV6Column(file *os.File, col *V6ColumnEntry, rowCount int) ([]interface{}, error) {
	tc, err := ms.readV6ColumnTyped(file, col, rowCount)
	if err != nil {
		return nil, err
	}
	if tc == nil {
		return nil, nil
	}
	values := make([]interface{}, tc.len())
	for i := range values {
		values[i] = tc.valueAt(i)
	}
	return values, nil
}

// readV6ColumnTyped reads and decodes a column using type-specific decoders,
// avoiding ~1000 interface{} boxing allocations per column.
func (ms *Storage) readV6ColumnTyped(file *os.File, col *V6ColumnEntry, rowCount int) (*typedColumn, error) {
	compressed := make([]byte, col.Size)
	if _, err := file.ReadAt(compressed, col.Offset); err != nil {
		return nil, err
	}

	decompressed, err := ms.compressor.Decompress(compressed)
	if err != nil {
		return nil, err
	}

	colType := compression.ColumnType(col.ColumnType)

	switch colType {
	case compression.ColumnTypeInt64:
		encoder := compression.NewDeltaEncoder()
		vals, nulls, err := encoder.DecodeInt64(decompressed, rowCount)
		if err != nil {
			return nil, err
		}
		return &typedColumn{colType: colType, int64s: vals, nulls: nulls}, nil

	case compression.ColumnTypeFloat64:
		encoder := compression.NewGorillaEncoder()
		vals, nulls, err := encoder.DecodeFloat64(decompressed, rowCount)
		if err != nil {
			return nil, err
		}
		return &typedColumn{colType: colType, float64s: vals, nulls: nulls}, nil

	case compression.ColumnTypeString:
		encoder := compression.NewDictionaryEncoder()
		vals, nulls, err := encoder.DecodeStrings(decompressed, rowCount)
		if err != nil {
			return nil, err
		}
		return &typedColumn{colType: colType, strings: vals, nulls: nulls}, nil

	case compression.ColumnTypeBool:
		encoder := compression.NewBoolEncoder()
		vals, err := encoder.Decode(decompressed, rowCount)
		if err != nil {
			return nil, err
		}
		// BoolEncoder already returns []interface{} with only 1 alloc, convert to typed
		boolVals := make([]bool, len(vals))
		nulls := make([]bool, len(vals))
		for i, v := range vals {
			if v == nil {
				nulls[i] = true
			} else {
				boolVals[i] = v.(bool)
			}
		}
		return &typedColumn{colType: colType, boolVals: boolVals, nulls: nulls}, nil

	default:
		// Fallback to generic decode
		encoder := compression.GetEncoder(colType)
		vals, err := encoder.Decode(decompressed, rowCount)
		if err != nil {
			return nil, err
		}
		// Wrap as float64 column (default)
		float64s := make([]float64, len(vals))
		nulls := make([]bool, len(vals))
		for i, v := range vals {
			if v == nil {
				nulls[i] = true
			} else if f, ok := v.(float64); ok {
				float64s[i] = f
			}
		}
		return &typedColumn{colType: compression.ColumnTypeFloat64, float64s: float64s, nulls: nulls}, nil
	}
}

// readAllV6PartsInDG reads all data from all V6 part files in a DG directory
func (ms *Storage) readAllV6PartsInDG(dgDir string, dgMeta *DeviceGroupMetadata) []*DataPoint {
	var allPoints []*DataPoint

	for _, partFileName := range dgMeta.PartFileNames {
		partPath := filepath.Join(dgDir, partFileName)

		footer, err := ms.readV6PartFooter(partPath)
		if err != nil {
			ms.logger.Warn("Failed to read V6 part footer", "path", partPath, "error", err)
			continue
		}

		// Read all data (no filters)
		points, err := ms.readV6PartFileData(
			partPath, footer,
			nil, nil, // no device/field filters
			0, maxInt64, // all time
		)
		if err != nil {
			ms.logger.Warn("Failed to read V6 part data", "path", partPath, "error", err)
			continue
		}

		allPoints = append(allPoints, points...)
	}

	return allPoints
}

// queryV6DG queries a V6 device group by reading part files directly
func (ms *Storage) queryV6DG(dgDir string, dgMeta *DeviceGroupMetadata, deviceFilter, fieldFilter map[string]bool, startNano, endNano int64) ([]*DataPoint, error) {
	var allPoints []*DataPoint

	for partIdx, partFileName := range dgMeta.PartFileNames {
		partPath := filepath.Join(dgDir, partFileName)

		// Time range filter via part manifests
		if partIdx < len(dgMeta.Parts) {
			pm := dgMeta.Parts[partIdx]
			if pm.MaxTimestamp < startNano || pm.MinTimestamp > endNano {
				continue
			}
		}

		footer, err := ms.readV6PartFooter(partPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read V6 part footer %s: %w", partPath, err)
		}

		points, err := ms.readV6PartFileData(partPath, footer, deviceFilter, fieldFilter, startNano, endNano)
		if err != nil {
			return nil, fmt.Errorf("failed to query V6 part %s: %w", partPath, err)
		}

		allPoints = append(allPoints, points...)
	}

	return allPoints, nil
}

// writeV6DeviceGroupDir writes a V6 device group directory with part files directly inside.
// No CG subdirectories are created.
func (ms *Storage) writeV6DeviceGroupDir(
	dateDir string,
	dgIdx int,
	deviceGroups map[string][]*DataPoint,
	fieldNames []string,
	fieldTypes []compression.ColumnType,
) (DeviceGroupManifest, *DeviceGroupMetadata, []*TmpFileInfo, error) {
	dgDirName := fmt.Sprintf("dg_%04d", dgIdx)
	dgDir := filepath.Join(dateDir, dgDirName)

	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		return DeviceGroupManifest{}, nil, nil, fmt.Errorf("failed to create DG dir: %w", err)
	}

	// Split devices within this DG into parts by row count
	parts := ms.splitIntoParts(deviceGroups)

	dgMeta := &DeviceGroupMetadata{
		Magic:         DeviceGroupMetadataMagic,
		Version:       V6StorageVersion,
		GroupIndex:    uint32(dgIdx),
		DevicePartMap: make(map[string][]int),
	}

	var allTmpInfos []*TmpFileInfo
	var totalRows uint32
	var minTs, maxTs int64

	deviceIDs := make([]string, 0, len(deviceGroups))
	for id := range deviceGroups {
		deviceIDs = append(deviceIDs, id)
	}
	sort.Strings(deviceIDs)

	// Track device→part mapping from the parts split
	for partIdx, partData := range parts {
		for deviceID := range partData {
			dgMeta.DevicePartMap[deviceID] = append(dgMeta.DevicePartMap[deviceID], partIdx)
		}
	}

	// Calculate total rows and time range from parts
	for _, partData := range parts {
		for _, points := range partData {
			totalRows += uint32(len(points))
			for _, p := range points {
				ts := p.Time.UnixNano()
				if minTs == 0 || ts < minTs {
					minTs = ts
				}
				if ts > maxTs {
					maxTs = ts
				}
			}
		}
	}

	// Write each part as a single V6 file directly in DG dir
	partFileNames := make([]string, len(parts))
	for partIdx, partData := range parts {
		partFileName := fmt.Sprintf("part_%04d.bin", partIdx)
		partFilePath := filepath.Join(dgDir, partFileName)
		partFileNames[partIdx] = partFileName

		// Get sorted device IDs for this part
		partDeviceIDs := make([]string, 0, len(partData))
		for id := range partData {
			partDeviceIDs = append(partDeviceIDs, id)
		}
		sort.Strings(partDeviceIDs)

		// Build part stats
		var partRows uint32
		var partMinTs, partMaxTs int64
		partRowCounts := make([]uint32, len(partDeviceIDs))
		for di, deviceID := range partDeviceIDs {
			points := partData[deviceID]
			partRowCounts[di] = uint32(len(points))
			partRows += uint32(len(points))
			for _, p := range points {
				ts := p.Time.UnixNano()
				if partMinTs == 0 || ts < partMinTs {
					partMinTs = ts
				}
				if ts > partMaxTs {
					partMaxTs = ts
				}
			}
		}

		_, tmpInfo, err := ms.writeV6PartFile(partFilePath, partIdx, partDeviceIDs, partData, fieldNames, fieldTypes)
		if err != nil {
			ms.cleanupTmpFiles(allTmpInfos)
			return DeviceGroupManifest{}, nil, nil, fmt.Errorf("failed to write V6 part %d in DG %d: %w", partIdx, dgIdx, err)
		}
		allTmpInfos = append(allTmpInfos, tmpInfo)

		dgMeta.Parts = append(dgMeta.Parts, PartManifest{
			FileName:     partFileName,
			PartIndex:    uint32(partIdx),
			RowCount:     partRows,
			DeviceCount:  uint32(len(partDeviceIDs)),
			MinTimestamp: partMinTs,
			MaxTimestamp: partMaxTs,
			DeviceNames:  partDeviceIDs,
			RowCounts:    partRowCounts,
			IsSorted:     true,
		})
	}

	dgMeta.DeviceCount = uint32(len(deviceIDs))
	dgMeta.PartCount = uint32(len(parts))
	dgMeta.RowCount = totalRows
	dgMeta.DeviceNames = deviceIDs
	dgMeta.PartFileNames = partFileNames

	// Write DG metadata
	dgMetaPath := filepath.Join(dgDir, "_metadata.idx")
	dgMetaTmpInfo, err := ms.writeDeviceGroupMetadata(dgMetaPath, dgMeta)
	if err != nil {
		ms.cleanupTmpFiles(allTmpInfos)
		return DeviceGroupManifest{}, nil, nil, fmt.Errorf("failed to write DG metadata: %w", err)
	}
	allTmpInfos = append(allTmpInfos, dgMetaTmpInfo)

	dgManifest := DeviceGroupManifest{
		DirName:      dgDirName,
		GroupIndex:   uint32(dgIdx),
		DeviceCount:  uint32(len(deviceIDs)),
		PartCount:    uint32(len(parts)),
		RowCount:     totalRows,
		MinTimestamp: minTs,
		MaxTimestamp: maxTs,
		DeviceNames:  deviceIDs,
	}

	return dgManifest, dgMeta, allTmpInfos, nil
}

// appendV6PartsToExistingDG appends new V6 part files to an existing DG.
func (ms *Storage) appendV6PartsToExistingDG(
	dgDir string,
	dgIdx int,
	existingDG DeviceGroupManifest,
	incomingData map[string][]*DataPoint,
	fieldNames []string,
	fieldTypes []compression.ColumnType,
) (DeviceGroupManifest, []*TmpFileInfo, error) {
	// Read existing DG metadata to know current part count
	dgMetaPath := filepath.Join(dgDir, "_metadata.idx")
	dgMeta, err := ms.readDeviceGroupMetadata(dgMetaPath)
	if err != nil {
		return DeviceGroupManifest{}, nil, fmt.Errorf("failed to read DG metadata: %w", err)
	}

	// Split incoming data into parts by row count
	newParts := ms.splitIntoParts(incomingData)

	var allTmpInfos []*TmpFileInfo
	var newPartRows uint32
	var newMinTs, newMaxTs int64

	// Calculate stats for new data
	for _, partData := range newParts {
		for _, points := range partData {
			newPartRows += uint32(len(points))
			for _, p := range points {
				ts := p.Time.UnixNano()
				if newMinTs == 0 || ts < newMinTs {
					newMinTs = ts
				}
				if ts > newMaxTs {
					newMaxTs = ts
				}
			}
		}
	}

	existingPartCount := int(dgMeta.PartCount)

	// Write new part files starting from existingPartCount
	for newPartIdx, partData := range newParts {
		partIdx := existingPartCount + newPartIdx
		partFileName := fmt.Sprintf("part_%04d.bin", partIdx)
		partFilePath := filepath.Join(dgDir, partFileName)

		// Get sorted device IDs for this part
		partDeviceIDs := make([]string, 0, len(partData))
		for id := range partData {
			partDeviceIDs = append(partDeviceIDs, id)
		}
		sort.Strings(partDeviceIDs)

		// Build part stats
		var partRows uint32
		var partMinTs, partMaxTs int64
		partRowCounts := make([]uint32, len(partDeviceIDs))
		for di, deviceID := range partDeviceIDs {
			points := partData[deviceID]
			partRowCounts[di] = uint32(len(points))
			partRows += uint32(len(points))
			for _, p := range points {
				ts := p.Time.UnixNano()
				if partMinTs == 0 || ts < partMinTs {
					partMinTs = ts
				}
				if ts > partMaxTs {
					partMaxTs = ts
				}
			}
		}

		_, tmpInfo, err := ms.writeV6PartFile(partFilePath, partIdx, partDeviceIDs, partData, fieldNames, fieldTypes)
		if err != nil {
			ms.cleanupTmpFiles(allTmpInfos)
			return DeviceGroupManifest{}, nil, fmt.Errorf("failed to write appended V6 part %d: %w", partIdx, err)
		}
		allTmpInfos = append(allTmpInfos, tmpInfo)

		dgMeta.PartFileNames = append(dgMeta.PartFileNames, partFileName)
		dgMeta.Parts = append(dgMeta.Parts, PartManifest{
			FileName:     partFileName,
			PartIndex:    uint32(partIdx),
			RowCount:     partRows,
			DeviceCount:  uint32(len(partDeviceIDs)),
			MinTimestamp: partMinTs,
			MaxTimestamp: partMaxTs,
			DeviceNames:  partDeviceIDs,
			RowCounts:    partRowCounts,
			IsSorted:     true,
		})

		// Update device→part mapping
		for _, deviceID := range partDeviceIDs {
			dgMeta.DevicePartMap[deviceID] = append(dgMeta.DevicePartMap[deviceID], partIdx)
		}
	}

	// Merge device names: existing DG devices + new incoming devices
	deviceSet := make(map[string]bool)
	for _, d := range existingDG.DeviceNames {
		deviceSet[d] = true
	}
	for d := range incomingData {
		deviceSet[d] = true
	}
	allDeviceIDs := make([]string, 0, len(deviceSet))
	for id := range deviceSet {
		allDeviceIDs = append(allDeviceIDs, id)
	}
	sort.Strings(allDeviceIDs)

	// Update DG metadata
	newPartCount := existingPartCount + len(newParts)
	dgMeta.DeviceCount = uint32(len(allDeviceIDs))
	dgMeta.PartCount = uint32(newPartCount)
	dgMeta.RowCount += newPartRows
	dgMeta.DeviceNames = allDeviceIDs

	// Write updated DG metadata
	dgMetaTmpInfo, err := ms.writeDeviceGroupMetadata(dgMetaPath, dgMeta)
	if err != nil {
		ms.cleanupTmpFiles(allTmpInfos)
		return DeviceGroupManifest{}, nil, fmt.Errorf("failed to rewrite DG metadata: %w", err)
	}
	allTmpInfos = append(allTmpInfos, dgMetaTmpInfo)

	// Build updated DG manifest
	minTs := existingDG.MinTimestamp
	maxTs := existingDG.MaxTimestamp
	if minTs == 0 || (newMinTs > 0 && newMinTs < minTs) {
		minTs = newMinTs
	}
	if newMaxTs > maxTs {
		maxTs = newMaxTs
	}

	updatedManifest := DeviceGroupManifest{
		DirName:      existingDG.DirName,
		GroupIndex:   existingDG.GroupIndex,
		DeviceCount:  uint32(len(allDeviceIDs)),
		PartCount:    uint32(newPartCount),
		RowCount:     existingDG.RowCount + newPartRows,
		MinTimestamp: minTs,
		MaxTimestamp: maxTs,
		DeviceNames:  allDeviceIDs,
	}

	return updatedManifest, allTmpInfos, nil
}

// collectFieldsFromData extracts sorted field names and their inferred types
// from a set of device→points data. Used by compaction where we don't have
// separate old/new field lists.
func collectFieldsFromData(deviceGroups map[string][]*DataPoint) ([]string, []compression.ColumnType) {
	fieldSet := make(map[string]bool)
	fieldSample := make(map[string]interface{})

	for _, points := range deviceGroups {
		for _, p := range points {
			for k, v := range p.Fields {
				fieldSet[k] = true
				if fieldSample[k] == nil && v != nil {
					fieldSample[k] = v
				}
			}
		}
	}

	fieldNames := make([]string, 0, len(fieldSet))
	for k := range fieldSet {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)

	fieldTypes := make([]compression.ColumnType, len(fieldNames))
	for i, name := range fieldNames {
		if sample := fieldSample[name]; sample != nil {
			fieldTypes[i] = compression.InferColumnType([]interface{}{sample})
		}
	}

	return fieldNames, fieldTypes
}

// cleanupV6StaleParts removes old part files from a DG directory
// that are no longer referenced after compaction.
func (ms *Storage) cleanupV6StaleParts(dgDir string, manifest DeviceGroupManifest) {
	// Build set of valid file names
	validFiles := make(map[string]bool)
	validFiles["_metadata.idx"] = true
	validFiles["_metadata.idx.tmp"] = true

	dgMeta, err := ms.readDeviceGroupMetadata(filepath.Join(dgDir, "_metadata.idx"))
	if err != nil {
		ms.logger.Warn("Failed to read DG metadata for V6 cleanup", "dir", dgDir, "error", err)
		return
	}

	for _, partName := range dgMeta.PartFileNames {
		validFiles[partName] = true
	}

	// Scan directory and remove stale files
	entries, err := os.ReadDir(dgDir)
	if err != nil {
		return
	}

	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if validFiles[name] {
			continue
		}
		ext := filepath.Ext(name)
		if ext == ".bin" || ext == ".tmp" {
			stalePath := filepath.Join(dgDir, name)
			if err := os.Remove(stalePath); err != nil {
				ms.logger.Warn("Failed to remove stale V6 file", "path", stalePath, "error", err)
			} else {
				removed++
			}
		}
	}

	if removed > 0 {
		ms.logger.Debug("Cleaned up stale V6 files after compaction",
			"dg_dir", dgDir,
			"removed", removed)
	}
}
