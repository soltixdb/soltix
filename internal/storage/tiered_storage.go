package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// ============================================================================
// 3-Tier Storage Architecture
// ============================================================================
//
// Tier 1: Group      — hash(db, collection, device_id) % TotalGroups → group_XXXX
// Tier 2: Device     — within a group, data is organized per device_id → dg_XXXX
// Tier 3: Partition  — time-based partitioning within device groups → part_XXXX.bin
//
// Note: V6 columnar format stores all columns inside each partition file,
// eliminating the separate Column Group (cg_XXXX/) directory tier from V5.
//
// Directory layout:
//   data/{db}/{collection}/group_{gid}/{year}/{month}/{date}/dg_XXXX/part_XXXX.bin
//
// Sync boundary: only replica nodes within the same group sync with each other.
//
// ============================================================================

// GroupStorageConfig holds configuration for group-aware tiered storage
type GroupStorageConfig struct {
	DataDir            string
	Compression        compression.Algorithm
	MaxRowsPerPart     int
	MaxPartSize        int64
	MinRowsPerPart     int
	MaxDevicesPerGroup int
	Timezone           *time.Location
}

// DefaultGroupStorageConfig returns a default configuration
func DefaultGroupStorageConfig(dataDir string) GroupStorageConfig {
	return GroupStorageConfig{
		DataDir:            dataDir,
		Compression:        compression.Snappy,
		MaxRowsPerPart:     DefaultMaxRowsPerPart,
		MaxPartSize:        DefaultMaxPartSize,
		MinRowsPerPart:     DefaultMinRowsPerPart,
		MaxDevicesPerGroup: DefaultMaxDevicesPerGroup,
	}
}

// TieredStorage is the top-level group-aware storage engine.
// It routes writes and queries through the group tier before delegating
// to the underlying columnar Storage (which handles device/partition tiers).
type TieredStorage struct {
	mu     sync.RWMutex
	config GroupStorageConfig
	logger *logging.Logger

	// Per-group Storage instances (lazy-created)
	groupEngines map[int]*Storage
	enginesMu    sync.RWMutex
}

// NewTieredStorage creates a new 3-tier group-aware storage engine
func NewTieredStorage(config GroupStorageConfig, logger *logging.Logger) *TieredStorage {
	_ = os.MkdirAll(config.DataDir, 0o755)

	logger.Info("Tiered storage initialized (Group → Device → Partition)",
		"data_dir", config.DataDir,
		"max_rows_per_part", config.MaxRowsPerPart,
		"max_devices_per_group", config.MaxDevicesPerGroup)

	return &TieredStorage{
		config:       config,
		logger:       logger,
		groupEngines: make(map[int]*Storage),
	}
}

// ============================================================================
// Path Builder
// ============================================================================

// GroupDir returns the directory path for a specific group
// e.g., data/group_0042
func (ts *TieredStorage) GroupDir(groupID int) string {
	return filepath.Join(ts.config.DataDir, fmt.Sprintf("group_%04d", groupID))
}

// GroupDateDir returns the full date directory path within a group
// e.g., data/group_0042/{db}/{collection}/{year}/{month}/{date}
func (ts *TieredStorage) GroupDateDir(groupID int, database, collection string, t time.Time) string {
	tz := ts.config.Timezone
	if tz == nil {
		tz = time.UTC
	}
	lt := t.In(tz)
	return filepath.Join(
		ts.GroupDir(groupID),
		database,
		collection,
		lt.Format("2006"),
		lt.Format("01"),
		lt.Format("20060102"),
	)
}

// ParseGroupID extracts the group ID from a group directory name
// e.g., "group_0042" → 42
func ParseGroupID(dirName string) (int, error) {
	var groupID int
	_, err := fmt.Sscanf(dirName, "group_%d", &groupID)
	if err != nil {
		return 0, fmt.Errorf("invalid group dir name %q: %w", dirName, err)
	}
	return groupID, nil
}

// ============================================================================
// Group Engine Management
// ============================================================================

// getOrCreateEngine returns the Storage engine for a group, creating it if needed
func (ts *TieredStorage) getOrCreateEngine(groupID int) *Storage {
	ts.enginesMu.RLock()
	engine, exists := ts.groupEngines[groupID]
	ts.enginesMu.RUnlock()

	if exists {
		return engine
	}

	ts.enginesMu.Lock()
	defer ts.enginesMu.Unlock()

	// Double-check
	if engine, exists = ts.groupEngines[groupID]; exists {
		return engine
	}

	groupDir := ts.GroupDir(groupID)
	engine = NewStorageWithConfig(
		groupDir,
		ts.config.Compression,
		ts.logger,
		ts.config.MaxRowsPerPart,
		ts.config.MaxPartSize,
		ts.config.MinRowsPerPart,
		ts.config.MaxDevicesPerGroup,
	)

	if ts.config.Timezone != nil {
		engine.SetTimezone(ts.config.Timezone)
	}
	ts.groupEngines[groupID] = engine

	ts.logger.Debug("Created storage engine for group",
		"group_id", groupID,
		"dir", groupDir)

	return engine
}

// GetEngine returns the storage engine for a specific group (public access)
func (ts *TieredStorage) GetEngine(groupID int) *Storage {
	return ts.getOrCreateEngine(groupID)
}

// ============================================================================
// Write Operations
// ============================================================================

// WriteBatch writes data points to the appropriate group-partitioned storage.
// Each DataPoint must have GroupID set (via coordinator routing).
func (ts *TieredStorage) WriteBatch(entries []*DataPoint) error {
	if len(entries) == 0 {
		return nil
	}

	// Group entries by GroupID
	grouped := ts.groupByGroupID(entries)

	ts.logger.Info("Tiered storage write batch",
		"total_entries", len(entries),
		"groups", len(grouped))

	// Write each group's entries to its own storage engine
	for groupID, points := range grouped {
		engine := ts.getOrCreateEngine(groupID)
		if err := engine.WriteBatch(points); err != nil {
			return fmt.Errorf("failed to write group %d: %w", groupID, err)
		}
	}

	return nil
}

// groupByGroupID groups DataPoints by their GroupID
func (ts *TieredStorage) groupByGroupID(entries []*DataPoint) map[int][]*DataPoint {
	result := make(map[int][]*DataPoint)
	for _, entry := range entries {
		result[entry.GroupID] = append(result[entry.GroupID], entry)
	}
	return result
}

// ============================================================================
// Query Operations
// ============================================================================

// Query retrieves data points from group storage engines.
// deviceIDs can be nil/empty — validation is done at the router layer.
// Internally, aggregation pipeline also calls this with nil deviceIDs to scan all raw data.
func (ts *TieredStorage) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	// Discover group directories on disk and ensure engines exist
	groupIDs, err := ts.ListGroupIDs()
	if err != nil {
		ts.logger.Warn("Failed to list group IDs from disk", "error", err)
	}
	for _, gid := range groupIDs {
		_ = ts.getOrCreateEngine(gid) // Ensure engine is loaded
	}

	// Now query all loaded group engines
	var allPoints []*DataPoint

	ts.enginesMu.RLock()
	engines := make(map[int]*Storage, len(ts.groupEngines))
	for k, v := range ts.groupEngines {
		engines[k] = v
	}
	ts.enginesMu.RUnlock()

	for groupID, engine := range engines {
		points, err := engine.Query(database, collection, deviceIDs, startTime, endTime, fields)
		if err != nil {
			ts.logger.Warn("Failed to query group",
				"group_id", groupID,
				"error", err)
			continue
		}
		allPoints = append(allPoints, points...)
	}

	return allPoints, nil
}

// QueryGroup queries a specific group's storage engine directly
func (ts *TieredStorage) QueryGroup(groupID int, database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	engine := ts.getOrCreateEngine(groupID)
	return engine.Query(database, collection, deviceIDs, startTime, endTime, fields)
}

// ============================================================================
// SetTimezone
// ============================================================================

// SetTimezone sets the timezone for all group engines
func (ts *TieredStorage) SetTimezone(tz *time.Location) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.config.Timezone = tz

	ts.enginesMu.RLock()
	defer ts.enginesMu.RUnlock()
	for _, engine := range ts.groupEngines {
		engine.SetTimezone(tz)
	}
}

// ============================================================================
// Group Metadata & Listing
// ============================================================================

// ListGroupIDs returns all group IDs that have data on disk
func (ts *TieredStorage) ListGroupIDs() ([]int, error) {
	entries, err := os.ReadDir(ts.config.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to list data dir: %w", err)
	}

	var groupIDs []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "group_") {
			continue
		}
		groupID, err := ParseGroupID(entry.Name())
		if err != nil {
			continue
		}
		groupIDs = append(groupIDs, groupID)
	}

	sort.Ints(groupIDs)
	return groupIDs, nil
}

// GetAllEngines returns all currently loaded storage engines (for compaction, etc.)
func (ts *TieredStorage) GetAllEngines() []*Storage {
	ts.enginesMu.RLock()
	defer ts.enginesMu.RUnlock()

	engines := make([]*Storage, 0, len(ts.groupEngines))
	for _, engine := range ts.groupEngines {
		engines = append(engines, engine)
	}
	return engines
}

// GroupDataSize returns the total data size in bytes for a specific group
func (ts *TieredStorage) GroupDataSize(groupID int) (int64, error) {
	groupDir := ts.GroupDir(groupID)
	var totalSize int64

	err := filepath.Walk(groupDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	return totalSize, nil
}

// ============================================================================
// Implements MainStorage interface
// ============================================================================

// TieredStorageAdapter wraps TieredStorage to implement MainStorage
type TieredStorageAdapter struct {
	ts *TieredStorage
}

// NewTieredStorageAdapter creates a new adapter
func NewTieredStorageAdapter(ts *TieredStorage) *TieredStorageAdapter {
	return &TieredStorageAdapter{ts: ts}
}

// WriteBatch implements MainStorage
func (a *TieredStorageAdapter) WriteBatch(entries []*DataPoint) error {
	return a.ts.WriteBatch(entries)
}

// Query implements MainStorage
func (a *TieredStorageAdapter) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	return a.ts.Query(database, collection, deviceIDs, startTime, endTime, fields)
}

// SetTimezone implements MainStorage
func (a *TieredStorageAdapter) SetTimezone(tz *time.Location) {
	a.ts.SetTimezone(tz)
}
