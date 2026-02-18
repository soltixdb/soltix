package aggregation

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
)

// =============================================================================
// V6 Columnar Aggregation Storage — Single-File Parts with Footer Index
// =============================================================================
//
// 2-level hierarchy:
//   - Device Groups (dg_XXXX/): group devices by count (max maxDevicesPerGroup)
//   - Parts (part_XXXX.bin): single file with ALL metrics for ALL fields + footer-based column index
//
// Directory structure: partDir/dg_XXXX/part_XXXX.bin
// Two-tier metadata:
//   - Global _metadata.idx in partDir: field list, DG manifests, device->DG map
//   - DG _metadata.idx in dg_XXXX/: part file names, per-part manifests
//
// Each part file contains:
//   Header (64 bytes) + column chunks (per device x field x metric) + Footer + FooterSize(4) + FooterOffset(8)
//
// Footer indexes every column by (DeviceIdx, FieldIdx, MetricType) so
// readers can seek directly to any metric/field/device combination.
// =============================================================================

// MetricType represents the aggregation metric type
type MetricType string

const (
	MetricSum     MetricType = "sum"
	MetricAvg     MetricType = "avg"
	MetricMin     MetricType = "min"
	MetricMax     MetricType = "max"
	MetricCount   MetricType = "count"
	MetricMinTime MetricType = "min_time"
	MetricMaxTime MetricType = "max_time"
)

// AllMetricTypes returns all metric types
func AllMetricTypes() []MetricType {
	return []MetricType{MetricSum, MetricAvg, MetricMin, MetricMax, MetricCount, MetricMinTime, MetricMaxTime}
}

// MetricTypeToIndex converts a MetricType to a uint8 index for binary encoding
func MetricTypeToIndex(m MetricType) uint8 {
	switch m {
	case MetricSum:
		return 0
	case MetricAvg:
		return 1
	case MetricMin:
		return 2
	case MetricMax:
		return 3
	case MetricCount:
		return 4
	case MetricMinTime:
		return 5
	case MetricMaxTime:
		return 6
	default:
		return 0
	}
}

// MetricTypeFromIndex converts a uint8 index back to MetricType
func MetricTypeFromIndex(idx uint8) MetricType {
	switch idx {
	case 0:
		return MetricSum
	case 1:
		return MetricAvg
	case 2:
		return MetricMin
	case 3:
		return MetricMax
	case 4:
		return MetricCount
	case 5:
		return MetricMinTime
	case 6:
		return MetricMaxTime
	default:
		return MetricSum
	}
}

// File format constants
const (
	AggPartMagic   uint32 = 0x41474750 // "AGGP"
	AggPartVersion uint32 = 6          // V6 - single-file parts with footer index
	MetadataFile          = "_metadata.idx"

	// Header size for V6 part files
	V6AggHeaderSize = 64

	// Default DG threshold
	DefaultMaxDevicesPerGroup = 50
)

// =============================================================================
// V6 Footer structures
// =============================================================================

// V6AggColumnEntry describes a single column chunk in the footer index.
// Each entry identifies a (device, field, metric) triple and its byte range.
type V6AggColumnEntry struct {
	DeviceIdx  uint32 // Index into the footer's DeviceNames
	FieldIdx   uint32 // Index into the footer's FieldNames (0 = _time)
	MetricIdx  uint8  // 0=sum, 1=avg, 2=min, 3=max, 4=count (0xFF for _time)
	Offset     int64  // Byte offset in the file
	Size       uint32 // Compressed size in bytes
	RowCount   uint32 // Number of rows in this chunk
	ColumnType uint8  // 0=int64, 1=float64
}

// V6AggPartFooter is the in-memory representation of a V6 aggregation part footer.
type V6AggPartFooter struct {
	DeviceNames    []string           // Ordered device list
	FieldNames     []string           // Field dictionary: index 0 = "_time", 1..N = sorted field names
	Columns        []V6AggColumnEntry // All column entries
	RowCountPerDev []uint32           // Row count per device
}

// =============================================================================
// Metadata structures (two-tier: global + per-DG)
// =============================================================================

// PartitionMetadata is the GLOBAL metadata file in partDir/_metadata.idx.
// V6: No column group definitions - all fields live in single part files.
type PartitionMetadata struct {
	Version   int       `json:"version"`
	Level     string    `json:"level"` // "1h", "1d", "1M", "1y"
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Data statistics
	RowCount    int64 `json:"row_count"`
	DeviceCount int   `json:"device_count"`
	MinTime     int64 `json:"min_time"`
	MaxTime     int64 `json:"max_time"`

	// Schema
	Fields []FieldMeta `json:"fields"`

	// Device groups (lightweight manifests)
	DeviceGroups   []DeviceGroupInfo `json:"device_groups"`
	DeviceGroupMap map[string]int    `json:"device_group_map"` // deviceID -> DG index
}

// DGMetadata is the per-DG metadata file in dg_XXXX/_metadata.idx.
// V6: Contains part info (no CG sub-directories).
type DGMetadata struct {
	Version     int        `json:"version"`
	GroupIndex  int        `json:"group_index"`
	DirName     string     `json:"dir_name"`
	DeviceNames []string   `json:"device_names"`
	Parts       []PartInfo `json:"parts"`
	RowCount    int64      `json:"row_count"`
	MinTime     int64      `json:"min_time"`
	MaxTime     int64      `json:"max_time"`
}

// DeviceGroupInfo describes a device group
type DeviceGroupInfo struct {
	DirName     string   `json:"dir_name"` // e.g. "dg_0000"
	GroupIndex  int      `json:"group_index"`
	DeviceNames []string `json:"device_names"`
}

// FieldMeta describes a field in the aggregation
type FieldMeta struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "float64"
	Position int    `json:"position"`
}

// PartInfo contains information about a single V6 part file
type PartInfo struct {
	PartNum  int    `json:"part_num"`
	FileName string `json:"file_name"`
	RowCount int64  `json:"row_count"`
	MinTime  int64  `json:"min_time"`
	MaxTime  int64  `json:"max_time"`
	Size     int64  `json:"size"`
	Checksum uint32 `json:"checksum"`
}

// =============================================================================
// Storage implementation
// =============================================================================

// StorageConfig contains configuration
type StorageConfig struct {
	MaxRowsPerPart     int   // Max rows per part file (default: 10000)
	MaxPartSize        int64 // Max size per part file in bytes (default: 64MB)
	MaxDevicesPerGroup int   // Max devices per device group (default: 50)
}

// DefaultStorageConfig returns default configuration
func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		MaxRowsPerPart:     10000,
		MaxPartSize:        64 * 1024 * 1024, // 64MB
		MaxDevicesPerGroup: DefaultMaxDevicesPerGroup,
	}
}

// Storage manages V6 columnar aggregation data files
type Storage struct {
	baseDir    string
	config     StorageConfig
	compressor compression.Compressor
	logger     *logging.Logger
	timezone   *time.Location

	// Encoders from compression package
	deltaEncoder      *compression.DeltaEncoder
	dictionaryEncoder *compression.DictionaryEncoder
	gorillaEncoder    *compression.GorillaEncoder

	// Cache for metadata
	metaCache   map[string]*PartitionMetadata
	metaCacheMu sync.RWMutex
}

// NewStorage creates a new columnar aggregation storage
func NewStorage(baseDir string, logger *logging.Logger) *Storage {
	return NewStorageWithConfig(baseDir, logger, DefaultStorageConfig())
}

// NewStorageWithConfig creates a new columnar storage with config
func NewStorageWithConfig(baseDir string, logger *logging.Logger, config StorageConfig) *Storage {
	return &Storage{
		baseDir:           baseDir,
		config:            config,
		compressor:        compression.NewSnappyCompressor(),
		deltaEncoder:      compression.NewDeltaEncoder(),
		dictionaryEncoder: compression.NewDictionaryEncoder(),
		gorillaEncoder:    compression.NewGorillaEncoder(),
		logger:            logger,
		timezone:          time.UTC,
		metaCache:         make(map[string]*PartitionMetadata),
	}
}

// SetTimezone sets the timezone for date-based file organization
func (s *Storage) SetTimezone(tz *time.Location) {
	if tz != nil {
		s.timezone = tz
	}
}

// GetTimezone returns the configured timezone
func (s *Storage) GetTimezone() *time.Location {
	return s.timezone
}

// =============================================================================
// Directory structure helpers
// =============================================================================

// getPartitionDir returns the directory path for a partition
func (s *Storage) getPartitionDir(level AggregationLevel, database, collection string, t time.Time) string {
	localTime := t.In(s.timezone)

	switch level {
	case AggregationHourly:
		return filepath.Join(s.baseDir, "agg_1h", database, collection,
			localTime.Format("2006"), localTime.Format("01"), localTime.Format("02"))
	case AggregationDaily:
		return filepath.Join(s.baseDir, "agg_1d", database, collection,
			localTime.Format("2006"), localTime.Format("01"))
	case AggregationMonthly:
		return filepath.Join(s.baseDir, "agg_1M", database, collection,
			localTime.Format("2006"))
	case AggregationYearly:
		return filepath.Join(s.baseDir, "agg_1y", database, collection)
	default:
		return filepath.Join(s.baseDir, "agg_1h", database, collection,
			localTime.Format("2006"), localTime.Format("01"), localTime.Format("02"))
	}
}

// getPartFileName returns the V6 part file name
func (s *Storage) getPartFileName(partNum int) string {
	return fmt.Sprintf("part_%04d.bin", partNum)
}

// =============================================================================
// Device Group helpers
// =============================================================================

// getDeviceGroupDir returns the directory name for a device group index
func getDeviceGroupDir(index int) string {
	return fmt.Sprintf("dg_%04d", index)
}

// assignDeviceGroups assigns devices to groups of maxDevicesPerGroup.
// Preserves existing assignments and adds new devices to existing or new groups.
// Fixed: tracks effective group size during assignment loop.
func (s *Storage) assignDeviceGroups(meta *PartitionMetadata, deviceIDs []string) {
	maxDevices := s.config.MaxDevicesPerGroup
	if maxDevices <= 0 {
		maxDevices = DefaultMaxDevicesPerGroup
	}

	if meta.DeviceGroupMap == nil {
		meta.DeviceGroupMap = make(map[string]int)
	}

	// Collect new devices not yet assigned
	var newDevices []string
	for _, id := range deviceIDs {
		if _, exists := meta.DeviceGroupMap[id]; !exists {
			newDevices = append(newDevices, id)
		}
	}
	sort.Strings(newDevices)

	if len(newDevices) == 0 {
		return
	}

	// Track effective group sizes
	groupSizes := make(map[int]int, len(meta.DeviceGroups))
	for i := range meta.DeviceGroups {
		groupSizes[i] = len(meta.DeviceGroups[i].DeviceNames)
	}

	for _, deviceID := range newDevices {
		assigned := false
		for i := range meta.DeviceGroups {
			if groupSizes[i] < maxDevices {
				meta.DeviceGroups[i].DeviceNames = append(meta.DeviceGroups[i].DeviceNames, deviceID)
				meta.DeviceGroupMap[deviceID] = i
				groupSizes[i]++
				assigned = true
				break
			}
		}
		if !assigned {
			dgIdx := len(meta.DeviceGroups)
			dg := DeviceGroupInfo{
				DirName:     getDeviceGroupDir(dgIdx),
				GroupIndex:  dgIdx,
				DeviceNames: []string{deviceID},
			}
			meta.DeviceGroups = append(meta.DeviceGroups, dg)
			meta.DeviceGroupMap[deviceID] = dgIdx
			groupSizes[dgIdx] = 1
		}
	}
}

// collectFieldNames collects all unique field names from aggregated points
func collectFieldNames(points []*AggregatedPoint) []string {
	fieldSet := make(map[string]struct{})
	for _, p := range points {
		for name := range p.Fields {
			fieldSet[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(fieldSet))
	for name := range fieldSet {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// collectDeviceIDs collects all unique device IDs from aggregated points
func collectDeviceIDs(points []*AggregatedPoint) []string {
	deviceSet := make(map[string]struct{})
	for _, p := range points {
		deviceSet[p.DeviceID] = struct{}{}
	}
	ids := make([]string, 0, len(deviceSet))
	for id := range deviceSet {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// =============================================================================
// Metadata operations (JSON-based, two-tier)
// =============================================================================

// readMetadata reads partition metadata from disk
func (s *Storage) readMetadata(partDir string) (*PartitionMetadata, error) {
	s.metaCacheMu.RLock()
	if meta, ok := s.metaCache[partDir]; ok {
		s.metaCacheMu.RUnlock()
		return meta, nil
	}
	s.metaCacheMu.RUnlock()

	metaPath := filepath.Join(partDir, MetadataFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta PartitionMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	s.metaCacheMu.Lock()
	s.metaCache[partDir] = &meta
	s.metaCacheMu.Unlock()

	return &meta, nil
}

// writeMetadata writes partition metadata to disk
func (s *Storage) writeMetadata(partDir string, meta *PartitionMetadata) error {
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	meta.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaPath := filepath.Join(partDir, MetadataFile)
	tmpPath := metaPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	if err := os.Rename(tmpPath, metaPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename metadata: %w", err)
	}

	s.metaCacheMu.Lock()
	s.metaCache[partDir] = meta
	s.metaCacheMu.Unlock()

	return nil
}

// invalidateMetadataCache removes a partition from cache
func (s *Storage) invalidateMetadataCache(partDir string) {
	s.metaCacheMu.Lock()
	delete(s.metaCache, partDir)
	s.metaCacheMu.Unlock()
}

// =============================================================================
// Per-DG metadata operations
// =============================================================================

// readDGMetadata reads per-DG metadata from dg_XXXX/_metadata.idx
func (s *Storage) readDGMetadata(dgDir string) (*DGMetadata, error) {
	metaPath := filepath.Join(dgDir, MetadataFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read DG metadata: %w", err)
	}

	var meta DGMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DG metadata: %w", err)
	}

	return &meta, nil
}

// writeDGMetadata writes per-DG metadata to dg_XXXX/_metadata.idx
func (s *Storage) writeDGMetadata(dgDir string, meta *DGMetadata) error {
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		return fmt.Errorf("failed to create DG directory: %w", err)
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal DG metadata: %w", err)
	}

	metaPath := filepath.Join(dgDir, MetadataFile)
	tmpPath := metaPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write DG metadata: %w", err)
	}

	if err := os.Rename(tmpPath, metaPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename DG metadata: %w", err)
	}

	return nil
}

// =============================================================================
// Columnar data structures for encoding/decoding
// =============================================================================

// ColumnarData represents columnar data for ALL metrics in a single structure.
// V6: all 5 metric types are stored together in a single part file.
type ColumnarData struct {
	// Shared columns (per row)
	Timestamps []int64  // Delta encoded
	DeviceIDs  []string // Dictionary encoded

	// Per-field, per-metric: field_name -> values
	// FloatColumns keys: "sum\x00fieldName", "avg\x00fieldName", etc.
	// IntColumns keys: "count\x00fieldName", "min_time\x00fieldName", "max_time\x00fieldName"
	FloatColumns map[string][]float64
	IntColumns   map[string][]int64
}

// V6 column key helpers - encode metric+field into a single map key

func v6FloatKey(metric MetricType, fieldName string) string {
	return string(metric) + "\x00" + fieldName
}

func v6IntMetricKey(metric MetricType, fieldName string) string {
	return string(metric) + "\x00" + fieldName
}

func v6IntKey(fieldName string) string {
	return v6IntMetricKey(MetricCount, fieldName)
}

func v6ParseFloatKey(key string) (MetricType, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return MetricType(key[:i]), key[i+1:]
		}
	}
	return "", key
}

func v6ParseIntKey(key string) (MetricType, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			metric := MetricType(key[:i])
			if metric == "" {
				metric = MetricCount
			}
			return metric, key[i+1:]
		}
	}
	return MetricCount, key
}

// NewColumnarData creates a new columnar data container
func NewColumnarData(capacity int) *ColumnarData {
	return &ColumnarData{
		Timestamps:   make([]int64, 0, capacity),
		DeviceIDs:    make([]string, 0, capacity),
		FloatColumns: make(map[string][]float64),
		IntColumns:   make(map[string][]int64),
	}
}

// AddRowAllMetrics adds a row with ALL metric types at once (V6 style).
func (cd *ColumnarData) AddRowAllMetrics(timestamp int64, deviceID string, fields map[string]*AggregatedField) {
	cd.Timestamps = append(cd.Timestamps, timestamp)
	cd.DeviceIDs = append(cd.DeviceIDs, deviceID)

	for fieldName, field := range fields {
		sumKey := v6FloatKey(MetricSum, fieldName)
		cd.FloatColumns[sumKey] = append(cd.FloatColumns[sumKey], field.Sum)

		avgKey := v6FloatKey(MetricAvg, fieldName)
		cd.FloatColumns[avgKey] = append(cd.FloatColumns[avgKey], field.Avg)

		minKey := v6FloatKey(MetricMin, fieldName)
		cd.FloatColumns[minKey] = append(cd.FloatColumns[minKey], field.Min)

		maxKey := v6FloatKey(MetricMax, fieldName)
		cd.FloatColumns[maxKey] = append(cd.FloatColumns[maxKey], field.Max)

		countKey := v6IntKey(fieldName)
		cd.IntColumns[countKey] = append(cd.IntColumns[countKey], field.Count)

		minTimeKey := v6IntMetricKey(MetricMinTime, fieldName)
		cd.IntColumns[minTimeKey] = append(cd.IntColumns[minTimeKey], field.MinTime)

		maxTimeKey := v6IntMetricKey(MetricMaxTime, fieldName)
		cd.IntColumns[maxTimeKey] = append(cd.IntColumns[maxTimeKey], field.MaxTime)
	}
}

// RowCount returns the number of rows
func (cd *ColumnarData) RowCount() int {
	return len(cd.Timestamps)
}

// =============================================================================
// V6 Part file write
// =============================================================================

// pendingPartWrite holds information about a pending part file write
type pendingPartWrite struct {
	partNum  int
	tmpPath  string
	filePath string
	partInfo *PartInfo
	isAppend bool
}

// writeV6PartFile writes a single V6 aggregation part file containing ALL metrics
// for ALL fields with a footer-based column index.
//
// File layout:
//
//	[Header 64B] [column chunks...] [Footer] [FooterSize 4B] [FooterOffset 8B]
//
// Column chunks are organized per device -> per field -> per metric.
// The _time column (delta-encoded int64) is written once per device.
// Float metrics (sum/avg/min/max) use Gorilla encoding.
// Count metric uses delta encoding (int64).
func (s *Storage) writeV6PartFile(partDir string, partNum int, level AggregationLevel, data *ColumnarData) (*pendingPartWrite, error) {
	if data.RowCount() == 0 {
		return nil, nil
	}

	fileName := s.getPartFileName(partNum)
	filePath := filepath.Join(partDir, fileName)
	tmpPath := filePath + ".tmp"

	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create part directory: %w", err)
	}

	file, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create V6 part file: %w", err)
	}

	var writeErr error
	defer func() {
		_ = file.Close()
		if writeErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	// Reserve header space
	if writeErr = file.Truncate(V6AggHeaderSize); writeErr != nil {
		return nil, writeErr
	}
	currentOffset := int64(V6AggHeaderSize)

	// Collect unique devices in order and group rows by device
	deviceOrderMap := make(map[string]int)
	deviceRows := make(map[string][]int)
	var deviceNames []string
	for i, devID := range data.DeviceIDs {
		if _, exists := deviceOrderMap[devID]; !exists {
			deviceOrderMap[devID] = len(deviceNames)
			deviceNames = append(deviceNames, devID)
		}
		deviceRows[devID] = append(deviceRows[devID], i)
	}

	// Collect field names from columns
	fieldSet := make(map[string]struct{})
	for key := range data.FloatColumns {
		_, fieldName := v6ParseFloatKey(key)
		if fieldName != "" {
			fieldSet[fieldName] = struct{}{}
		}
	}
	for key := range data.IntColumns {
		_, fieldName := v6ParseIntKey(key)
		if fieldName != "" {
			fieldSet[fieldName] = struct{}{}
		}
	}
	sortedFields := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		sortedFields = append(sortedFields, f)
	}
	sort.Strings(sortedFields)

	// Build field dictionary: [_time, field0, field1, ...]
	dictFields := make([]string, 0, len(sortedFields)+1)
	dictFields = append(dictFields, "_time")
	dictFields = append(dictFields, sortedFields...)

	var columns []V6AggColumnEntry
	rowCountPerDev := make([]uint32, len(deviceNames))
	totalRows := uint32(0)
	var globalMinTime, globalMaxTime int64

	// Write column chunks per device
	for devIdx, devID := range deviceNames {
		rows := deviceRows[devID]
		rowCount := uint32(len(rows))
		totalRows += rowCount
		rowCountPerDev[devIdx] = rowCount

		// Extract timestamps for this device
		timestamps := make([]int64, len(rows))
		for i, rowIdx := range rows {
			timestamps[i] = data.Timestamps[rowIdx]
		}

		// Track min/max time
		for _, ts := range timestamps {
			if globalMinTime == 0 || ts < globalMinTime {
				globalMinTime = ts
			}
			if ts > globalMaxTime {
				globalMaxTime = ts
			}
		}

		// Write _time column (delta-encoded int64)
		tsIfaces := make([]interface{}, len(timestamps))
		for i, ts := range timestamps {
			tsIfaces[i] = ts
		}
		tsData, encErr := s.deltaEncoder.Encode(tsIfaces)
		if encErr != nil {
			writeErr = encErr
			return nil, fmt.Errorf("failed to encode timestamps: %w", encErr)
		}
		compressed, compErr := s.compressor.Compress(tsData)
		if compErr != nil {
			writeErr = compErr
			return nil, fmt.Errorf("failed to compress timestamps: %w", compErr)
		}
		if _, wErr := file.WriteAt(compressed, currentOffset); wErr != nil {
			writeErr = wErr
			return nil, fmt.Errorf("failed to write timestamps: %w", wErr)
		}
		columns = append(columns, V6AggColumnEntry{
			DeviceIdx:  uint32(devIdx),
			FieldIdx:   0, // _time
			MetricIdx:  0xFF,
			Offset:     currentOffset,
			Size:       uint32(len(compressed)),
			RowCount:   rowCount,
			ColumnType: 0, // int64
		})
		currentOffset += int64(len(compressed))

		// Write metric columns per field
		for fIdx, fieldName := range sortedFields {
			fieldIdx := uint32(fIdx + 1) // +1 because 0 = _time

			// Float metrics: sum, avg, min, max
			for _, metric := range []MetricType{MetricSum, MetricAvg, MetricMin, MetricMax} {
				key := v6FloatKey(metric, fieldName)
				allValues := data.FloatColumns[key]
				if len(allValues) == 0 {
					continue
				}

				values := make([]float64, len(rows))
				for i, rowIdx := range rows {
					if rowIdx < len(allValues) {
						values[i] = allValues[rowIdx]
					}
				}

				ifaces := make([]interface{}, len(values))
				for i, v := range values {
					ifaces[i] = v
				}
				encoded, encErr := s.gorillaEncoder.Encode(ifaces)
				if encErr != nil {
					writeErr = encErr
					return nil, fmt.Errorf("failed to encode float column %s/%s: %w", metric, fieldName, encErr)
				}
				comp, compErr := s.compressor.Compress(encoded)
				if compErr != nil {
					writeErr = compErr
					return nil, fmt.Errorf("failed to compress float column: %w", compErr)
				}
				if _, wErr := file.WriteAt(comp, currentOffset); wErr != nil {
					writeErr = wErr
					return nil, wErr
				}
				columns = append(columns, V6AggColumnEntry{
					DeviceIdx:  uint32(devIdx),
					FieldIdx:   fieldIdx,
					MetricIdx:  MetricTypeToIndex(metric),
					Offset:     currentOffset,
					Size:       uint32(len(comp)),
					RowCount:   rowCount,
					ColumnType: 1, // float64
				})
				currentOffset += int64(len(comp))
			}

			// Int metrics: count, min_time, max_time
			for _, metric := range []MetricType{MetricCount, MetricMinTime, MetricMaxTime} {
				key := v6IntMetricKey(metric, fieldName)
				allValues := data.IntColumns[key]
				if len(allValues) == 0 {
					continue
				}

				values := make([]int64, len(rows))
				for i, rowIdx := range rows {
					if rowIdx < len(allValues) {
						values[i] = allValues[rowIdx]
					}
				}

				ifaces := make([]interface{}, len(values))
				for i, v := range values {
					ifaces[i] = v
				}
				encoded, encErr := s.deltaEncoder.Encode(ifaces)
				if encErr != nil {
					writeErr = encErr
					return nil, fmt.Errorf("failed to encode int column %s/%s: %w", metric, fieldName, encErr)
				}
				comp, compErr := s.compressor.Compress(encoded)
				if compErr != nil {
					writeErr = compErr
					return nil, fmt.Errorf("failed to compress int column: %w", compErr)
				}
				if _, wErr := file.WriteAt(comp, currentOffset); wErr != nil {
					writeErr = wErr
					return nil, wErr
				}
				columns = append(columns, V6AggColumnEntry{
					DeviceIdx:  uint32(devIdx),
					FieldIdx:   fieldIdx,
					MetricIdx:  MetricTypeToIndex(metric),
					Offset:     currentOffset,
					Size:       uint32(len(comp)),
					RowCount:   rowCount,
					ColumnType: 0, // int64
				})
				currentOffset += int64(len(comp))
			}
		}
	}

	// Build and write footer
	footer := &V6AggPartFooter{
		DeviceNames:    deviceNames,
		FieldNames:     dictFields,
		Columns:        columns,
		RowCountPerDev: rowCountPerDev,
	}

	footerData := s.encodeV6AggFooter(footer)
	footerOffset := currentOffset

	if _, wErr := file.WriteAt(footerData, footerOffset); wErr != nil {
		writeErr = wErr
		return nil, fmt.Errorf("failed to write footer: %w", wErr)
	}
	currentOffset += int64(len(footerData))

	// Write footer size (4 bytes)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(len(footerData)))
	if _, wErr := file.WriteAt(sizeBuf, currentOffset); wErr != nil {
		writeErr = wErr
		return nil, wErr
	}
	currentOffset += 4

	// Write footer offset (last 8 bytes)
	offsetBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBuf, uint64(footerOffset))
	if _, wErr := file.WriteAt(offsetBuf, currentOffset); wErr != nil {
		writeErr = wErr
		return nil, wErr
	}

	// Compute checksum of footer data
	dataChecksum := crc32.ChecksumIEEE(footerData)

	// Write header
	headerBuf := make([]byte, V6AggHeaderSize)
	binary.LittleEndian.PutUint32(headerBuf[0:], AggPartMagic)
	binary.LittleEndian.PutUint32(headerBuf[4:], AggPartVersion)
	copy(headerBuf[8:12], []byte(level))
	binary.LittleEndian.PutUint32(headerBuf[12:], uint32(s.compressor.Algorithm()))
	binary.LittleEndian.PutUint32(headerBuf[16:], totalRows)
	binary.LittleEndian.PutUint32(headerBuf[20:], uint32(len(sortedFields)))
	binary.LittleEndian.PutUint32(headerBuf[24:], uint32(len(deviceNames)))
	binary.LittleEndian.PutUint32(headerBuf[28:], uint32(len(columns)))
	binary.LittleEndian.PutUint64(headerBuf[32:], uint64(globalMinTime))
	binary.LittleEndian.PutUint64(headerBuf[40:], uint64(globalMaxTime))
	binary.LittleEndian.PutUint32(headerBuf[48:], dataChecksum)

	if _, wErr := file.WriteAt(headerBuf, 0); wErr != nil {
		writeErr = wErr
		return nil, wErr
	}

	if syncErr := file.Sync(); syncErr != nil {
		writeErr = syncErr
		return nil, syncErr
	}
	_ = file.Close()

	// Get file size
	stat, _ := os.Stat(tmpPath)
	fileSize := int64(0)
	if stat != nil {
		fileSize = stat.Size()
	}

	return &pendingPartWrite{
		partNum:  partNum,
		tmpPath:  tmpPath,
		filePath: filePath,
		partInfo: &PartInfo{
			PartNum:  partNum,
			FileName: fileName,
			RowCount: int64(totalRows),
			MinTime:  globalMinTime,
			MaxTime:  globalMaxTime,
			Size:     fileSize,
			Checksum: dataChecksum,
		},
	}, nil
}

// commitPendingWrites atomically renames all .tmp files to final names
func (s *Storage) commitPendingWrites(pending []*pendingPartWrite) error {
	for _, p := range pending {
		if p == nil {
			continue
		}
		if err := os.Rename(p.tmpPath, p.filePath); err != nil {
			s.cleanupPendingWrites(pending)
			return fmt.Errorf("failed to rename %s: %w", p.tmpPath, err)
		}
	}
	return nil
}

// cleanupPendingWrites removes any .tmp files from failed writes
func (s *Storage) cleanupPendingWrites(pending []*pendingPartWrite) {
	for _, p := range pending {
		if p == nil {
			continue
		}
		_ = os.Remove(p.tmpPath)
	}
}

// =============================================================================
// V6 Part file read
// =============================================================================

// readV6PartFooter reads the footer from a V6 aggregation part file
func (s *Storage) readV6PartFooter(filePath string) (*V6AggPartFooter, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	if fileSize < V6AggHeaderSize+12 {
		return nil, fmt.Errorf("V6 agg part file too small: %d bytes", fileSize)
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

	if footerOffset < V6AggHeaderSize || footerOffset+int64(footerSize) > fileSize-12 {
		return nil, fmt.Errorf("invalid footer offset/size: offset=%d size=%d fileSize=%d", footerOffset, footerSize, fileSize)
	}

	// Read footer data
	footerData := make([]byte, footerSize)
	if _, err := file.ReadAt(footerData, footerOffset); err != nil {
		return nil, fmt.Errorf("failed to read footer data: %w", err)
	}

	return s.decodeV6AggFooter(footerData)
}

// readV6PartFileData reads a V6 aggregation part file and returns ColumnarData.
func (s *Storage) readV6PartFileData(
	filePath string,
	footer *V6AggPartFooter,
	deviceFilter map[string]struct{},
	fieldFilter map[string]struct{},
	metricFilter map[MetricType]struct{},
	startNano, endNano int64,
) (*ColumnarData, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	// Build device filter index set
	var wantDevice map[uint32]bool
	if deviceFilter != nil {
		wantDevice = make(map[uint32]bool)
		for i, name := range footer.DeviceNames {
			if _, ok := deviceFilter[name]; ok {
				wantDevice[uint32(i)] = true
			}
		}
		if len(wantDevice) == 0 {
			return NewColumnarData(0), nil
		}
	}

	// Build field filter index set
	var wantField map[uint32]bool
	if fieldFilter != nil {
		wantField = make(map[uint32]bool)
		wantField[0] = true // Always include _time
		for i, name := range footer.FieldNames {
			if i == 0 {
				continue
			}
			if _, ok := fieldFilter[name]; ok {
				wantField[uint32(i)] = true
			}
		}
	}

	// Build metric filter set
	var wantMetric map[uint8]bool
	if metricFilter != nil {
		wantMetric = make(map[uint8]bool)
		for m := range metricFilter {
			wantMetric[MetricTypeToIndex(m)] = true
		}
	}

	// Read _time columns first for time filtering
	type deviceTimeData struct {
		timestamps []int64
		rows       []int // valid row indices after time filtering
	}
	timeByDevice := make(map[uint32]*deviceTimeData)

	for _, col := range footer.Columns {
		if col.FieldIdx != 0 {
			continue
		}
		if wantDevice != nil && !wantDevice[col.DeviceIdx] {
			continue
		}

		buf := make([]byte, col.Size)
		if _, rErr := file.ReadAt(buf, col.Offset); rErr != nil {
			return nil, fmt.Errorf("failed to read _time column: %w", rErr)
		}
		decompressed, dErr := s.compressor.Decompress(buf)
		if dErr != nil {
			return nil, fmt.Errorf("failed to decompress _time: %w", dErr)
		}
		tsValues, tsNulls, decErr := s.deltaEncoder.DecodeInt64(decompressed, int(col.RowCount))
		if decErr != nil {
			return nil, fmt.Errorf("failed to decode _time: %w", decErr)
		}

		timestamps := make([]int64, 0, len(tsValues))
		validRows := make([]int, 0, len(tsValues))
		for i, ts := range tsValues {
			if tsNulls != nil && tsNulls[i] {
				continue
			}
			if ts >= startNano && ts < endNano {
				timestamps = append(timestamps, ts)
				validRows = append(validRows, i)
			}
		}
		if len(timestamps) > 0 {
			timeByDevice[col.DeviceIdx] = &deviceTimeData{
				timestamps: timestamps,
				rows:       validRows,
			}
		}
	}

	if len(timeByDevice) == 0 {
		return NewColumnarData(0), nil
	}

	// Count total rows
	totalRows := 0
	for _, td := range timeByDevice {
		totalRows += len(td.timestamps)
	}

	result := NewColumnarData(totalRows)

	// Process devices in order
	devIndices := make([]uint32, 0, len(timeByDevice))
	for idx := range timeByDevice {
		devIndices = append(devIndices, idx)
	}
	sort.Slice(devIndices, func(i, j int) bool { return devIndices[i] < devIndices[j] })

	devResultOffset := make(map[uint32]int)
	for _, devIdx := range devIndices {
		td := timeByDevice[devIdx]
		devResultOffset[devIdx] = len(result.Timestamps)
		result.Timestamps = append(result.Timestamps, td.timestamps...)
		devName := footer.DeviceNames[devIdx]
		for range td.timestamps {
			result.DeviceIDs = append(result.DeviceIDs, devName)
		}
	}

	// Read metric columns
	for _, col := range footer.Columns {
		if col.FieldIdx == 0 {
			continue
		}
		if wantDevice != nil && !wantDevice[col.DeviceIdx] {
			continue
		}
		if wantField != nil && !wantField[col.FieldIdx] {
			continue
		}
		if col.MetricIdx == 0xFF {
			continue
		}
		if wantMetric != nil && !wantMetric[col.MetricIdx] {
			continue
		}

		td, ok := timeByDevice[col.DeviceIdx]
		if !ok {
			continue
		}

		fieldName := footer.FieldNames[col.FieldIdx]
		metric := MetricTypeFromIndex(col.MetricIdx)

		buf := make([]byte, col.Size)
		if _, rErr := file.ReadAt(buf, col.Offset); rErr != nil {
			return nil, fmt.Errorf("failed to read column: %w", rErr)
		}
		decompressed, dErr := s.compressor.Decompress(buf)
		if dErr != nil {
			return nil, fmt.Errorf("failed to decompress column: %w", dErr)
		}

		resultOffset := devResultOffset[col.DeviceIdx]

		if col.ColumnType == 1 {
			// float64 — typed decode avoids ~1000 interface{} boxing allocs
			floatVals, floatNulls, decErr := s.gorillaEncoder.DecodeFloat64(decompressed, int(col.RowCount))
			if decErr != nil {
				return nil, fmt.Errorf("failed to decode float column: %w", decErr)
			}

			key := v6FloatKey(metric, fieldName)
			if _, exists := result.FloatColumns[key]; !exists {
				result.FloatColumns[key] = make([]float64, totalRows)
			}
			vals := result.FloatColumns[key]

			for i, origIdx := range td.rows {
				if origIdx < len(floatVals) && (floatNulls == nil || !floatNulls[origIdx]) {
					vals[resultOffset+i] = floatVals[origIdx]
				}
			}
			result.FloatColumns[key] = vals
		} else {
			// int64 — typed decode avoids ~1000 interface{} boxing allocs
			intVals, intNulls, decErr := s.deltaEncoder.DecodeInt64(decompressed, int(col.RowCount))
			if decErr != nil {
				return nil, fmt.Errorf("failed to decode int column: %w", decErr)
			}

			key := v6IntMetricKey(metric, fieldName)
			if _, exists := result.IntColumns[key]; !exists {
				result.IntColumns[key] = make([]int64, totalRows)
			}
			vals := result.IntColumns[key]

			for i, origIdx := range td.rows {
				if origIdx < len(intVals) && (intNulls == nil || !intNulls[origIdx]) {
					vals[resultOffset+i] = intVals[origIdx]
				}
			}
			result.IntColumns[key] = vals
		}
	}

	return result, nil
}

// =============================================================================
// V6 Footer encode/decode
// =============================================================================

// encodeV6AggFooter serializes the footer to binary
func (s *Storage) encodeV6AggFooter(footer *V6AggPartFooter) []byte {
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

	// Column entries (26 bytes each)
	binary.LittleEndian.PutUint32(intBuf, uint32(len(footer.Columns)))
	buf = append(buf, intBuf[:4]...)
	for _, col := range footer.Columns {
		binary.LittleEndian.PutUint32(intBuf, col.DeviceIdx)
		buf = append(buf, intBuf[:4]...)
		binary.LittleEndian.PutUint32(intBuf, col.FieldIdx)
		buf = append(buf, intBuf[:4]...)
		buf = append(buf, col.MetricIdx)
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

// decodeV6AggFooter deserializes footer from binary
func (s *Storage) decodeV6AggFooter(data []byte) (*V6AggPartFooter, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("V6 agg footer too short")
	}

	offset := 0
	footer := &V6AggPartFooter{}

	// Device names
	deviceCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	footer.DeviceNames = make([]string, deviceCount)
	for i := 0; i < deviceCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("footer truncated at device name %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("footer truncated at device name %d data", i)
		}
		footer.DeviceNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Row count per device
	footer.RowCountPerDev = make([]uint32, deviceCount)
	for i := 0; i < deviceCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("footer truncated at row count %d", i)
		}
		footer.RowCountPerDev[i] = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}

	// Field dictionary
	if offset+4 > len(data) {
		return nil, fmt.Errorf("footer truncated at field count")
	}
	fieldCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	footer.FieldNames = make([]string, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("footer truncated at field name %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("footer truncated at field name %d data", i)
		}
		footer.FieldNames[i] = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	// Column entries (26 bytes each)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("footer truncated at column count")
	}
	colCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	footer.Columns = make([]V6AggColumnEntry, colCount)
	for i := 0; i < colCount; i++ {
		if offset+26 > len(data) {
			return nil, fmt.Errorf("footer truncated at column entry %d", i)
		}
		footer.Columns[i] = V6AggColumnEntry{
			DeviceIdx:  binary.LittleEndian.Uint32(data[offset:]),
			FieldIdx:   binary.LittleEndian.Uint32(data[offset+4:]),
			MetricIdx:  data[offset+8],
			Offset:     int64(binary.LittleEndian.Uint64(data[offset+9:])),
			Size:       binary.LittleEndian.Uint32(data[offset+17:]),
			RowCount:   binary.LittleEndian.Uint32(data[offset+21:]),
			ColumnType: data[offset+25],
		}
		offset += 26
	}

	return footer, nil
}

// =============================================================================
// Merge helpers for append operations (V6)
// =============================================================================

// mergeColumnarDataForWrite merges existing columnar data with new data for append operations
func (s *Storage) mergeColumnarDataForWrite(existing, newData *ColumnarData) *ColumnarData {
	totalRows := existing.RowCount() + newData.RowCount()
	merged := NewColumnarData(totalRows)

	// Merge timestamps
	merged.Timestamps = append(merged.Timestamps, existing.Timestamps...)
	merged.Timestamps = append(merged.Timestamps, newData.Timestamps...)

	// Merge device IDs
	merged.DeviceIDs = append(merged.DeviceIDs, existing.DeviceIDs...)
	merged.DeviceIDs = append(merged.DeviceIDs, newData.DeviceIDs...)

	// Merge float columns
	allFloatFields := make(map[string]struct{})
	for field := range existing.FloatColumns {
		allFloatFields[field] = struct{}{}
	}
	for field := range newData.FloatColumns {
		allFloatFields[field] = struct{}{}
	}
	for field := range allFloatFields {
		existingVals := existing.FloatColumns[field]
		newVals := newData.FloatColumns[field]
		if len(existingVals) == 0 {
			existingVals = make([]float64, existing.RowCount())
		}
		if len(newVals) == 0 {
			newVals = make([]float64, newData.RowCount())
		}
		merged.FloatColumns[field] = append(append([]float64{}, existingVals...), newVals...)
	}

	// Merge int columns
	allIntFields := make(map[string]struct{})
	for field := range existing.IntColumns {
		allIntFields[field] = struct{}{}
	}
	for field := range newData.IntColumns {
		allIntFields[field] = struct{}{}
	}
	for field := range allIntFields {
		existingVals := existing.IntColumns[field]
		newVals := newData.IntColumns[field]
		if len(existingVals) == 0 {
			existingVals = make([]int64, existing.RowCount())
		}
		if len(newVals) == 0 {
			newVals = make([]int64, newData.RowCount())
		}
		merged.IntColumns[field] = append(append([]int64{}, existingVals...), newVals...)
	}

	return merged
}

// sortColumnarDataByTimestamp sorts columnar data by timestamp
func (s *Storage) sortColumnarDataByTimestamp(data *ColumnarData) {
	if data.RowCount() <= 1 {
		return
	}

	n := data.RowCount()
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		return data.Timestamps[indices[i]] < data.Timestamps[indices[j]]
	})

	newTimestamps := make([]int64, n)
	newDeviceIDs := make([]string, n)
	for i, idx := range indices {
		newTimestamps[i] = data.Timestamps[idx]
		newDeviceIDs[i] = data.DeviceIDs[idx]
	}
	data.Timestamps = newTimestamps
	data.DeviceIDs = newDeviceIDs

	for field, values := range data.FloatColumns {
		newValues := make([]float64, n)
		for i, idx := range indices {
			if idx < len(values) {
				newValues[i] = values[idx]
			}
		}
		data.FloatColumns[field] = newValues
	}

	for field, values := range data.IntColumns {
		newValues := make([]int64, n)
		for i, idx := range indices {
			if idx < len(values) {
				newValues[i] = values[idx]
			}
		}
		data.IntColumns[field] = newValues
	}
}

// sliceColumnarData extracts a slice of rows from columnar data
func (s *Storage) sliceColumnarData(data *ColumnarData, start, end int) *ColumnarData {
	sliced := NewColumnarData(end - start)

	sliced.Timestamps = append(sliced.Timestamps, data.Timestamps[start:end]...)
	sliced.DeviceIDs = append(sliced.DeviceIDs, data.DeviceIDs[start:end]...)

	for field, values := range data.FloatColumns {
		if start < len(values) {
			endIdx := end
			if endIdx > len(values) {
				endIdx = len(values)
			}
			sliced.FloatColumns[field] = append(sliced.FloatColumns[field], values[start:endIdx]...)
		}
	}

	for field, values := range data.IntColumns {
		if start < len(values) {
			endIdx := end
			if endIdx > len(values) {
				endIdx = len(values)
			}
			sliced.IntColumns[field] = append(sliced.IntColumns[field], values[start:endIdx]...)
		}
	}

	return sliced
}
