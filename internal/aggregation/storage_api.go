package aggregation

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// =============================================================================
// High-level Write API (V6 - single-file parts with footer index)
// =============================================================================

// WriteAggregatedPoints writes aggregated points to V6 columnar storage.
// Points are organized into Device Groups (DG):
//   - Devices are split into groups of maxDevicesPerGroup
//   - Each DG gets single V6 part files containing ALL metrics for ALL fields
//
// Metadata is split into 2 tiers:
//   - Global: partDir/_metadata.idx (DG definitions, device->DG map, field schema)
//   - Per-DG: partDir/dg_XXXX/_metadata.idx (part info, device names, stats)
//
// Directory: partDir/dg_XXXX/part_XXXX.bin
func (s *Storage) WriteAggregatedPoints(level AggregationLevel, database, collection string, points []*AggregatedPoint, t time.Time) error {
	if len(points) == 0 {
		return nil
	}

	partDir := s.getPartitionDir(level, database, collection, t)

	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return fmt.Errorf("failed to create partition directory: %w", err)
	}

	// Read existing global metadata or create new
	meta, err := s.readMetadata(partDir)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	if meta == nil {
		meta = &PartitionMetadata{
			Version:        6,
			Level:          string(level),
			CreatedAt:      time.Now(),
			DeviceGroupMap: make(map[string]int),
		}
	}
	// Ensure maps are initialized (for existing metadata loaded from disk)
	if meta.DeviceGroupMap == nil {
		meta.DeviceGroupMap = make(map[string]int)
	}

	// Collect all device IDs and field names
	allDeviceIDs := collectDeviceIDs(points)
	allFieldNames := collectFieldNames(points)

	// Assign device groups
	s.assignDeviceGroups(meta, allDeviceIDs)

	// Update field schema
	if len(meta.Fields) == 0 {
		for i, name := range allFieldNames {
			meta.Fields = append(meta.Fields, FieldMeta{
				Name:     name,
				Type:     "float64",
				Position: i,
			})
		}
	} else {
		// Add any new fields
		existingFields := make(map[string]struct{})
		for _, f := range meta.Fields {
			existingFields[f.Name] = struct{}{}
		}
		for _, name := range allFieldNames {
			if _, exists := existingFields[name]; !exists {
				meta.Fields = append(meta.Fields, FieldMeta{
					Name:     name,
					Type:     "float64",
					Position: len(meta.Fields),
				})
			}
		}
	}

	// Group points by device group index
	pointsByDG := make(map[int][]*AggregatedPoint)
	for _, point := range points {
		dgIdx := meta.DeviceGroupMap[point.DeviceID]
		pointsByDG[dgIdx] = append(pointsByDG[dgIdx], point)
	}

	// Track time range and device count
	var minTime, maxTime int64
	deviceSet := make(map[string]struct{})
	for _, point := range points {
		ts := point.Time.UnixNano()
		if minTime == 0 || ts < minTime {
			minTime = ts
		}
		if ts > maxTime {
			maxTime = ts
		}
		deviceSet[point.DeviceID] = struct{}{}
	}

	// Write data per DG (V6: single part file per DG containing ALL metrics)
	for dgIdx, dgPoints := range pointsByDG {
		dg := meta.DeviceGroups[dgIdx]
		dgDir := filepath.Join(partDir, dg.DirName)

		// Read or create per-DG metadata
		dgMeta, err := s.readDGMetadata(dgDir)
		if err != nil {
			return fmt.Errorf("failed to read DG metadata %s: %w", dg.DirName, err)
		}
		if dgMeta == nil {
			dgMeta = &DGMetadata{
				Version:    6,
				GroupIndex: dgIdx,
				DirName:    dg.DirName,
			}
		}
		dgMeta.DeviceNames = dg.DeviceNames

		// Build V6 columnar data with ALL metrics
		cd := NewColumnarData(len(dgPoints))
		for _, point := range dgPoints {
			cd.AddRowAllMetrics(point.Time.UnixNano(), point.DeviceID, point.Fields)
		}

		if cd.RowCount() == 0 {
			continue
		}

		// Determine whether to append to last part or create a new one
		var pendingWrites []*pendingPartWrite

		if len(dgMeta.Parts) > 0 {
			lastPart := &dgMeta.Parts[len(dgMeta.Parts)-1]

			// Check size-based limit
			overSize := s.config.MaxPartSize > 0 && lastPart.Size >= s.config.MaxPartSize
			overRows := lastPart.RowCount+int64(cd.RowCount()) > int64(s.config.MaxRowsPerPart)

			if !overSize && !overRows {
				// Append to existing part - read existing data and merge
				existingFilePath := filepath.Join(dgDir, lastPart.FileName)
				existingFooter, fErr := s.readV6PartFooter(existingFilePath)
				if fErr != nil {
					return fmt.Errorf("failed to read existing part footer %s: %w", lastPart.FileName, fErr)
				}
				if existingFooter != nil {
					// Read all existing data
					existingData, rErr := s.readV6PartFileData(
						existingFilePath, existingFooter,
						nil, nil, nil,
						math.MinInt64, math.MaxInt64,
					)
					if rErr != nil {
						return fmt.Errorf("failed to read existing part %s: %w", lastPart.FileName, rErr)
					}
					if existingData != nil && existingData.RowCount() > 0 {
						cd = s.mergeColumnarDataForWrite(existingData, cd)
					}
				}

				pending, wErr := s.writeV6PartFile(dgDir, lastPart.PartNum, level, cd)
				if wErr != nil {
					return fmt.Errorf("failed to write merged part for %s: %w", dg.DirName, wErr)
				}
				if pending != nil {
					pending.isAppend = true
					pendingWrites = append(pendingWrites, pending)
				}
			} else {
				// Create new part
				partNum := lastPart.PartNum + 1

				pending, wErr := s.writeV6PartFile(dgDir, partNum, level, cd)
				if wErr != nil {
					return fmt.Errorf("failed to write part for %s: %w", dg.DirName, wErr)
				}
				if pending != nil {
					pending.isAppend = false
					pendingWrites = append(pendingWrites, pending)
				}
			}
		} else {
			// First part for this DG
			pending, wErr := s.writeV6PartFile(dgDir, 0, level, cd)
			if wErr != nil {
				return fmt.Errorf("failed to write first part for %s: %w", dg.DirName, wErr)
			}
			if pending != nil {
				pending.isAppend = false
				pendingWrites = append(pendingWrites, pending)
			}
		}

		// Commit all pending writes
		if err := s.commitPendingWrites(pendingWrites); err != nil {
			return fmt.Errorf("failed to commit writes for %s: %w", dg.DirName, err)
		}

		// Update DG metadata with new part info
		for _, pending := range pendingWrites {
			if pending == nil || pending.partInfo == nil {
				continue
			}
			if pending.isAppend {
				dgMeta.Parts[len(dgMeta.Parts)-1] = *pending.partInfo
			} else {
				dgMeta.Parts = append(dgMeta.Parts, *pending.partInfo)
			}
		}

		// Update DG metadata stats
		var dgRowCount int64
		var dgMinTime, dgMaxTime int64
		for _, p := range dgMeta.Parts {
			dgRowCount += p.RowCount
			if dgMinTime == 0 || p.MinTime < dgMinTime {
				dgMinTime = p.MinTime
			}
			if p.MaxTime > dgMaxTime {
				dgMaxTime = p.MaxTime
			}
		}
		dgMeta.RowCount = dgRowCount
		dgMeta.MinTime = dgMinTime
		dgMeta.MaxTime = dgMaxTime

		// Write per-DG metadata
		if err := s.writeDGMetadata(dgDir, dgMeta); err != nil {
			return fmt.Errorf("failed to write DG metadata %s: %w", dg.DirName, err)
		}
	}

	// Update global metadata stats
	meta.RowCount += int64(len(points))
	meta.DeviceCount = len(deviceSet)
	if meta.MinTime == 0 || minTime < meta.MinTime {
		meta.MinTime = minTime
	}
	if maxTime > meta.MaxTime {
		meta.MaxTime = maxTime
	}

	// Write global metadata
	if err := s.writeMetadata(partDir, meta); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	if s.logger != nil {
		s.logger.Debug("Wrote aggregated points",
			"level", level,
			"database", database,
			"collection", collection,
			"points", len(points),
			"device_groups", len(meta.DeviceGroups),
			"partDir", partDir)
	}

	return nil
}

// =============================================================================
// High-level Read API
// =============================================================================

// ReadOptions configures read operations
type ReadOptions struct {
	StartTime time.Time
	EndTime   time.Time
	DeviceIDs []string     // Optional: filter by device IDs
	Fields    []string     // Optional: projection
	Metrics   []MetricType // Optional: which metrics to read
}

// ReadAggregatedPoints reads aggregated points from V6 columnar storage
func (s *Storage) ReadAggregatedPoints(level AggregationLevel, database, collection string, opts ReadOptions) ([]*AggregatedPoint, error) {
	// If no metrics specified, read all
	if len(opts.Metrics) == 0 {
		opts.Metrics = AllMetricTypes()
	}

	// Determine partition directories to scan
	partDirs := s.getPartitionDirsInRange(level, database, collection, opts.StartTime, opts.EndTime)

	// Build device ID filter set if provided
	var deviceFilter map[string]struct{}
	if len(opts.DeviceIDs) > 0 {
		deviceFilter = make(map[string]struct{})
		for _, id := range opts.DeviceIDs {
			deviceFilter[id] = struct{}{}
		}
	}

	// Build field filter set if provided
	var fieldFilter map[string]struct{}
	if len(opts.Fields) > 0 {
		fieldFilter = make(map[string]struct{})
		for _, f := range opts.Fields {
			fieldFilter[f] = struct{}{}
		}
	}

	// Build metric filter set
	var metricFilter map[MetricType]struct{}
	if len(opts.Metrics) > 0 && len(opts.Metrics) < len(AllMetricTypes()) {
		metricFilter = make(map[MetricType]struct{})
		for _, m := range opts.Metrics {
			metricFilter[m] = struct{}{}
		}
	}

	// Read data from all partitions
	// Key: timestamp_deviceID -> AggregatedPoint
	pointsMap := make(map[string]*AggregatedPoint)

	startNano := opts.StartTime.UnixNano()
	endNano := opts.EndTime.UnixNano()

	for _, partDir := range partDirs {
		if err := s.readPartitionData(partDir, startNano, endNano, deviceFilter, fieldFilter, metricFilter, pointsMap); err != nil {
			if s.logger != nil {
				s.logger.Error("Failed to read partition", "partDir", partDir, "error", err)
			}
			// Continue with other partitions
		}
	}

	// Convert map to slice and sort by timestamp
	result := make([]*AggregatedPoint, 0, len(pointsMap))
	for _, point := range pointsMap {
		result = append(result, point)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Time.Equal(result[j].Time) {
			return result[i].DeviceID < result[j].DeviceID
		}
		return result[i].Time.Before(result[j].Time)
	})

	return result, nil
}

// getPartitionDirsInRange returns partition directories that may contain data in the time range
func (s *Storage) getPartitionDirsInRange(level AggregationLevel, database, collection string, start, end time.Time) []string {
	var dirs []string

	switch level {
	case AggregationHourly:
		// Hourly data is organized by day
		for t := start.Truncate(24 * time.Hour); !t.After(end); t = t.AddDate(0, 0, 1) {
			dir := s.getPartitionDir(level, database, collection, t)
			if _, err := os.Stat(dir); err == nil {
				dirs = append(dirs, dir)
			}
		}

	case AggregationDaily:
		// Daily data is organized by month
		for t := TruncateToMonth(start.In(s.timezone)); !t.After(end); t = t.AddDate(0, 1, 0) {
			dir := s.getPartitionDir(level, database, collection, t)
			if _, err := os.Stat(dir); err == nil {
				dirs = append(dirs, dir)
			}
		}

	case AggregationMonthly:
		// Monthly data is organized by year
		for t := TruncateToYear(start.In(s.timezone)); !t.After(end); t = t.AddDate(1, 0, 0) {
			dir := s.getPartitionDir(level, database, collection, t)
			if _, err := os.Stat(dir); err == nil {
				dirs = append(dirs, dir)
			}
		}

	case AggregationYearly:
		// Yearly data is in a single directory
		dir := s.getPartitionDir(level, database, collection, start)
		if _, err := os.Stat(dir); err == nil {
			dirs = append(dirs, dir)
		}
	}

	return dirs
}

// readPartitionData reads data from one partition directory (V6 2-tier metadata layout)
func (s *Storage) readPartitionData(
	partDir string,
	startNano, endNano int64,
	deviceFilter map[string]struct{},
	fieldFilter map[string]struct{},
	metricFilter map[MetricType]struct{},
	pointsMap map[string]*AggregatedPoint,
) error {
	meta, err := s.readMetadata(partDir)
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}

	// Check if partition overlaps time range
	if meta.MaxTime < startNano || meta.MinTime > endNano {
		return nil
	}

	// Iterate over DGs
	for _, dg := range meta.DeviceGroups {
		// If device filter is set, check if any device in this DG matches
		if deviceFilter != nil {
			dgMatch := false
			for _, devName := range dg.DeviceNames {
				if _, ok := deviceFilter[devName]; ok {
					dgMatch = true
					break
				}
			}
			if !dgMatch {
				continue // Skip this DG entirely
			}
		}

		dgDir := filepath.Join(partDir, dg.DirName)

		// Read per-DG metadata to get part info
		dgMeta, err := s.readDGMetadata(dgDir)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("Failed to read DG metadata", "dgDir", dgDir, "error", err)
			}
			continue
		}
		if dgMeta == nil {
			continue
		}

		// Read each V6 part file
		for _, part := range dgMeta.Parts {
			// Check if part overlaps time range
			if part.MaxTime < startNano || part.MinTime > endNano {
				continue
			}

			filePath := filepath.Join(dgDir, part.FileName)
			footer, fErr := s.readV6PartFooter(filePath)
			if fErr != nil {
				return fmt.Errorf("failed to read part footer %s/%s: %w", dg.DirName, part.FileName, fErr)
			}
			if footer == nil {
				continue
			}

			data, rErr := s.readV6PartFileData(filePath, footer, deviceFilter, fieldFilter, metricFilter, startNano, endNano)
			if rErr != nil {
				return fmt.Errorf("failed to read part %s/%s: %w", dg.DirName, part.FileName, rErr)
			}
			if data == nil || data.RowCount() == 0 {
				continue
			}

			// Merge V6 columnar data into pointsMap
			s.mergeV6ColumnarData(data, deviceFilter, pointsMap)
		}
	}

	return nil
}

// mergeV6ColumnarData merges V6 columnar data (with all metrics) into points map
func (s *Storage) mergeV6ColumnarData(
	data *ColumnarData,
	deviceFilter map[string]struct{},
	pointsMap map[string]*AggregatedPoint,
) {
	for i := 0; i < data.RowCount(); i++ {
		ts := data.Timestamps[i]
		deviceID := data.DeviceIDs[i]

		// Apply device filter
		if deviceFilter != nil {
			if _, ok := deviceFilter[deviceID]; !ok {
				continue
			}
		}

		// Get or create point
		key := fmt.Sprintf("%d_%s", ts, deviceID)
		point, ok := pointsMap[key]
		if !ok {
			point = &AggregatedPoint{
				Time:     time.Unix(0, ts),
				DeviceID: deviceID,
				Fields:   make(map[string]*AggregatedField),
			}
			pointsMap[key] = point
		}

		// Merge float columns (sum, avg, min, max)
		for colKey, values := range data.FloatColumns {
			metric, fieldName := v6ParseFloatKey(colKey)
			if fieldName == "" || i >= len(values) {
				continue
			}

			field, ok := point.Fields[fieldName]
			if !ok {
				field = &AggregatedField{}
				point.Fields[fieldName] = field
			}

			switch metric {
			case MetricSum:
				field.Sum = values[i]
			case MetricAvg:
				field.Avg = values[i]
			case MetricMin:
				field.Min = values[i]
			case MetricMax:
				field.Max = values[i]
			}
		}

		// Merge int columns (count)
		for colKey, values := range data.IntColumns {
			metric, fieldName := v6ParseIntKey(colKey)
			if fieldName == "" || i >= len(values) {
				continue
			}

			field, ok := point.Fields[fieldName]
			if !ok {
				field = &AggregatedField{}
				point.Fields[fieldName] = field
			}

			switch metric {
			case MetricCount:
				field.Count = values[i]
			case MetricMinTime:
				field.MinTime = values[i]
			case MetricMaxTime:
				field.MaxTime = values[i]
			}
		}
	}
}

// =============================================================================
// Query helpers
// =============================================================================

// GetPartitionStats returns statistics for a partition
func (s *Storage) GetPartitionStats(level AggregationLevel, database, collection string, t time.Time) (*PartitionMetadata, error) {
	partDir := s.getPartitionDir(level, database, collection, t)
	return s.readMetadata(partDir)
}

// ListPartitions lists all partitions for a level/database/collection
// Only returns directories containing global partition metadata (not DG metadata)
func (s *Storage) ListPartitions(level AggregationLevel, database, collection string) ([]string, error) {
	baseDir := filepath.Join(s.baseDir, "agg_"+string(level), database, collection)

	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		return nil, nil
	}

	var partitions []string
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if info.IsDir() {
			// Skip DG directories (they have their own _metadata.idx)
			base := filepath.Base(path)
			if len(base) >= 3 && base[:3] == "dg_" {
				return filepath.SkipDir
			}

			metaPath := filepath.Join(path, MetadataFile)
			if _, err := os.Stat(metaPath); err == nil {
				partitions = append(partitions, path)
			}
		}
		return nil
	})

	return partitions, err
}

// =============================================================================
// Cleanup/Maintenance
// =============================================================================

// CompactOptions configures compaction behavior
type CompactOptions struct {
	// MinPartsToCompact is the minimum number of parts needed to trigger compaction
	// Default: 2
	MinPartsToCompact int

	// TargetRowsPerPart is the target rows per compacted part
	// Default: uses StorageConfig.MaxRowsPerPart
	TargetRowsPerPart int

	// DryRun if true, only reports what would be done without making changes
	DryRun bool
}

// DefaultCompactOptions returns default compaction options
func DefaultCompactOptions() CompactOptions {
	return CompactOptions{
		MinPartsToCompact: 2,
		TargetRowsPerPart: 0, // Use config default
		DryRun:            false,
	}
}

// CompactResult contains the result of a compaction operation
type CompactResult struct {
	PartDir      string
	DGDir        string
	PartsBefore  int
	PartsAfter   int
	RowsMerged   int64
	FilesRemoved []string
}

// Compact merges multiple small parts into larger parts
// This reduces the number of files and improves read performance
func (s *Storage) Compact(partDir string) error {
	return s.CompactWithOptions(partDir, DefaultCompactOptions())
}

// CompactWithOptions performs compaction with custom options
func (s *Storage) CompactWithOptions(partDir string, opts CompactOptions) error {
	results, err := s.doCompact(partDir, opts)
	if err != nil {
		return err
	}

	// Log results
	if s.logger != nil {
		for _, r := range results {
			if r.PartsBefore > r.PartsAfter {
				s.logger.Info("Compacted partition",
					"partDir", r.PartDir,
					"dg", r.DGDir,
					"parts_before", r.PartsBefore,
					"parts_after", r.PartsAfter,
					"rows_merged", r.RowsMerged,
					"files_removed", len(r.FilesRemoved))
			}
		}
	}

	return nil
}

// CompactAll compacts all partitions for a level/database/collection
func (s *Storage) CompactAll(level AggregationLevel, database, collection string) ([]CompactResult, error) {
	partitions, err := s.ListPartitions(level, database, collection)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions: %w", err)
	}

	var allResults []CompactResult
	for _, partDir := range partitions {
		results, err := s.doCompact(partDir, DefaultCompactOptions())
		if err != nil {
			if s.logger != nil {
				s.logger.Error("Failed to compact partition", "partDir", partDir, "error", err)
			}
			continue
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

// doCompact performs the actual compaction logic (V6 2-tier metadata layout)
func (s *Storage) doCompact(partDir string, opts CompactOptions) ([]CompactResult, error) {
	// Read global metadata
	meta, err := s.readMetadata(partDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	if meta == nil {
		return nil, nil // No data to compact
	}

	// Determine target rows per part
	targetRows := opts.TargetRowsPerPart
	if targetRows <= 0 {
		targetRows = s.config.MaxRowsPerPart
	}

	var results []CompactResult

	// Compact each DG
	for _, dg := range meta.DeviceGroups {
		dgDir := filepath.Join(partDir, dg.DirName)

		// Read DG metadata for part info
		dgMeta, err := s.readDGMetadata(dgDir)
		if err != nil || dgMeta == nil {
			continue
		}

		if len(dgMeta.Parts) < opts.MinPartsToCompact {
			continue
		}

		result, cErr := s.compactDGParts(dgDir, dgMeta.Parts, targetRows, meta.Level, opts.DryRun)
		if cErr != nil {
			return nil, fmt.Errorf("failed to compact %s: %w", dg.DirName, cErr)
		}

		if result != nil {
			result.PartDir = partDir
			result.DGDir = dg.DirName
			results = append(results, *result)

			if !opts.DryRun && result.PartsAfter < result.PartsBefore {
				// Update DG metadata with new parts info
				dgMeta.Parts = s.buildNewV6PartsInfo(dgDir, result.PartsAfter)

				// Recalculate DG stats
				var dgRowCount int64
				var dgMinTime, dgMaxTime int64
				for _, p := range dgMeta.Parts {
					dgRowCount += p.RowCount
					if dgMinTime == 0 || p.MinTime < dgMinTime {
						dgMinTime = p.MinTime
					}
					if p.MaxTime > dgMaxTime {
						dgMaxTime = p.MaxTime
					}
				}
				dgMeta.RowCount = dgRowCount
				dgMeta.MinTime = dgMinTime
				dgMeta.MaxTime = dgMaxTime

				if wErr := s.writeDGMetadata(dgDir, dgMeta); wErr != nil {
					return nil, fmt.Errorf("failed to write DG metadata: %w", wErr)
				}
			}
		}
	}

	// Invalidate global metadata cache
	if !opts.DryRun && len(results) > 0 {
		s.invalidateMetadataCache(partDir)
	}

	return results, nil
}

// compactDGParts compacts all V6 parts in a device group
func (s *Storage) compactDGParts(dgDir string, parts []PartInfo, targetRows int, levelStr string, dryRun bool) (*CompactResult, error) {
	if len(parts) < 2 {
		return nil, nil
	}

	// Read all parts and merge into one large dataset
	var allData *ColumnarData
	var totalRows int64

	for _, part := range parts {
		filePath := filepath.Join(dgDir, part.FileName)
		footer, fErr := s.readV6PartFooter(filePath)
		if fErr != nil {
			return nil, fmt.Errorf("failed to read part footer %s: %w", part.FileName, fErr)
		}
		if footer == nil {
			continue
		}

		data, rErr := s.readV6PartFileData(filePath, footer, nil, nil, nil, math.MinInt64, math.MaxInt64)
		if rErr != nil {
			return nil, fmt.Errorf("failed to read part %s: %w", part.FileName, rErr)
		}
		if data == nil || data.RowCount() == 0 {
			continue
		}

		totalRows += int64(data.RowCount())

		if allData == nil {
			allData = data
		} else {
			allData = s.mergeColumnarDataForWrite(allData, data)
		}
	}

	if allData == nil || allData.RowCount() == 0 {
		return nil, nil
	}

	// Sort merged data by timestamp
	s.sortColumnarDataByTimestamp(allData)

	// Calculate how many new parts we need
	numNewParts := (allData.RowCount() + targetRows - 1) / targetRows

	// Ensure we don't produce more or equal parts (to avoid infinite loop)
	if numNewParts >= len(parts) {
		return nil, nil
	}

	result := &CompactResult{
		PartsBefore: len(parts),
		PartsAfter:  numNewParts,
		RowsMerged:  totalRows,
	}

	if dryRun {
		return result, nil
	}

	level := AggregationLevel(levelStr)

	// Write new compacted parts
	newPartsWritten := 0
	for partNum := 0; partNum < numNewParts; partNum++ {
		startIdx := partNum * targetRows
		endIdx := startIdx + targetRows
		if endIdx > allData.RowCount() {
			endIdx = allData.RowCount()
		}

		// Slice the data for this part
		partData := s.sliceColumnarData(allData, startIdx, endIdx)

		// Write with .compact suffix first (atomic operation)
		compactFileName := fmt.Sprintf("part_%04d.compact", partNum)
		compactFilePath := filepath.Join(dgDir, compactFileName)

		pending, wErr := s.writeV6PartFile(dgDir, partNum, level, partData)
		if wErr != nil {
			s.cleanupCompactFiles(dgDir)
			return nil, fmt.Errorf("failed to write compacted part %d: %w", partNum, wErr)
		}
		if pending != nil {
			// Rename .tmp to .compact
			if rErr := os.Rename(pending.tmpPath, compactFilePath); rErr != nil {
				s.cleanupCompactFiles(dgDir)
				return nil, fmt.Errorf("failed to rename to compact file: %w", rErr)
			}
		}
		newPartsWritten++
	}

	// Now atomically replace old parts with new ones
	// 1. Remove old part files
	for _, part := range parts {
		filePath := filepath.Join(dgDir, part.FileName)
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			if s.logger != nil {
				s.logger.Warn("Failed to remove old part file", "file", filePath, "error", err)
			}
		}
		result.FilesRemoved = append(result.FilesRemoved, part.FileName)
	}

	// 2. Rename compact files to final names
	for partNum := 0; partNum < newPartsWritten; partNum++ {
		compactFileName := fmt.Sprintf("part_%04d.compact", partNum)
		compactFilePath := filepath.Join(dgDir, compactFileName)
		finalFileName := s.getPartFileName(partNum)
		finalFilePath := filepath.Join(dgDir, finalFileName)

		if rErr := os.Rename(compactFilePath, finalFilePath); rErr != nil {
			return nil, fmt.Errorf("failed to rename compact file: %w", rErr)
		}
	}

	return result, nil
}

// cleanupCompactFiles removes any leftover .compact files
func (s *Storage) cleanupCompactFiles(dgDir string) {
	pattern := filepath.Join(dgDir, "part_*.compact")
	files, _ := filepath.Glob(pattern)
	for _, f := range files {
		_ = os.Remove(f)
	}
}

// buildNewV6PartsInfo reads the new V6 part files and builds PartInfo slice
func (s *Storage) buildNewV6PartsInfo(dgDir string, numParts int) []PartInfo {
	parts := make([]PartInfo, 0, numParts)

	for partNum := 0; partNum < numParts; partNum++ {
		fileName := s.getPartFileName(partNum)
		filePath := filepath.Join(dgDir, fileName)

		stat, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		// Read footer to get device/field info
		footer, fErr := s.readV6PartFooter(filePath)
		if fErr != nil || footer == nil {
			continue
		}

		// Calculate row count from footer
		var rowCount uint32
		for _, rc := range footer.RowCountPerDev {
			rowCount += rc
		}

		// Get min/max time from _time columns
		var minTime, maxTime int64
		for _, col := range footer.Columns {
			if col.FieldIdx == 0 { // _time column
				// We'd need to read the data to get actual values
				// For now, use a simpler approach
				break
			}
		}

		// Read _time data for accurate min/max
		data, rErr := s.readV6PartFileData(filePath, footer, nil, nil, nil, math.MinInt64, math.MaxInt64)
		if rErr == nil && data != nil && data.RowCount() > 0 {
			minTime = data.Timestamps[0]
			maxTime = data.Timestamps[len(data.Timestamps)-1]
		}

		parts = append(parts, PartInfo{
			PartNum:  partNum,
			FileName: fileName,
			RowCount: int64(rowCount),
			MinTime:  minTime,
			MaxTime:  maxTime,
			Size:     stat.Size(),
		})
	}

	return parts
}

// NeedsCompaction checks if a partition needs compaction (V6 2-tier metadata layout)
func (s *Storage) NeedsCompaction(partDir string, minParts int) (bool, map[string]int) {
	meta, err := s.readMetadata(partDir)
	if err != nil || meta == nil {
		return false, nil
	}

	partsCount := make(map[string]int)
	needsCompact := false

	// Count parts per DG
	for _, dg := range meta.DeviceGroups {
		dgDir := filepath.Join(partDir, dg.DirName)
		dgMeta, dgErr := s.readDGMetadata(dgDir)
		if dgErr != nil || dgMeta == nil {
			continue
		}

		partsCount[dg.DirName] = len(dgMeta.Parts)
		if len(dgMeta.Parts) >= minParts {
			needsCompact = true
		}
	}

	return needsCompact, partsCount
}

// DeleteOldData deletes aggregation data older than a specified time
func (s *Storage) DeleteOldData(level AggregationLevel, database, collection string, before time.Time) error {
	partDirs := s.getPartitionDirsInRange(level, database, collection, time.Time{}, before)

	for _, partDir := range partDirs {
		meta, err := s.readMetadata(partDir)
		if err != nil {
			continue
		}
		if meta == nil {
			continue
		}

		// Only delete if all data is before the cutoff
		if meta.MaxTime < before.UnixNano() {
			if err := os.RemoveAll(partDir); err != nil {
				if s.logger != nil {
					s.logger.Error("Failed to delete partition", "partDir", partDir, "error", err)
				}
			} else {
				s.invalidateMetadataCache(partDir)
				if s.logger != nil {
					s.logger.Info("Deleted old aggregation partition", "partDir", partDir)
				}
			}
		}
	}

	return nil
}
