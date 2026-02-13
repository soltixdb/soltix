package grpc

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
)

// StorageServiceHandler implements gRPC storage service methods
type StorageServiceHandler struct {
	pb.UnimplementedStorageServiceServer

	dataDir          string
	nodeID           string
	logger           *logging.Logger
	memoryStore      *storage.MemoryStore
	columnarStorage  *storage.TieredStorage // 4-tier group-aware storage
	aggregateStorage aggregation.AggregationStorage
}

// NewStorageServiceHandler creates a new storage service handler with V3 Columnar storage
func NewStorageServiceHandler(
	dataDir string,
	nodeID string,
	logger *logging.Logger,
	memoryStore *storage.MemoryStore,
	columnarStorage *storage.TieredStorage,
	aggregateStorage aggregation.AggregationStorage,
) *StorageServiceHandler {
	return &StorageServiceHandler{
		dataDir:          dataDir,
		nodeID:           nodeID,
		logger:           logger,
		memoryStore:      memoryStore,
		columnarStorage:  columnarStorage,
		aggregateStorage: aggregateStorage,
	}
}

// QueryShard queries data from a specific shard
func (h *StorageServiceHandler) QueryShard(ctx context.Context, req *pb.QueryShardRequest) (*pb.QueryShardResponse, error) {
	h.logger.Debug("QueryShard request received",
		"shard_id", req.ShardId,
		"database", req.Database,
		"collection", req.Collection,
		"device_count", len(req.Ids),
		"interval", req.Interval,
		"aggregation", req.Aggregation)

	// Validate request
	if req.Database == "" || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "database and collection are required")
	}

	if req.StartTime == 0 || req.EndTime == 0 {
		return nil, status.Error(codes.InvalidArgument, "start_time and end_time are required")
	}

	// Convert timestamps
	startTime := time.Unix(0, req.StartTime*1e6) // milliseconds to nanoseconds
	endTime := time.Unix(0, req.EndTime*1e6)

	h.logger.Info("QueryShard time range",
		"start_time", startTime.Format(time.RFC3339),
		"end_time", endTime.Format(time.RFC3339),
		"start_ms", req.StartTime,
		"end_ms", req.EndTime,
		"interval", req.Interval,
		"aggregation", req.Aggregation)

	// Determine if we should query aggregated data
	if h.shouldUseAggregates(req.Interval) {
		h.logger.Debug("Using aggregated data", "interval", req.Interval)
		return h.queryAggregatedData(ctx, req, startTime, endTime)
	}

	// Use raw data (default or 1m/5m intervals)
	h.logger.Debug("Using raw data", "interval", req.Interval)

	// Log memory store state
	stats := h.memoryStore.GetStats()
	h.logger.Info("Memory store state",
		"total_count", stats["total_count"],
		"total_size_mb", stats["total_size_mb"],
		"databases", stats["databases"],
		"collections", stats["collections"],
		"devices", stats["devices"])

	// Query from memory store and V3 columnar storage concurrently
	var memoryResults, fileResults []*storage.DataPoint
	var wg sync.WaitGroup

	// Query memory store (hot data, last 2h)
	wg.Go(func() {
		memoryResults = h.queryMemoryStore(req.Database, req.Collection, req.Ids, startTime, endTime, req.Fields)
	})

	// Query V3 columnar storage (cold data)
	var storageErr error

	wg.Go(func() {
		if h.columnarStorage != nil {
			fileResults, storageErr = h.columnarStorage.Query(req.Database, req.Collection, req.Ids, startTime, endTime, req.Fields)
			if storageErr != nil {
				h.logger.Warn("Failed to query columnar storage", "error", storageErr)
			}
		}
	})

	// Wait for all queries to complete
	wg.Wait()

	h.logger.Debug("Storage query results",
		"memory_points", len(memoryResults),
		"storage_points", len(fileResults))

	// Merge and deduplicate results
	mergedPoints := h.mergeAndDeduplicate(memoryResults, fileResults)

	// Filter fields if specified
	if len(req.Fields) > 0 {
		mergedPoints = h.filterFields(mergedPoints, req.Fields)
	}

	// Apply limit
	if req.Limit > 0 && uint32(len(mergedPoints)) > req.Limit {
		mergedPoints = mergedPoints[:req.Limit]
	}

	// Group by device ID
	deviceResults := h.groupByDevice(mergedPoints)

	h.logger.Debug("QueryShard completed",
		"shard_id", req.ShardId,
		"memory_points", len(memoryResults),
		"file_points", len(fileResults),
		"total_results", len(mergedPoints),
		"devices", len(deviceResults))

	return &pb.QueryShardResponse{
		Success: true,
		ShardId: req.ShardId,
		Results: deviceResults,
		Count:   uint32(len(mergedPoints)),
	}, nil
}

// queryMemoryStore queries data from in-memory cache
func (h *StorageServiceHandler) queryMemoryStore(
	database, collection string,
	deviceIDs []string,
	startTime, endTime time.Time,
	fields []string,
) []*storage.DataPoint {
	allPoints := make([]*storage.DataPoint, 0)

	if len(deviceIDs) == 0 {
		// Query all devices
		availableDevices := h.memoryStore.GetDeviceIDs(database, collection)
		h.logger.Debug("Querying all devices",
			"database", database,
			"collection", collection,
			"available_devices", len(availableDevices),
			"device_ids", availableDevices)

		for _, deviceID := range availableDevices {
			points, _ := h.memoryStore.Query(database, collection, deviceID, startTime, endTime)
			h.logger.Debug("Device query result",
				"device_id", deviceID,
				"points", len(points))
			allPoints = append(allPoints, points...)
		}
	} else {
		// Query specific devices
		h.logger.Debug("Querying specific devices",
			"database", database,
			"collection", collection,
			"device_ids", deviceIDs)

		for _, deviceID := range deviceIDs {
			points, _ := h.memoryStore.Query(database, collection, deviceID, startTime, endTime)
			h.logger.Debug("Device query result",
				"device_id", deviceID,
				"points", len(points))
			allPoints = append(allPoints, points...)
		}
	}

	h.logger.Info("Memory store query completed",
		"database", database,
		"collection", collection,
		"total_points", len(allPoints))

	return allPoints
}

// mergeAndDeduplicate merges results from memory and file storage, removing duplicates
func (h *StorageServiceHandler) mergeAndDeduplicate(memoryPoints, filePoints []*storage.DataPoint) []*storage.DataPoint {
	if len(filePoints) == 0 {
		return memoryPoints
	}
	if len(memoryPoints) == 0 {
		return filePoints
	}

	// Build map by (deviceID, time) to deduplicate
	pointMap := make(map[string]map[int64]*storage.DataPoint)

	// Add file points first (older version)
	for _, point := range filePoints {
		if pointMap[point.ID] == nil {
			pointMap[point.ID] = make(map[int64]*storage.DataPoint)
		}
		pointMap[point.ID][point.Time.UnixNano()] = point
	}

	// Add memory points (newer version, overwrite if duplicate)
	for _, point := range memoryPoints {
		if pointMap[point.ID] == nil {
			pointMap[point.ID] = make(map[int64]*storage.DataPoint)
		}
		existing := pointMap[point.ID][point.Time.UnixNano()]
		if existing == nil || point.InsertedAt.After(existing.InsertedAt) {
			pointMap[point.ID][point.Time.UnixNano()] = point
		}
	}

	// Convert back to slice
	merged := make([]*storage.DataPoint, 0, len(memoryPoints)+len(filePoints))
	for _, timeMap := range pointMap {
		for _, point := range timeMap {
			merged = append(merged, point)
		}
	}

	// Sort by time
	storage.SortDataPoints(merged)

	return merged
}

// filterFields filters data points to include only specified fields
func (h *StorageServiceHandler) filterFields(points []*storage.DataPoint, fields []string) []*storage.DataPoint {
	if len(fields) == 0 {
		return points
	}

	fieldMap := make(map[string]bool)
	for _, field := range fields {
		fieldMap[field] = true
	}

	filtered := make([]*storage.DataPoint, len(points))
	for i, point := range points {
		filteredFields := make(map[string]interface{})
		for k, v := range point.Fields {
			if fieldMap[k] {
				filteredFields[k] = v
			}
		}
		filtered[i] = &storage.DataPoint{
			Database:   point.Database,
			Collection: point.Collection,
			ID:         point.ID,
			Time:       point.Time,
			Fields:     filteredFields,
			InsertedAt: point.InsertedAt,
		}
	}

	return filtered
}

// interfaceToStorageFieldValue converts a Go interface{} value to a protobuf FieldValue.
// Supports float64, float32, int, int64, bool, and string types.
func interfaceToStorageFieldValue(v interface{}) *pb.FieldValue {
	fv := &pb.FieldValue{}
	switch val := v.(type) {
	case float64:
		fv.Value = &pb.FieldValue_NumberValue{NumberValue: val}
	case float32:
		fv.Value = &pb.FieldValue_NumberValue{NumberValue: float64(val)}
	case int:
		fv.Value = &pb.FieldValue_IntValue{IntValue: int64(val)}
	case int64:
		fv.Value = &pb.FieldValue_IntValue{IntValue: val}
	case int32:
		fv.Value = &pb.FieldValue_IntValue{IntValue: int64(val)}
	case bool:
		fv.Value = &pb.FieldValue_BoolValue{BoolValue: val}
	case string:
		fv.Value = &pb.FieldValue_StringValue{StringValue: val}
	default:
		// Fallback: store as number if possible, else skip
		fv.Value = &pb.FieldValue_NumberValue{NumberValue: 0}
	}
	return fv
}

// groupByDevice groups data points by device ID for response
func (h *StorageServiceHandler) groupByDevice(points []*storage.DataPoint) []*pb.DataPointResult {
	deviceMap := make(map[string][]*pb.DataPoint)

	for _, point := range points {
		// Convert fields to map<string, FieldValue> (supports all types)
		pbFields := make(map[string]*pb.FieldValue)
		for k, v := range point.Fields {
			if v == nil {
				continue // Skip null fields (omit from response)
			}
			pbFields[k] = interfaceToStorageFieldValue(v)
		}

		pbPoint := &pb.DataPoint{
			Time:       point.Time.UnixNano() / 1e6, // nanoseconds to milliseconds
			Id:         point.ID,
			Fields:     pbFields,
			InsertedAt: point.InsertedAt.UnixNano() / 1e6,
		}

		deviceMap[point.ID] = append(deviceMap[point.ID], pbPoint)
	}

	// Convert map to slice
	results := make([]*pb.DataPointResult, 0, len(deviceMap))
	for deviceID, dataPoints := range deviceMap {
		results = append(results, &pb.DataPointResult{
			Id:         deviceID,
			DataPoints: dataPoints,
		})
	}

	return results
}

// shouldUseAggregates determines if aggregated data should be used based on interval
func (h *StorageServiceHandler) shouldUseAggregates(interval string) bool {
	switch interval {
	case "1h", "1d", "1mo", "1y":
		return true
	case "1m", "5m", "":
		return false
	default:
		return false
	}
}

// queryAggregatedData queries aggregated data based on interval
func (h *StorageServiceHandler) queryAggregatedData(
	ctx context.Context,
	req *pb.QueryShardRequest,
	startTime, endTime time.Time,
) (*pb.QueryShardResponse, error) {
	// Map interval to aggregation level
	var aggregationLevel aggregation.AggregationLevel
	switch req.Interval {
	case "1h":
		aggregationLevel = aggregation.AggregationHourly
	case "1d":
		aggregationLevel = aggregation.AggregationDaily
	case "1mo":
		aggregationLevel = aggregation.AggregationMonthly
	case "1y":
		aggregationLevel = aggregation.AggregationYearly
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid interval for aggregated query: %s", req.Interval)
	}

	// Determine aggregation type (default to sum)
	aggregationType := req.Aggregation
	if aggregationType == "" {
		aggregationType = "sum"
	}

	h.logger.Debug("Querying aggregated data",
		"level", aggregationLevel,
		"aggregation_type", aggregationType,
		"start_time", startTime.Format(time.RFC3339),
		"end_time", endTime.Format(time.RFC3339),
		"requested_fields", req.Fields,
		"requested_device_ids", req.Ids)

	// Query aggregated data based on level
	var aggregatedPoints []*aggregation.AggregatedPoint
	var err error

	switch aggregationLevel {
	case aggregation.AggregationHourly:
		aggregatedPoints, err = h.queryHourlyAggregates(req.Database, req.Collection, startTime, endTime)
	case aggregation.AggregationDaily:
		aggregatedPoints, err = h.queryDailyAggregates(req.Database, req.Collection, startTime, endTime)
	case aggregation.AggregationMonthly:
		aggregatedPoints, err = h.queryMonthlyAggregates(req.Database, req.Collection, startTime, endTime)
	case aggregation.AggregationYearly:
		aggregatedPoints, err = h.queryYearlyAggregates(req.Database, req.Collection, startTime, endTime)
	}

	if err != nil {
		h.logger.Error("Failed to query aggregated data", "error", err, "level", aggregationLevel)
		return nil, status.Errorf(codes.Internal, "failed to query aggregated data: %v", err)
	}

	// Filter by device IDs if specified
	if len(req.Ids) > 0 {
		aggregatedPoints = h.filterAggregatedByDevices(aggregatedPoints, req.Ids)
	}

	// Convert aggregated points to data points based on aggregation type
	dataPoints := h.convertAggregatedToDataPoints(aggregatedPoints, aggregationType, req.Fields)

	// Group by device
	deviceResults := h.groupByDevice(dataPoints)

	h.logger.Debug("Aggregated query completed",
		"level", aggregationLevel,
		"aggregation_type", aggregationType,
		"aggregate_points", len(aggregatedPoints),
		"data_points", len(dataPoints),
		"devices", len(deviceResults))

	return &pb.QueryShardResponse{
		Success: true,
		ShardId: req.ShardId,
		Results: deviceResults,
		Count:   uint32(len(dataPoints)),
	}, nil
}

// queryHourlyAggregates queries hourly aggregated data
func (h *StorageServiceHandler) queryHourlyAggregates(
	database, collection string,
	startTime, endTime time.Time,
) ([]*aggregation.AggregatedPoint, error) {
	var allPoints []*aggregation.AggregatedPoint

	// Get storage timezone for consistent date boundaries
	storageTZ := h.aggregateStorage.GetTimezone()

	// Convert query times to storage timezone for date iteration
	startInTZ := startTime.In(storageTZ)
	endInTZ := endTime.In(storageTZ)

	// Truncate to day boundary in storage timezone
	currentDate := time.Date(startInTZ.Year(), startInTZ.Month(), startInTZ.Day(), 0, 0, 0, 0, storageTZ)
	endDate := time.Date(endInTZ.Year(), endInTZ.Month(), endInTZ.Day(), 0, 0, 0, 0, storageTZ)

	h.logger.Debug("Query hourly aggregates",
		"start_time", startTime,
		"end_time", endTime,
		"storage_timezone", storageTZ.String(),
		"start_date", currentDate.Format("20060102"),
		"end_date", endDate.Format("20060102"))

	for !currentDate.After(endDate) {
		date := currentDate.Format("20060102")
		points, err := h.aggregateStorage.ReadHourly(database, collection, date)
		if err != nil {
			h.logger.Warn("Failed to read hourly aggregates",
				"date", date,
				"error", err)
			currentDate = currentDate.Add(24 * time.Hour)
			continue
		}

		h.logger.Debug("Read hourly aggregates",
			"date", date,
			"points_count", len(points))

		// Filter by time range (compare actual timestamps, not timezone-converted)
		for _, point := range points {
			if (point.Time.Equal(startTime) || point.Time.After(startTime)) &&
				(point.Time.Equal(endTime) || point.Time.Before(endTime)) {
				allPoints = append(allPoints, point)
			}
		}

		currentDate = currentDate.Add(24 * time.Hour)
	}

	return allPoints, nil
}

// queryDailyAggregates queries daily aggregated data
func (h *StorageServiceHandler) queryDailyAggregates(
	database, collection string,
	startTime, endTime time.Time,
) ([]*aggregation.AggregatedPoint, error) {
	var allPoints []*aggregation.AggregatedPoint

	// Get storage timezone for consistent month boundaries
	storageTZ := h.aggregateStorage.GetTimezone()

	// Convert query times to storage timezone
	startInTZ := startTime.In(storageTZ)
	endInTZ := endTime.In(storageTZ)

	// Iterate through each month in the range (using storage timezone)
	currentMonth := time.Date(startInTZ.Year(), startInTZ.Month(), 1, 0, 0, 0, 0, storageTZ)
	endMonth := time.Date(endInTZ.Year(), endInTZ.Month(), 1, 0, 0, 0, 0, storageTZ)

	for !currentMonth.After(endMonth) {
		points, err := h.aggregateStorage.ReadDailyForMonth(database, collection, currentMonth)
		if err != nil {
			h.logger.Warn("Failed to read daily aggregates",
				"month", currentMonth.Format("200601"),
				"error", err)
			currentMonth = currentMonth.AddDate(0, 1, 0)
			continue
		}

		// Filter by time range
		for _, point := range points {
			if (point.Time.Equal(startTime) || point.Time.After(startTime)) &&
				(point.Time.Equal(endTime) || point.Time.Before(endTime)) {
				allPoints = append(allPoints, point)
			}
		}

		currentMonth = currentMonth.AddDate(0, 1, 0)
	}

	return allPoints, nil
}

// queryMonthlyAggregates queries monthly aggregated data
func (h *StorageServiceHandler) queryMonthlyAggregates(
	database, collection string,
	startTime, endTime time.Time,
) ([]*aggregation.AggregatedPoint, error) {
	var allPoints []*aggregation.AggregatedPoint

	// Get storage timezone for consistent year boundaries
	storageTZ := h.aggregateStorage.GetTimezone()

	// Convert query times to storage timezone
	startInTZ := startTime.In(storageTZ)
	endInTZ := endTime.In(storageTZ)

	// Iterate through each year in the range (using storage timezone)
	currentYear := time.Date(startInTZ.Year(), 1, 1, 0, 0, 0, 0, storageTZ)
	endYear := time.Date(endInTZ.Year(), 1, 1, 0, 0, 0, 0, storageTZ)

	for !currentYear.After(endYear) {
		points, err := h.aggregateStorage.ReadMonthlyForYear(database, collection, currentYear)
		if err != nil {
			h.logger.Warn("Failed to read monthly aggregates",
				"year", currentYear.Format("2006"),
				"error", err)
			currentYear = currentYear.AddDate(1, 0, 0)
			continue
		}

		// Filter by time range
		for _, point := range points {
			if (point.Time.Equal(startTime) || point.Time.After(startTime)) &&
				(point.Time.Equal(endTime) || point.Time.Before(endTime)) {
				allPoints = append(allPoints, point)
			}
		}

		currentYear = currentYear.AddDate(1, 0, 0)
	}

	return allPoints, nil
}

// queryYearlyAggregates queries yearly aggregated data
func (h *StorageServiceHandler) queryYearlyAggregates(
	database, collection string,
	startTime, endTime time.Time,
) ([]*aggregation.AggregatedPoint, error) {
	// Yearly aggregates are stored in a single file per database/collection
	points, err := h.aggregateStorage.ReadYearly(database, collection)
	if err != nil {
		h.logger.Warn("Failed to read yearly aggregates",
			"database", database,
			"collection", collection,
			"error", err)
		return []*aggregation.AggregatedPoint{}, nil
	}

	// Filter by time range
	var filteredPoints []*aggregation.AggregatedPoint
	for _, point := range points {
		if (point.Time.Equal(startTime) || point.Time.After(startTime)) &&
			(point.Time.Equal(endTime) || point.Time.Before(endTime)) {
			filteredPoints = append(filteredPoints, point)
		}
	}

	return filteredPoints, nil
}

// filterAggregatedByDevices filters aggregated points by device IDs
func (h *StorageServiceHandler) filterAggregatedByDevices(
	points []*aggregation.AggregatedPoint,
	deviceIDs []string,
) []*aggregation.AggregatedPoint {
	if len(deviceIDs) == 0 {
		return points
	}

	deviceMap := make(map[string]bool)
	for _, id := range deviceIDs {
		deviceMap[id] = true
	}

	filtered := make([]*aggregation.AggregatedPoint, 0)
	for _, point := range points {
		if deviceMap[point.DeviceID] {
			filtered = append(filtered, point)
		}
	}

	return filtered
}

// convertAggregatedToDataPoints converts aggregated points to data points
func (h *StorageServiceHandler) convertAggregatedToDataPoints(
	aggregatedPoints []*aggregation.AggregatedPoint,
	aggregationType string,
	requestedFields []string,
) []*storage.DataPoint {
	dataPoints := make([]*storage.DataPoint, 0, len(aggregatedPoints))

	for _, aggPoint := range aggregatedPoints {
		fields := make(map[string]interface{})

		// Extract the requested aggregation value for each field
		for fieldName, aggField := range aggPoint.Fields {
			// If specific fields requested, skip others
			if len(requestedFields) > 0 {
				found := false
				for _, reqField := range requestedFields {
					if reqField == fieldName {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Extract value based on aggregation type
			var value float64
			switch aggregationType {
			case "sum":
				value = aggField.Sum
			case "avg":
				value = aggField.Avg
			case "min":
				value = aggField.Min
			case "max":
				value = aggField.Max
			case "count":
				value = float64(aggField.Count)
			default:
				value = aggField.Sum // default to sum
			}

			fields[fieldName] = value
		}

		// Only add if there are fields
		if len(fields) > 0 {
			dataPoints = append(dataPoints, &storage.DataPoint{
				Database:   "", // Not needed for response
				Collection: "",
				ID:         aggPoint.DeviceID,
				Time:       aggPoint.Time,
				Fields:     fields,
				InsertedAt: time.Now(), // Use current time
			})
		}
	}

	return dataPoints
}

// GetShardInfo returns shard statistics and metadata
func (h *StorageServiceHandler) GetShardInfo(ctx context.Context, req *pb.GetShardInfoRequest) (*pb.GetShardInfoResponse, error) {
	h.logger.Debug("GetShardInfo request received", "shard_id", req.ShardId)

	// TODO: Implement shard info retrieval
	// For now, return basic info

	return &pb.GetShardInfoResponse{
		ShardId: req.ShardId,
		NodeId:  h.nodeID,
		Status:  "active",
	}, nil
}

// QueryShardStream streams data from a specific shard in chunks
// This is used for large dataset queries to avoid memory issues
func (h *StorageServiceHandler) QueryShardStream(req *pb.QueryShardStreamRequest, stream pb.StorageService_QueryShardStreamServer) error {
	h.logger.Debug("QueryShardStream request received",
		"shard_id", req.ShardId,
		"database", req.Database,
		"collection", req.Collection,
		"device_count", len(req.Ids),
		"interval", req.Interval,
		"chunk_size", req.ChunkSize)

	// Validate request
	if req.Database == "" || req.Collection == "" {
		return status.Error(codes.InvalidArgument, "database and collection are required")
	}

	if req.StartTime == 0 || req.EndTime == 0 {
		return status.Error(codes.InvalidArgument, "start_time and end_time are required")
	}

	// Default chunk size
	chunkSize := int(req.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	// Convert timestamps
	startTime := time.Unix(0, req.StartTime*1e6)
	endTime := time.Unix(0, req.EndTime*1e6)

	h.logger.Info("QueryShardStream time range",
		"start_time", startTime.Format(time.RFC3339),
		"end_time", endTime.Format(time.RFC3339),
		"chunk_size", chunkSize)

	// Determine data source
	if h.shouldUseAggregates(req.Interval) {
		return h.streamAggregatedData(req, stream, startTime, endTime, chunkSize)
	}

	return h.streamRawData(req, stream, startTime, endTime, chunkSize)
}

// streamRawData streams raw data points in chunks
func (h *StorageServiceHandler) streamRawData(
	req *pb.QueryShardStreamRequest,
	stream pb.StorageService_QueryShardStreamServer,
	startTime, endTime time.Time,
	chunkSize int,
) error {
	// Query from memory store
	memoryResults := h.queryMemoryStore(req.Database, req.Collection, req.Ids, startTime, endTime, req.Fields)

	// Query from columnar storage
	var fileResults []*storage.DataPoint
	var storageErr error
	if h.columnarStorage != nil {
		fileResults, storageErr = h.columnarStorage.Query(req.Database, req.Collection, req.Ids, startTime, endTime, req.Fields)
		if storageErr != nil {
			h.logger.Warn("Failed to query columnar storage", "error", storageErr)
		}
	}

	// Merge and deduplicate
	mergedPoints := h.mergeAndDeduplicate(memoryResults, fileResults)

	// Filter fields if specified
	if len(req.Fields) > 0 {
		mergedPoints = h.filterFields(mergedPoints, req.Fields)
	}

	h.logger.Debug("Streaming raw data",
		"total_points", len(mergedPoints),
		"chunk_size", chunkSize)

	// Stream in chunks
	return h.streamDataPointsInChunks(req.ShardId, mergedPoints, chunkSize, stream)
}

// streamAggregatedData streams aggregated data points in chunks
func (h *StorageServiceHandler) streamAggregatedData(
	req *pb.QueryShardStreamRequest,
	stream pb.StorageService_QueryShardStreamServer,
	startTime, endTime time.Time,
	chunkSize int,
) error {
	if h.aggregateStorage == nil {
		return status.Error(codes.Unavailable, "aggregate storage not available")
	}

	// Determine aggregation level
	var aggregationLevel aggregation.AggregationLevel
	switch req.Interval {
	case "1h":
		aggregationLevel = aggregation.AggregationHourly
	case "1d":
		aggregationLevel = aggregation.AggregationDaily
	case "1mo":
		aggregationLevel = aggregation.AggregationMonthly
	case "1y":
		aggregationLevel = aggregation.AggregationYearly
	default:
		return status.Errorf(codes.InvalidArgument, "invalid interval for aggregated query: %s", req.Interval)
	}

	// Determine aggregation type (default to sum)
	aggregationType := req.Aggregation
	if aggregationType == "" {
		aggregationType = "sum"
	}

	h.logger.Debug("Streaming aggregated data",
		"level", aggregationLevel,
		"aggregation_type", aggregationType,
		"interval", req.Interval)

	// Query aggregated data based on level
	var aggregatedPoints []*aggregation.AggregatedPoint
	var err error

	switch aggregationLevel {
	case aggregation.AggregationHourly:
		aggregatedPoints, err = h.queryHourlyAggregates(req.Database, req.Collection, startTime, endTime)
	case aggregation.AggregationDaily:
		aggregatedPoints, err = h.queryDailyAggregates(req.Database, req.Collection, startTime, endTime)
	case aggregation.AggregationMonthly:
		aggregatedPoints, err = h.queryMonthlyAggregates(req.Database, req.Collection, startTime, endTime)
	case aggregation.AggregationYearly:
		aggregatedPoints, err = h.queryYearlyAggregates(req.Database, req.Collection, startTime, endTime)
	}

	if err != nil {
		h.logger.Error("Failed to query aggregated data for streaming", "error", err, "level", aggregationLevel)
		return status.Errorf(codes.Internal, "failed to query aggregated data: %v", err)
	}

	// Filter by device IDs if specified
	if len(req.Ids) > 0 {
		aggregatedPoints = h.filterAggregatedByDevices(aggregatedPoints, req.Ids)
	}

	// Convert aggregated points to data points based on aggregation type
	dataPoints := h.convertAggregatedToDataPoints(aggregatedPoints, aggregationType, req.Fields)

	h.logger.Debug("Streaming aggregated data",
		"total_points", len(dataPoints),
		"chunk_size", chunkSize)

	// Stream in chunks
	return h.streamDataPointsInChunks(req.ShardId, dataPoints, chunkSize, stream)
}

// streamDataPointsInChunks streams data points in fixed-size chunks
func (h *StorageServiceHandler) streamDataPointsInChunks(
	shardID uint32,
	points []*storage.DataPoint,
	chunkSize int,
	stream pb.StorageService_QueryShardStreamServer,
) error {
	totalPoints := len(points)
	chunkID := uint32(0)

	for i := 0; i < totalPoints; i += chunkSize {
		// Check if context is cancelled
		if err := stream.Context().Err(); err != nil {
			h.logger.Warn("Stream context cancelled", "error", err)
			return err
		}

		end := i + chunkSize
		if end > totalPoints {
			end = totalPoints
		}

		chunkPoints := points[i:end]
		isFinal := end >= totalPoints

		// Group by device for this chunk
		deviceResults := h.groupByDevice(chunkPoints)

		chunk := &pb.QueryShardChunk{
			ChunkId: chunkID,
			ShardId: shardID,
			Results: deviceResults,
			Count:   uint32(len(chunkPoints)),
			IsFinal: isFinal,
		}

		if err := stream.Send(chunk); err != nil {
			h.logger.Error("Failed to send chunk", "chunk_id", chunkID, "error", err)
			return err
		}

		h.logger.Debug("Sent chunk",
			"chunk_id", chunkID,
			"points", len(chunkPoints),
			"is_final", isFinal)

		chunkID++
	}

	// Send empty final chunk if no data
	if totalPoints == 0 {
		chunk := &pb.QueryShardChunk{
			ChunkId: 0,
			ShardId: shardID,
			Results: nil,
			Count:   0,
			IsFinal: true,
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}

	h.logger.Info("QueryShardStream completed",
		"shard_id", shardID,
		"total_points", totalPoints,
		"chunks_sent", chunkID)

	return nil
}
