package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/grpc"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
)

// QueryCoordinator coordinates distributed queries across storage nodes
type QueryCoordinator struct {
	logger          *logging.Logger
	metadataManager metadata.Manager
	shardRouter     *ShardRouter
	grpcPool        *grpc.ConnectionPool
	formatter       *ResultFormatter
}

// NewQueryCoordinator creates a new query coordinator
func NewQueryCoordinator(
	logger *logging.Logger,
	metadataManager metadata.Manager,
	shardRouter *ShardRouter,
) *QueryCoordinator {
	return &QueryCoordinator{
		logger:          logger,
		metadataManager: metadataManager,
		shardRouter:     shardRouter,
		grpcPool:        grpc.NewConnectionPool(logger),
		formatter:       NewResultFormatter(),
	}
}

// QueryRequest represents a query request
type QueryRequest struct {
	Database    string
	Collection  string
	DeviceIDs   []string
	StartTime   time.Time
	EndTime     time.Time
	Fields      []string
	Limit       int64
	Interval    string // 1m, 5m, 1h, 1d, 1mo, 1y
	Aggregation string // max, min, avg, sum
}

// QueryResult represents query results in old format (for internal processing)
type QueryResult struct {
	DeviceID   string                 `json:"device_id"`
	DataPoints []models.DataPointView `json:"data_points"`
}

// StreamChunk represents a chunk of streaming results
type StreamChunk struct {
	ChunkID    int32                  `json:"chunk_id"`
	DeviceID   string                 `json:"device_id"`
	DataPoints []models.DataPointView `json:"data_points"`
	IsFinal    bool                   `json:"is_final"`
	Error      error                  `json:"-"`
}

// StreamQueryRequest extends QueryRequest with streaming options
type StreamQueryRequest struct {
	QueryRequest
	ChunkSize int32 // Number of data points per chunk
}

// FormattedQueryResult represents query results in new time-series format
type FormattedQueryResult struct {
	DeviceID string                   `json:"id"`
	Times    []string                 `json:"times"`
	Fields   map[string][]interface{} `json:"-"` // field_name -> [value1, value2, ...] (nil for missing)
}

// MarshalJSON customizes JSON output to flatten fields to top level
func (f FormattedQueryResult) MarshalJSON() ([]byte, error) {
	// Create a map with id and times
	result := make(map[string]interface{})
	result["id"] = f.DeviceID
	result["times"] = f.Times

	// Add all fields at top level
	for fieldName, fieldValues := range f.Fields {
		result[fieldName] = fieldValues
	}

	return json.Marshal(result)
}

// fieldValueToInterface converts a protobuf FieldValue to a Go interface{}.
func fieldValueToInterface(fv *pb.FieldValue) interface{} {
	if fv == nil {
		return nil
	}
	switch v := fv.Value.(type) {
	case *pb.FieldValue_NumberValue:
		return v.NumberValue
	case *pb.FieldValue_IntValue:
		return v.IntValue
	case *pb.FieldValue_BoolValue:
		return v.BoolValue
	case *pb.FieldValue_StringValue:
		return v.StringValue
	default:
		return nil
	}
}

// Query executes a distributed query across storage nodes
// Routes based on device groups: hash(db, collection, device_id) → group → nodes
// device_ids are required — data without device_id is considered invalid
// Post-processing (downsampling, anomaly detection, timezone conversion) should be done by handlers
func (qc *QueryCoordinator) Query(ctx context.Context, req *QueryRequest) ([]FormattedQueryResult, error) {
	qc.logger.Debug("Query request received",
		"database", req.Database,
		"collection", req.Collection,
		"start_time", req.StartTime,
		"end_time", req.EndTime,
		"device_count", len(req.DeviceIDs),
		"interval", req.Interval,
		"aggregation", req.Aggregation,
		"limit", req.Limit)

	// device_ids are required for all queries
	if len(req.DeviceIDs) == 0 {
		return nil, fmt.Errorf("device_ids (ids) are required for queries")
	}

	// 1. Route devices to groups and determine which nodes to query
	groupManager := qc.shardRouter.GetGroupManager()
	nodeShards := make(map[string][]*ShardAssignment)

	// Group devices by their assigned group/nodes
	type groupDevices struct {
		group   *GroupAssignment
		devices []string
	}
	groupMap := make(map[int]*groupDevices)

	for _, deviceID := range req.DeviceIDs {
		groupAssignment, err := groupManager.RouteWrite(ctx, req.Database, req.Collection, deviceID)
		if err != nil {
			qc.logger.Warn("Failed to route device to group",
				"device_id", deviceID,
				"error", err)
			continue
		}

		if _, exists := groupMap[groupAssignment.GroupID]; !exists {
			groupMap[groupAssignment.GroupID] = &groupDevices{
				group:   groupAssignment,
				devices: make([]string, 0),
			}
		}
		groupMap[groupAssignment.GroupID].devices = append(groupMap[groupAssignment.GroupID].devices, deviceID)
	}

	// Convert to nodeShards format (query primary node of each group)
	for _, gd := range groupMap {
		primaryNode := gd.group.PrimaryNode
		assignment := &ShardAssignment{
			ShardID:      fmt.Sprintf("group_%d", gd.group.GroupID),
			Database:     req.Database,
			Collection:   req.Collection,
			PrimaryNode:  primaryNode,
			ReplicaNodes: gd.group.ReplicaNodes,
		}
		nodeShards[primaryNode] = append(nodeShards[primaryNode], assignment)
	}

	if len(nodeShards) == 0 {
		qc.logger.Warn("No nodes to query")
		return []FormattedQueryResult{}, nil
	}

	qc.logger.Info("Grouped query by nodes",
		"node_count", len(nodeShards),
		"nodes", func() []string {
			nodes := make([]string, 0, len(nodeShards))
			for nodeID := range nodeShards {
				nodes = append(nodes, nodeID)
			}
			return nodes
		}())

	// 2. Execute parallel queries to nodes
	results, queryMeta := qc.executeParallelQueries(ctx, req, nodeShards)

	// Log query metadata if there were partial failures
	if queryMeta.PartialData {
		qc.logger.Warn("Query completed with partial data",
			"total_nodes", queryMeta.TotalNodes,
			"failed_nodes", queryMeta.FailedNodes)
	}

	// 3. Merge, deduplicate and format results using formatter
	formatted := qc.formatter.MergeAndFormat(results, req.Fields)

	// 4. Apply global limit if specified
	if req.Limit > 0 {
		formatted = qc.formatter.ApplyLimit(formatted, req.Limit)
	}

	qc.logger.Debug("Query completed",
		"devices", len(formatted),
		"total_points", qc.formatter.CountPoints(formatted))

	return formatted, nil
}

// routeDevicesToNodes routes device IDs to their group nodes and returns nodeShards mapping
func (qc *QueryCoordinator) routeDevicesToNodes(ctx context.Context, database, collection string, deviceIDs []string) map[string][]*ShardAssignment {
	groupManager := qc.shardRouter.GetGroupManager()
	nodeShards := make(map[string][]*ShardAssignment)

	type groupDevices struct {
		group   *GroupAssignment
		devices []string
	}
	groupMap := make(map[int]*groupDevices)

	for _, deviceID := range deviceIDs {
		groupAssignment, err := groupManager.RouteWrite(ctx, database, collection, deviceID)
		if err != nil {
			qc.logger.Warn("Failed to route device to group",
				"device_id", deviceID,
				"error", err)
			continue
		}

		if _, exists := groupMap[groupAssignment.GroupID]; !exists {
			groupMap[groupAssignment.GroupID] = &groupDevices{
				group:   groupAssignment,
				devices: make([]string, 0),
			}
		}
		groupMap[groupAssignment.GroupID].devices = append(groupMap[groupAssignment.GroupID].devices, deviceID)
	}

	for _, gd := range groupMap {
		primaryNode := gd.group.PrimaryNode
		assignment := &ShardAssignment{
			ShardID:      fmt.Sprintf("group_%d", gd.group.GroupID),
			Database:     database,
			Collection:   collection,
			PrimaryNode:  primaryNode,
			ReplicaNodes: gd.group.ReplicaNodes,
		}
		nodeShards[primaryNode] = append(nodeShards[primaryNode], assignment)
	}

	return nodeShards
}

// NodeQueryRequest represents a query for a specific node
type NodeQueryRequest struct {
	NodeID      string
	Shards      []*ShardAssignment
	DeviceIDs   []string
	StartTime   time.Time
	EndTime     time.Time
	Fields      []string
	Limit       int64
	Interval    string
	Aggregation string
}

// NodeQueryResult contains results and potential error from a node query
type NodeQueryResult struct {
	NodeID  string
	Results []QueryResult
	Error   error
}

// QueryMetadata contains metadata about query execution
type QueryMetadata struct {
	TotalNodes  int      `json:"total_nodes"`
	FailedNodes []string `json:"failed_nodes,omitempty"`
	PartialData bool     `json:"partial_data"`
}

// executeParallelQueries executes queries to all nodes in parallel
func (qc *QueryCoordinator) executeParallelQueries(ctx context.Context, req *QueryRequest, nodeShards map[string][]*ShardAssignment) ([]QueryResult, *QueryMetadata) {
	var wg sync.WaitGroup
	resultsChan := make(chan NodeQueryResult, len(nodeShards))

	// Launch parallel queries to each node
	for nodeID, shards := range nodeShards {
		wg.Add(1)
		go func(nodeID string, shards []*ShardAssignment) {
			defer wg.Done()

			nodeResults, err := qc.queryNode(ctx, &NodeQueryRequest{
				NodeID:      nodeID,
				Shards:      shards,
				DeviceIDs:   req.DeviceIDs,
				StartTime:   req.StartTime,
				EndTime:     req.EndTime,
				Fields:      req.Fields,
				Limit:       req.Limit,
				Interval:    req.Interval,
				Aggregation: req.Aggregation,
			})

			resultsChan <- NodeQueryResult{
				NodeID:  nodeID,
				Results: nodeResults,
				Error:   err,
			}
		}(nodeID, shards)
	}

	// Wait for all queries to complete
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect all results and track failures
	var allResults []QueryResult
	var failedNodes []string

	for nodeResult := range resultsChan {
		if nodeResult.Error != nil {
			failedNodes = append(failedNodes, nodeResult.NodeID)
			qc.logger.Warn("Node query failed",
				"node_id", nodeResult.NodeID,
				"error", nodeResult.Error.Error())
		} else {
			allResults = append(allResults, nodeResult.Results...)
		}
	}

	metadata := &QueryMetadata{
		TotalNodes:  len(nodeShards),
		FailedNodes: failedNodes,
		PartialData: len(failedNodes) > 0,
	}

	if len(failedNodes) > 0 {
		qc.logger.Warn("Query completed with partial data",
			"total_nodes", len(nodeShards),
			"failed_nodes", failedNodes,
			"successful_nodes", len(nodeShards)-len(failedNodes))
	}

	return allResults, metadata
}

// queryNode queries a single storage node for multiple shards
func (qc *QueryCoordinator) queryNode(ctx context.Context, req *NodeQueryRequest) ([]QueryResult, error) {
	// Get node address from etcd
	nodeAddress, err := qc.getNodeAddress(ctx, req.NodeID)
	if err != nil {
		qc.logger.Error("Failed to get node address",
			"node_id", req.NodeID,
			"error", err.Error())
		return nil, fmt.Errorf("failed to get node %s address: %w", req.NodeID, err)
	}

	qc.logger.Debug("Querying storage node",
		"node_id", req.NodeID,
		"address", nodeAddress,
		"shards", len(req.Shards))

	// Get or create gRPC connection
	conn, err := qc.grpcPool.GetConnection(nodeAddress)
	if err != nil {
		qc.logger.Error("Failed to get gRPC connection",
			"node_id", req.NodeID,
			"address", nodeAddress,
			"error", err.Error())
		return nil, fmt.Errorf("failed to connect to node %s at %s: %w", req.NodeID, nodeAddress, err)
	}

	client := pb.NewStorageServiceClient(conn)

	// Query each shard on this node
	var allResults []QueryResult
	var shardErrors []string

	for _, shard := range req.Shards {
		shardResults, err := qc.queryShard(ctx, client, shard, req)
		if err != nil {
			shardErrors = append(shardErrors, fmt.Sprintf("shard %s: %v", shard.ShardID, err))
			continue
		}
		allResults = append(allResults, shardResults...)
	}

	// If all shards failed, return error
	if len(shardErrors) == len(req.Shards) {
		return nil, fmt.Errorf("all shards failed on node %s: %v", req.NodeID, shardErrors)
	}

	// If some shards failed, log warning but return partial results
	if len(shardErrors) > 0 {
		qc.logger.Warn("Some shards failed on node",
			"node_id", req.NodeID,
			"failed_shards", len(shardErrors),
			"total_shards", len(req.Shards),
			"errors", shardErrors)
	}

	return allResults, nil
}

// queryShard queries a single shard
func (qc *QueryCoordinator) queryShard(ctx context.Context, client pb.StorageServiceClient, shard *ShardAssignment, req *NodeQueryRequest) ([]QueryResult, error) {
	// Create query request
	pbReq := &pb.QueryShardRequest{
		ShardId:     0, // Not used currently
		Database:    shard.Database,
		Collection:  shard.Collection,
		Ids:         req.DeviceIDs,
		StartTime:   req.StartTime.UnixNano() / 1e6, // Convert to milliseconds
		EndTime:     req.EndTime.UnixNano() / 1e6,
		Fields:      req.Fields,
		Limit:       uint32(req.Limit),
		Interval:    req.Interval,
		Aggregation: req.Aggregation,
	}

	qc.logger.Debug("Sending QueryShard request",
		"shard_id", shard.ShardID,
		"node_id", req.NodeID,
		"database", shard.Database,
		"collection", shard.Collection,
		"start_time_ms", pbReq.StartTime,
		"end_time_ms", pbReq.EndTime,
		"device_ids", req.DeviceIDs)

	// Create timeout context for this query
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Execute query
	resp, err := client.QueryShard(queryCtx, pbReq)
	if err != nil {
		qc.logger.Error("Failed to query shard",
			"shard_id", shard.ShardID,
			"node_id", req.NodeID,
			"error", err.Error(),
			"error_type", fmt.Sprintf("%T", err))
		return nil, fmt.Errorf("query shard %s failed: %w", shard.ShardID, err)
	}

	if !resp.Success {
		qc.logger.Warn("Shard query failed",
			"shard_id", shard.ShardID,
			"error", resp.Error)
		return nil, fmt.Errorf("shard %s query unsuccessful: %s", shard.ShardID, resp.Error)
	}

	// Convert protobuf results to QueryResult
	results := make([]QueryResult, 0, len(resp.Results))
	for _, deviceResult := range resp.Results {
		dataPoints := make([]models.DataPointView, 0, len(deviceResult.DataPoints))
		for _, dp := range deviceResult.DataPoints {
			// Convert map[string]*pb.FieldValue to map[string]interface{}
			fields := make(map[string]interface{}, len(dp.Fields))
			for k, fv := range dp.Fields {
				fields[k] = fieldValueToInterface(fv)
			}
			dataPoints = append(dataPoints, models.DataPointView{
				Time:   time.Unix(0, dp.Time*1e6).Format(time.RFC3339), // Convert ms to RFC3339
				ID:     dp.Id,
				Fields: fields,
			})
		}

		results = append(results, QueryResult{
			DeviceID:   deviceResult.Id,
			DataPoints: dataPoints,
		})
	}

	qc.logger.Debug("Shard query completed",
		"shard_id", shard.ShardID,
		"results", len(results))

	return results, nil
}

// getNodeAddress retrieves the gRPC address for a node from etcd
func (qc *QueryCoordinator) getNodeAddress(ctx context.Context, nodeID string) (string, error) {
	nodeKey := fmt.Sprintf("/soltix/nodes/%s", nodeID)
	nodeData, err := qc.metadataManager.Get(ctx, nodeKey)
	if err != nil {
		return "", fmt.Errorf("node not found: %w", err)
	}

	// Parse JSON manually
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(nodeData), &raw); err != nil {
		return "", fmt.Errorf("failed to parse node info: %w", err)
	}

	if addr, ok := raw["address"].(string); ok {
		return addr, nil
	}

	return "", fmt.Errorf("address field not found in node info")
}

// QueryStream executes a streaming query and returns a channel of chunks
// This streams data from storage nodes via gRPC and forwards to caller
// device_ids are required — data without device_id is considered invalid
func (qc *QueryCoordinator) QueryStream(ctx context.Context, req *StreamQueryRequest) <-chan StreamChunk {
	chunkChan := make(chan StreamChunk, 100) // Buffered channel for backpressure

	go func() {
		defer close(chunkChan)

		if len(req.DeviceIDs) == 0 {
			qc.logger.Warn("Streaming query requires device_ids")
			chunkChan <- StreamChunk{Error: fmt.Errorf("device_ids (ids) are required for streaming queries")}
			chunkChan <- StreamChunk{IsFinal: true}
			return
		}

		// Route devices to groups/nodes
		nodeShards := qc.routeDevicesToNodes(ctx, req.Database, req.Collection, req.DeviceIDs)

		if len(nodeShards) == 0 {
			qc.logger.Warn("No nodes found for streaming query",
				"database", req.Database,
				"collection", req.Collection,
				"device_count", len(req.DeviceIDs))
			chunkChan <- StreamChunk{IsFinal: true}
			return
		}

		qc.logger.Debug("Routing streaming query by device groups",
			"node_count", len(nodeShards),
			"database", req.Database,
			"collection", req.Collection)

		// Execute parallel streaming queries to all nodes
		qc.executeParallelStreamQueries(ctx, req, nodeShards, chunkChan)
	}()

	return chunkChan
}

// executeParallelStreamQueries executes streaming queries to all nodes in parallel
func (qc *QueryCoordinator) executeParallelStreamQueries(ctx context.Context, req *StreamQueryRequest, nodeShards map[string][]*ShardAssignment, chunkChan chan<- StreamChunk) {
	var wg sync.WaitGroup
	chunkIDCounter := int32(0)
	var counterMu sync.Mutex

	// Launch parallel streaming queries to each node
	for nodeID, shards := range nodeShards {
		wg.Add(1)
		go func(nodeID string, shards []*ShardAssignment) {
			defer wg.Done()

			err := qc.streamFromNode(ctx, &NodeQueryRequest{
				NodeID:      nodeID,
				Shards:      shards,
				DeviceIDs:   req.DeviceIDs,
				StartTime:   req.StartTime,
				EndTime:     req.EndTime,
				Fields:      req.Fields,
				Limit:       req.Limit,
				Interval:    req.Interval,
				Aggregation: req.Aggregation,
			}, req.ChunkSize, chunkChan, &chunkIDCounter, &counterMu)
			if err != nil {
				qc.logger.Error("Failed to stream from node",
					"node_id", nodeID,
					"error", err.Error())
				// Send error chunk
				select {
				case chunkChan <- StreamChunk{Error: err}:
				case <-ctx.Done():
				}
			}
		}(nodeID, shards)
	}

	// Wait for all streaming queries to complete
	wg.Wait()

	// Send final chunk
	select {
	case chunkChan <- StreamChunk{IsFinal: true}:
	case <-ctx.Done():
	}
}

// streamFromNode streams data from a single node
func (qc *QueryCoordinator) streamFromNode(ctx context.Context, req *NodeQueryRequest, chunkSize int32, chunkChan chan<- StreamChunk, chunkIDCounter *int32, counterMu *sync.Mutex) error {
	// Get node address from etcd
	nodeAddress, err := qc.getNodeAddress(ctx, req.NodeID)
	if err != nil {
		return fmt.Errorf("failed to get node %s address: %w", req.NodeID, err)
	}

	qc.logger.Debug("Streaming from storage node",
		"node_id", req.NodeID,
		"address", nodeAddress,
		"shards", len(req.Shards))

	// Get or create gRPC connection
	conn, err := qc.grpcPool.GetConnection(nodeAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to node %s at %s: %w", req.NodeID, nodeAddress, err)
	}

	client := pb.NewStorageServiceClient(conn)

	// Stream from each shard on this node
	for _, shard := range req.Shards {
		err := qc.streamFromShard(ctx, client, shard, req, chunkSize, chunkChan, chunkIDCounter, counterMu)
		if err != nil {
			qc.logger.Warn("Failed to stream from shard",
				"shard_id", shard.ShardID,
				"error", err.Error())
			// Continue with other shards
		}
	}

	return nil
}

// streamFromShard streams data from a single shard using gRPC streaming
func (qc *QueryCoordinator) streamFromShard(ctx context.Context, client pb.StorageServiceClient, shard *ShardAssignment, req *NodeQueryRequest, chunkSize int32, chunkChan chan<- StreamChunk, chunkIDCounter *int32, counterMu *sync.Mutex) error {
	// Create streaming query request
	pbReq := &pb.QueryShardStreamRequest{
		Database:    shard.Database,
		Collection:  shard.Collection,
		Ids:         req.DeviceIDs,
		StartTime:   req.StartTime.UnixNano() / 1e6, // Convert to milliseconds
		EndTime:     req.EndTime.UnixNano() / 1e6,
		Fields:      req.Fields,
		Interval:    req.Interval,
		Aggregation: req.Aggregation,
		ChunkSize:   uint32(chunkSize),
	}

	qc.logger.Debug("Starting gRPC stream from shard",
		"shard_id", shard.ShardID,
		"database", shard.Database,
		"collection", shard.Collection,
		"chunk_size", chunkSize)

	// Create timeout context for this stream
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Execute streaming query
	stream, err := client.QueryShardStream(streamCtx, pbReq)
	if err != nil {
		return fmt.Errorf("failed to start stream from shard %s: %w", shard.ShardID, err)
	}

	// Receive and forward chunks
	for {
		chunk, err := stream.Recv()
		if err != nil {
			// Check if stream ended normally
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("error receiving chunk from shard %s: %w", shard.ShardID, err)
		}

		// Process each device result in the chunk
		for _, deviceResult := range chunk.Results {
			dataPoints := make([]models.DataPointView, 0, len(deviceResult.DataPoints))
			for _, dp := range deviceResult.DataPoints {
				fields := make(map[string]interface{}, len(dp.Fields))
				for k, fv := range dp.Fields {
					fields[k] = fieldValueToInterface(fv)
				}
				dataPoints = append(dataPoints, models.DataPointView{
					Time:   time.Unix(0, dp.Time*1e6).Format(time.RFC3339),
					ID:     dp.Id,
					Fields: fields,
				})
			}

			// Get next chunk ID
			counterMu.Lock()
			*chunkIDCounter++
			currentChunkID := *chunkIDCounter
			counterMu.Unlock()

			// Forward to output channel
			select {
			case chunkChan <- StreamChunk{
				ChunkID:    currentChunkID,
				DeviceID:   deviceResult.Id,
				DataPoints: dataPoints,
				IsFinal:    false,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// If this is the final chunk from this shard, break
		if chunk.IsFinal {
			break
		}
	}

	qc.logger.Debug("Completed streaming from shard",
		"shard_id", shard.ShardID)

	return nil
}
