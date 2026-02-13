package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient implements RemoteSyncClient using gRPC
type GRPCClient struct {
	logger      *logging.Logger
	connections map[string]*grpc.ClientConn
	mu          sync.RWMutex
	timeout     time.Duration
}

// NewGRPCClient creates a new gRPC sync client
func NewGRPCClient(logger *logging.Logger, timeout time.Duration) *GRPCClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &GRPCClient{
		logger:      logger,
		connections: make(map[string]*grpc.ClientConn),
		timeout:     timeout,
	}
}

// getConnection gets or creates a gRPC connection to a node
func (c *GRPCClient) getConnection(address string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, exists := c.connections[address]
	c.mu.RUnlock()

	if exists && conn != nil {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double check after acquiring write lock
	if conn, exists = c.connections[address]; exists && conn != nil {
		return conn, nil
	}

	// Create new connection using NewClient (grpc.DialContext is deprecated)
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	c.connections[address] = conn
	c.logger.Debug("Created gRPC connection", "address", address)
	return conn, nil
}

// SyncShard streams data points from a remote node
func (c *GRPCClient) SyncShard(ctx context.Context, nodeAddress string, req *SyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	pointsCh := make(chan *storage.DataPoint, 1000)
	errCh := make(chan error, 1)

	go func() {
		defer close(pointsCh)
		defer close(errCh)

		conn, err := c.getConnection(nodeAddress)
		if err != nil {
			errCh <- err
			return
		}

		client := pb.NewStorageServiceClient(conn)

		// Use SyncService if available, otherwise fall back to QueryShard
		// For now, use QueryShard as fallback since SyncService proto isn't defined yet
		grpcReq := &pb.QueryShardRequest{
			Database:   req.Database,
			Collection: req.Collection,
			StartTime:  req.FromTimestamp.UnixMilli(),
			EndTime:    req.ToTimestamp.UnixMilli(),
			Ids:        req.DeviceIDs,
		}

		resp, err := client.QueryShard(ctx, grpcReq)
		if err != nil {
			errCh <- fmt.Errorf("failed to query shard: %w", err)
			return
		}

		if !resp.Success {
			errCh <- fmt.Errorf("query failed: %s", resp.Error)
			return
		}

		// Convert response to data points
		for _, result := range resp.Results {
			for _, dp := range result.DataPoints {
				point := &storage.DataPoint{
					Database:   req.Database,
					Collection: req.Collection,
					ShardID:    req.ShardID,
					Time:       time.UnixMilli(dp.Time),
					ID:         dp.Id,
					Fields:     make(map[string]interface{}),
					InsertedAt: time.UnixMilli(dp.InsertedAt),
				}

				// Convert fields
				for k, v := range dp.Fields {
					point.Fields[k] = v
				}

				select {
				case pointsCh <- point:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			}
		}
	}()

	return pointsCh, errCh
}

// GetChecksum gets the checksum of a shard from a remote node
func (c *GRPCClient) GetChecksum(ctx context.Context, nodeAddress string, req *ChecksumRequest) (*SyncChecksum, error) {
	conn, err := c.getConnection(nodeAddress)
	if err != nil {
		return nil, err
	}

	client := pb.NewStorageServiceClient(conn)

	// Query data and calculate checksum locally
	// This is a temporary solution until we add a dedicated checksum RPC
	grpcReq := &pb.QueryShardRequest{
		Database:   req.Database,
		Collection: req.Collection,
		StartTime:  req.FromTimestamp.UnixMilli(),
		EndTime:    req.ToTimestamp.UnixMilli(),
	}

	resp, err := client.QueryShard(ctx, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to query for checksum: %w", err)
	}

	// Calculate checksum from response
	checksum := calculateChecksum(resp)

	return &SyncChecksum{
		ShardID:    req.ShardID,
		PointCount: int64(resp.Count),
		Checksum:   checksum,
		FromTime:   req.FromTimestamp,
		ToTime:     req.ToTimestamp,
	}, nil
}

// GetLastTimestamp gets the last timestamp of a shard from a remote node
func (c *GRPCClient) GetLastTimestamp(ctx context.Context, nodeAddress string, req *LastTimestampRequest) (time.Time, error) {
	conn, err := c.getConnection(nodeAddress)
	if err != nil {
		return time.Time{}, err
	}

	client := pb.NewStorageServiceClient(conn)

	// Query recent data to find last timestamp
	// This is a temporary solution - should use dedicated RPC
	now := time.Now()
	grpcReq := &pb.QueryShardRequest{
		Database:   req.Database,
		Collection: req.Collection,
		StartTime:  now.Add(-24 * time.Hour).UnixMilli(),
		EndTime:    now.UnixMilli(),
		Limit:      1,
	}

	resp, err := client.QueryShard(ctx, grpcReq)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to query for last timestamp: %w", err)
	}

	var lastTS time.Time
	for _, result := range resp.Results {
		for _, dp := range result.DataPoints {
			ts := time.UnixMilli(dp.Time)
			if ts.After(lastTS) {
				lastTS = ts
			}
		}
	}

	return lastTS, nil
}

// Close closes all gRPC connections
func (c *GRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for addr, conn := range c.connections {
		if err := conn.Close(); err != nil {
			c.logger.Warn("Failed to close connection", "address", addr, "error", err)
			lastErr = err
		}
	}
	c.connections = make(map[string]*grpc.ClientConn)
	return lastErr
}

// SyncGroup streams data points for a specific group from a remote node
func (c *GRPCClient) SyncGroup(ctx context.Context, nodeAddress string, req *GroupSyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	pointsCh := make(chan *storage.DataPoint, 1000)
	errCh := make(chan error, 1)

	go func() {
		defer close(pointsCh)
		defer close(errCh)

		conn, err := c.getConnection(nodeAddress)
		if err != nil {
			errCh <- err
			return
		}

		client := pb.NewStorageServiceClient(conn)

		// Use QueryShard as fallback - filter by group via device IDs
		grpcReq := &pb.QueryShardRequest{
			Database:   req.Database,
			Collection: req.Collection,
			StartTime:  req.FromTimestamp.UnixMilli(),
			EndTime:    req.ToTimestamp.UnixMilli(),
			Ids:        req.DeviceIDs,
		}

		resp, err := client.QueryShard(ctx, grpcReq)
		if err != nil {
			errCh <- fmt.Errorf("failed to sync group %d: %w", req.GroupID, err)
			return
		}

		if !resp.Success {
			errCh <- fmt.Errorf("sync group %d failed: %s", req.GroupID, resp.Error)
			return
		}

		for _, result := range resp.Results {
			for _, dp := range result.DataPoints {
				point := &storage.DataPoint{
					Database:   req.Database,
					Collection: req.Collection,
					GroupID:    req.GroupID,
					Time:       time.UnixMilli(dp.Time),
					ID:         dp.Id,
					Fields:     make(map[string]interface{}),
					InsertedAt: time.UnixMilli(dp.InsertedAt),
				}
				for k, v := range dp.Fields {
					point.Fields[k] = v
				}

				select {
				case pointsCh <- point:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			}
		}
	}()

	return pointsCh, errCh
}

// GetGroupChecksum gets the checksum of a group from a remote node
func (c *GRPCClient) GetGroupChecksum(ctx context.Context, nodeAddress string, req *GroupChecksumRequest) (*SyncChecksum, error) {
	conn, err := c.getConnection(nodeAddress)
	if err != nil {
		return nil, err
	}

	client := pb.NewStorageServiceClient(conn)

	grpcReq := &pb.QueryShardRequest{
		Database:   req.Database,
		Collection: req.Collection,
		StartTime:  req.FromTimestamp.UnixMilli(),
		EndTime:    req.ToTimestamp.UnixMilli(),
	}

	resp, err := client.QueryShard(ctx, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to query for group checksum: %w", err)
	}

	checksum := calculateChecksum(resp)

	return &SyncChecksum{
		GroupID:    req.GroupID,
		PointCount: int64(resp.Count),
		Checksum:   checksum,
		FromTime:   req.FromTimestamp,
		ToTime:     req.ToTimestamp,
	}, nil
}

// calculateChecksum calculates a simple checksum from query response
func calculateChecksum(resp *pb.QueryShardResponse) string {
	if resp == nil || len(resp.Results) == 0 {
		return "empty"
	}

	// Simple checksum: combine count and first/last timestamps
	var minTime, maxTime int64
	for _, result := range resp.Results {
		for _, dp := range result.DataPoints {
			if minTime == 0 || dp.Time < minTime {
				minTime = dp.Time
			}
			if dp.Time > maxTime {
				maxTime = dp.Time
			}
		}
	}

	return fmt.Sprintf("%d:%d:%d", resp.Count, minTime, maxTime)
}

// StreamingSyncClient provides streaming sync capabilities
// This is for future use when we add streaming RPC to proto
type StreamingSyncClient struct {
	*GRPCClient
}

// NewStreamingSyncClient creates a new streaming sync client
func NewStreamingSyncClient(logger *logging.Logger, timeout time.Duration) *StreamingSyncClient {
	return &StreamingSyncClient{
		GRPCClient: NewGRPCClient(logger, timeout),
	}
}

// SyncShardStreaming streams data using server-side streaming RPC
// This is a placeholder for when we add streaming RPC support
func (c *StreamingSyncClient) SyncShardStreaming(ctx context.Context, nodeAddress string, req *SyncRequest) (<-chan *storage.DataPoint, <-chan error) {
	// For now, fall back to non-streaming implementation
	return c.SyncShard(ctx, nodeAddress, req)
}
