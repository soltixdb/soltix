package sync

import (
	"context"
	goSync "sync"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
	"google.golang.org/grpc"
)

func TestNewGRPCClient(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewGRPCClient(logger, 30*time.Second)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	if client.timeout != 30*time.Second {
		t.Errorf("Expected timeout=30s, got %v", client.timeout)
	}
	if len(client.connections) != 0 {
		t.Errorf("Expected empty connections map")
	}
}

func TestNewGRPCClient_ZeroTimeout(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewGRPCClient(logger, 0)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	// Zero timeout should default to 30s
	if client.timeout != 30*time.Second {
		t.Errorf("Expected default timeout=30s, got %v", client.timeout)
	}
}

func TestNewGRPCClient_NegativeTimeout(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewGRPCClient(logger, -5*time.Second)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	// Negative timeout should default to 30s
	if client.timeout != 30*time.Second {
		t.Errorf("Expected default timeout=30s, got %v", client.timeout)
	}
}

func TestGRPCClient_Close_NoConnections(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 30*time.Second)

	err := client.Close()
	if err != nil {
		t.Fatalf("Expected no error closing empty client, got: %v", err)
	}
}

func TestCalculateChecksum_NilResponse(t *testing.T) {
	result := calculateChecksum(nil)
	if result != "empty" {
		t.Errorf("Expected 'empty' for nil response, got %s", result)
	}
}

func TestCalculateChecksum_EmptyResults(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Results: []*pb.DataPointResult{},
	}

	result := calculateChecksum(resp)
	if result != "empty" {
		t.Errorf("Expected 'empty' for empty results, got %s", result)
	}
}

func TestCalculateChecksum_WithData(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 3,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{
					{Time: 1000, Id: "dev-1"},
					{Time: 2000, Id: "dev-2"},
					{Time: 3000, Id: "dev-3"},
				},
			},
		},
	}

	result := calculateChecksum(resp)
	if result == "" {
		t.Error("Expected non-empty checksum")
	}
	if result == "empty" {
		t.Error("Expected non-empty checksum for data")
	}

	// Verify format: "count:minTime:maxTime"
	expected := "3:1000:3000"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestCalculateChecksum_SinglePoint(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 1,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{
					{Time: 5000, Id: "dev-1"},
				},
			},
		},
	}

	result := calculateChecksum(resp)
	expected := "1:5000:5000"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestCalculateChecksum_MultipleResults(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 4,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{
					{Time: 1000, Id: "dev-1"},
					{Time: 3000, Id: "dev-2"},
				},
			},
			{
				DataPoints: []*pb.DataPoint{
					{Time: 500, Id: "dev-3"},
					{Time: 4000, Id: "dev-4"},
				},
			},
		},
	}

	result := calculateChecksum(resp)
	expected := "4:500:4000"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestNewStreamingSyncClient(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewStreamingSyncClient(logger, 30*time.Second)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	if client.timeout != 30*time.Second {
		t.Errorf("Expected timeout=30s, got %v", client.timeout)
	}
}

func TestNewStreamingSyncClient_ZeroTimeout(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewStreamingSyncClient(logger, 0)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	// Should default to 30s
	if client.timeout != 30*time.Second {
		t.Errorf("Expected default timeout=30s, got %v", client.timeout)
	}
}

func TestShardAssignment_Fields(t *testing.T) {
	now := time.Now()
	sa := ShardAssignment{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
		PrimaryNode:    "node-1",
		ReplicaNodes:   []string{"node-2", "node-3"},
	}

	if sa.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if sa.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
	if sa.PrimaryNode != "node-1" {
		t.Errorf("PrimaryNode mismatch")
	}
	if len(sa.ReplicaNodes) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(sa.ReplicaNodes))
	}
}

func TestGroupAssignment_Fields(t *testing.T) {
	ga := GroupAssignment{
		GroupID:      1,
		PrimaryNode:  "node-1",
		ReplicaNodes: []string{"node-2"},
		State:        "active",
		Epoch:        12345,
	}

	if ga.GroupID != 1 {
		t.Errorf("GroupID mismatch")
	}
	if ga.PrimaryNode != "node-1" {
		t.Errorf("PrimaryNode mismatch")
	}
	if ga.State != "active" {
		t.Errorf("State mismatch")
	}
	if ga.Epoch != 12345 {
		t.Errorf("Epoch mismatch")
	}
}

func TestNewEtcdMetadataManager(t *testing.T) {
	logger := logging.NewDevelopment()

	// nil client is acceptable for construction
	manager := NewEtcdMetadataManager(nil, logger)
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}
	if manager.client != nil {
		t.Error("Expected nil client")
	}
}

func TestGRPCClient_GetConnection(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 30*time.Second)

	// grpc.NewClient creates a lazy connection, so this should succeed
	conn, err := client.getConnection("localhost:50051")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if conn == nil {
		t.Fatal("Expected non-nil connection")
	}

	// Second call should return cached connection
	conn2, err := client.getConnection("localhost:50051")
	if err != nil {
		t.Fatalf("Expected no error on second call, got: %v", err)
	}
	if conn2 != conn {
		t.Error("Expected same connection object (cached)")
	}

	// Different address should create a new connection
	conn3, err := client.getConnection("localhost:50052")
	if err != nil {
		t.Fatalf("Expected no error for different address, got: %v", err)
	}
	if conn3 == conn {
		t.Error("Expected different connection for different address")
	}

	// Cleanup
	_ = client.Close()
}

func TestGRPCClient_GetConnection_ConcurrentAccess(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 30*time.Second)

	// Test concurrent getConnection calls to the same address
	var wg goSync.WaitGroup
	conns := make([]*grpc.ClientConn, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := client.getConnection("localhost:50051")
			if err != nil {
				t.Errorf("goroutine %d got error: %v", idx, err)
				return
			}
			conns[idx] = conn
		}(i)
	}

	wg.Wait()

	// All should have gotten a valid connection
	for i, conn := range conns {
		if conn == nil {
			t.Errorf("goroutine %d got nil connection", i)
		}
	}

	_ = client.Close()
}

func TestGRPCClient_Close_WithConnections(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 30*time.Second)

	// Create some connections
	_, err := client.getConnection("localhost:50051")
	if err != nil {
		t.Fatalf("Failed to create connection 1: %v", err)
	}
	_, err = client.getConnection("localhost:50052")
	if err != nil {
		t.Fatalf("Failed to create connection 2: %v", err)
	}

	// Close should succeed
	err = client.Close()
	if err != nil {
		t.Fatalf("Expected no error closing client, got: %v", err)
	}

	// After close, connections map should be empty
	if len(client.connections) != 0 {
		t.Errorf("Expected empty connections after close, got %d", len(client.connections))
	}
}

func TestGRPCClient_Close_MultipleTimes(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 30*time.Second)

	_, _ = client.getConnection("localhost:50051")

	// First close
	err := client.Close()
	if err != nil {
		t.Fatalf("First close error: %v", err)
	}

	// Second close should be safe (no connections to close)
	err = client.Close()
	if err != nil {
		t.Fatalf("Second close error: %v", err)
	}
}

func TestCalculateChecksum_ZeroTimestamps(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 2,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{
					{Time: 0, Id: "dev-1"},
					{Time: 0, Id: "dev-2"},
				},
			},
		},
	}

	result := calculateChecksum(resp)
	expected := "2:0:0"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestCalculateChecksum_LargeTimestamps(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 2,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{
					{Time: 1700000000000, Id: "dev-1"},
					{Time: 1700000001000, Id: "dev-2"},
				},
			},
		},
	}

	result := calculateChecksum(resp)
	expected := "2:1700000000000:1700000001000"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestCalculateChecksum_NilDataPoints(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 0,
		Results: []*pb.DataPointResult{
			{
				DataPoints: nil,
			},
		},
	}

	// Results exists but has no data points
	result := calculateChecksum(resp)
	// Since resp.Results is not empty but no data points, minTime=0 and maxTime=0
	expected := "0:0:0"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestCalculateChecksum_EmptyDataPointSlice(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 0,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{},
			},
		},
	}

	result := calculateChecksum(resp)
	expected := "0:0:0"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestCalculateChecksum_MultipleResultsMixedEmpty(t *testing.T) {
	resp := &pb.QueryShardResponse{
		Count: 3,
		Results: []*pb.DataPointResult{
			{
				DataPoints: []*pb.DataPoint{}, // empty
			},
			{
				DataPoints: []*pb.DataPoint{
					{Time: 100, Id: "dev-1"},
					{Time: 500, Id: "dev-2"},
				},
			},
			{
				DataPoints: nil, // nil
			},
			{
				DataPoints: []*pb.DataPoint{
					{Time: 200, Id: "dev-3"},
				},
			},
		},
	}

	result := calculateChecksum(resp)
	expected := "3:100:500"
	if result != expected {
		t.Errorf("Expected checksum=%s, got %s", expected, result)
	}
}

func TestNewStreamingSyncClient_NegativeTimeout(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewStreamingSyncClient(logger, -10*time.Second)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	if client.timeout != 30*time.Second {
		t.Errorf("Expected default timeout=30s, got %v", client.timeout)
	}
}

func TestNewStreamingSyncClient_CustomTimeout(t *testing.T) {
	logger := logging.NewDevelopment()

	client := NewStreamingSyncClient(logger, 60*time.Second)
	if client == nil {
		t.Fatal("Expected non-nil client")
	}
	if client.timeout != 60*time.Second {
		t.Errorf("Expected timeout=60s, got %v", client.timeout)
	}
	// Should have embedded GRPCClient
	if client.GRPCClient == nil {
		t.Error("Expected non-nil embedded GRPCClient")
	}
	_ = client.Close()
}

func TestGRPCClient_SyncShard_RPCFailure(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &SyncRequest{
		ShardID:       "shard-1",
		Database:      "testdb",
		Collection:    "testcol",
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		ToTimestamp:   time.Now(),
	}

	// This will connect lazily but fail at RPC call
	pointsCh, errCh := client.SyncShard(ctx, "localhost:19999", req)

	var points []*storage.DataPoint
	for p := range pointsCh {
		points = append(points, p)
	}

	// Should get an error from the channel
	var syncErr error
	select {
	case err := <-errCh:
		syncErr = err
	default:
	}

	if syncErr == nil {
		t.Error("Expected error from SyncShard to non-existent server")
	}
	if len(points) != 0 {
		t.Errorf("Expected 0 points, got %d", len(points))
	}

	_ = client.Close()
}

func TestGRPCClient_GetChecksum_RPCFailure(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &ChecksumRequest{
		ShardID:       "shard-1",
		Database:      "testdb",
		Collection:    "testcol",
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		ToTimestamp:   time.Now(),
	}

	checksum, err := client.GetChecksum(ctx, "localhost:19999", req)
	if err == nil {
		t.Error("Expected error from GetChecksum to non-existent server")
	}
	if checksum != nil {
		t.Errorf("Expected nil checksum on error, got: %v", checksum)
	}

	_ = client.Close()
}

func TestGRPCClient_GetLastTimestamp_RPCFailure(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	lastReq := &LastTimestampRequest{
		Database:   "testdb",
		Collection: "testcol",
	}

	ts, err := client.GetLastTimestamp(ctx, "localhost:19999", lastReq)
	if err == nil {
		t.Error("Expected error from GetLastTimestamp to non-existent server")
	}
	if !ts.IsZero() {
		t.Errorf("Expected zero time on error, got: %v", ts)
	}

	_ = client.Close()
}

func TestGRPCClient_SyncGroup_RPCFailure(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &GroupSyncRequest{
		GroupID:       1,
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		ToTimestamp:   time.Now(),
	}

	pointsCh, errCh := client.SyncGroup(ctx, "localhost:19999", req)

	var points []*storage.DataPoint
	for p := range pointsCh {
		points = append(points, p)
	}

	var syncErr error
	select {
	case err := <-errCh:
		syncErr = err
	default:
	}

	if syncErr == nil {
		t.Error("Expected error from SyncGroup to non-existent server")
	}
	if len(points) != 0 {
		t.Errorf("Expected 0 points, got %d", len(points))
	}

	_ = client.Close()
}

func TestGRPCClient_GetGroupChecksum_RPCFailure(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewGRPCClient(logger, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &GroupChecksumRequest{
		GroupID:       1,
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		ToTimestamp:   time.Now(),
	}

	checksum, err := client.GetGroupChecksum(ctx, "localhost:19999", req)
	if err == nil {
		t.Error("Expected error from GetGroupChecksum to non-existent server")
	}
	if checksum != nil {
		t.Errorf("Expected nil checksum on error, got: %v", checksum)
	}

	_ = client.Close()
}

func TestStreamingSyncClient_SyncShardStreaming_RPCFailure(t *testing.T) {
	logger := logging.NewDevelopment()
	client := NewStreamingSyncClient(logger, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &SyncRequest{
		ShardID:       "shard-1",
		Database:      "testdb",
		Collection:    "testcol",
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		ToTimestamp:   time.Now(),
	}

	pointsCh, errCh := client.SyncShardStreaming(ctx, "localhost:19999", req)

	var points []*storage.DataPoint
	for p := range pointsCh {
		points = append(points, p)
	}

	var syncErr error
	select {
	case err := <-errCh:
		syncErr = err
	default:
	}

	if syncErr == nil {
		t.Error("Expected error from SyncShardStreaming to non-existent server")
	}
	if len(points) != 0 {
		t.Errorf("Expected 0 points, got %d", len(points))
	}

	_ = client.Close()
}
