package sync

import (
	"testing"
	"time"
)

func TestGroupInfo_GetAllNodes(t *testing.T) {
	tests := []struct {
		name     string
		group    GroupInfo
		expected []string
	}{
		{
			name: "primary with replicas",
			group: GroupInfo{
				GroupID:      1,
				PrimaryNode:  "node-1",
				ReplicaNodes: []string{"node-2", "node-3"},
			},
			expected: []string{"node-1", "node-2", "node-3"},
		},
		{
			name: "primary only no replicas",
			group: GroupInfo{
				GroupID:      2,
				PrimaryNode:  "node-1",
				ReplicaNodes: []string{},
			},
			expected: []string{"node-1"},
		},
		{
			name: "primary with nil replicas",
			group: GroupInfo{
				GroupID:     3,
				PrimaryNode: "node-1",
			},
			expected: []string{"node-1"},
		},
		{
			name: "primary with single replica",
			group: GroupInfo{
				GroupID:      4,
				PrimaryNode:  "primary",
				ReplicaNodes: []string{"replica-1"},
			},
			expected: []string{"primary", "replica-1"},
		},
		{
			name: "empty primary with replicas",
			group: GroupInfo{
				GroupID:      5,
				PrimaryNode:  "",
				ReplicaNodes: []string{"node-2"},
			},
			expected: []string{"", "node-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.group.GetAllNodes()
			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d nodes, got %d", len(tt.expected), len(result))
			}
			for i, node := range result {
				if node != tt.expected[i] {
					t.Errorf("Node[%d]: expected %q, got %q", i, tt.expected[i], node)
				}
			}
		})
	}
}

func TestGroupInfo_GetAllNodes_OrderPreserved(t *testing.T) {
	group := GroupInfo{
		PrimaryNode:  "primary",
		ReplicaNodes: []string{"z-node", "a-node", "m-node"},
	}

	nodes := group.GetAllNodes()
	if nodes[0] != "primary" {
		t.Errorf("First node should be primary, got %s", nodes[0])
	}
	if nodes[1] != "z-node" {
		t.Errorf("Second node should be z-node, got %s", nodes[1])
	}
	if nodes[2] != "a-node" {
		t.Errorf("Third node should be a-node, got %s", nodes[2])
	}
	if nodes[3] != "m-node" {
		t.Errorf("Fourth node should be m-node, got %s", nodes[3])
	}
}

func TestShardInfo_Fields(t *testing.T) {
	now := time.Now()
	shard := ShardInfo{
		ShardID:        "shard-1",
		Database:       "testdb",
		Collection:     "testcol",
		PrimaryNode:    "node-1",
		ReplicaNodes:   []string{"node-2", "node-3"},
		TimeRangeStart: now.Add(-24 * time.Hour),
		TimeRangeEnd:   now,
	}

	if shard.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if shard.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
	if shard.Collection != "testcol" {
		t.Errorf("Collection mismatch")
	}
	if shard.PrimaryNode != "node-1" {
		t.Errorf("PrimaryNode mismatch")
	}
	if len(shard.ReplicaNodes) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(shard.ReplicaNodes))
	}
}

func TestNodeInfo_Fields(t *testing.T) {
	node := NodeInfo{
		ID:      "node-1",
		Address: "localhost:5555",
		Status:  "active",
	}

	if node.ID != "node-1" {
		t.Errorf("ID mismatch")
	}
	if node.Address != "localhost:5555" {
		t.Errorf("Address mismatch")
	}
	if node.Status != "active" {
		t.Errorf("Status mismatch")
	}
}

func TestSyncChecksum_Fields(t *testing.T) {
	now := time.Now()
	checksum := SyncChecksum{
		ShardID:    "shard-1",
		GroupID:    5,
		PointCount: 1000,
		Checksum:   "abc123",
		FromTime:   now.Add(-1 * time.Hour),
		ToTime:     now,
	}

	if checksum.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if checksum.GroupID != 5 {
		t.Errorf("GroupID mismatch")
	}
	if checksum.PointCount != 1000 {
		t.Errorf("PointCount mismatch")
	}
	if checksum.Checksum != "abc123" {
		t.Errorf("Checksum mismatch")
	}
}

func TestSyncProgress_Fields(t *testing.T) {
	now := time.Now()
	progress := SyncProgress{
		ShardID:       "shard-1",
		GroupID:       3,
		TotalPoints:   5000,
		SyncedPoints:  2500,
		PercentDone:   50.0,
		StartedAt:     now,
		EstimatedDone: now.Add(10 * time.Minute),
	}

	if progress.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if progress.GroupID != 3 {
		t.Errorf("GroupID mismatch")
	}
	if progress.TotalPoints != 5000 {
		t.Errorf("TotalPoints mismatch")
	}
	if progress.SyncedPoints != 2500 {
		t.Errorf("SyncedPoints mismatch")
	}
	if progress.PercentDone != 50.0 {
		t.Errorf("PercentDone mismatch")
	}
}

func TestSyncEventType_Constants(t *testing.T) {
	if SyncEventStarted != 0 {
		t.Errorf("SyncEventStarted should be 0, got %d", SyncEventStarted)
	}
	if SyncEventProgress != 1 {
		t.Errorf("SyncEventProgress should be 1, got %d", SyncEventProgress)
	}
	if SyncEventCompleted != 2 {
		t.Errorf("SyncEventCompleted should be 2, got %d", SyncEventCompleted)
	}
	if SyncEventFailed != 3 {
		t.Errorf("SyncEventFailed should be 3, got %d", SyncEventFailed)
	}
}

func TestSyncEvent_Fields(t *testing.T) {
	now := time.Now()
	progress := &SyncProgress{ShardID: "shard-1"}
	event := SyncEvent{
		Type:      SyncEventStarted,
		ShardID:   "shard-1",
		GroupID:   1,
		NodeID:    "node-1",
		Progress:  progress,
		Error:     nil,
		Timestamp: now,
	}

	if event.Type != SyncEventStarted {
		t.Errorf("Type mismatch")
	}
	if event.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if event.GroupID != 1 {
		t.Errorf("GroupID mismatch")
	}
	if event.NodeID != "node-1" {
		t.Errorf("NodeID mismatch")
	}
	if event.Progress != progress {
		t.Errorf("Progress mismatch")
	}
	if event.Error != nil {
		t.Errorf("Error should be nil")
	}
}

func TestSyncRequest_Fields(t *testing.T) {
	now := time.Now()
	req := SyncRequest{
		Database:      "testdb",
		Collection:    "testcol",
		ShardID:       "shard-1",
		FromTimestamp: now.Add(-1 * time.Hour),
		ToTimestamp:   now,
		DeviceIDs:     []string{"dev-1", "dev-2"},
	}

	if req.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
	if req.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if len(req.DeviceIDs) != 2 {
		t.Errorf("Expected 2 DeviceIDs, got %d", len(req.DeviceIDs))
	}
}

func TestGroupSyncRequest_Fields(t *testing.T) {
	now := time.Now()
	req := GroupSyncRequest{
		GroupID:       5,
		Database:      "testdb",
		Collection:    "testcol",
		FromTimestamp: now.Add(-2 * time.Hour),
		ToTimestamp:   now,
		DeviceIDs:     []string{"dev-1"},
	}

	if req.GroupID != 5 {
		t.Errorf("GroupID mismatch")
	}
	if req.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
	if len(req.DeviceIDs) != 1 {
		t.Errorf("Expected 1 DeviceID, got %d", len(req.DeviceIDs))
	}
}

func TestChecksumRequest_Fields(t *testing.T) {
	now := time.Now()
	req := ChecksumRequest{
		Database:      "testdb",
		Collection:    "testcol",
		ShardID:       "shard-1",
		FromTimestamp: now.Add(-1 * time.Hour),
		ToTimestamp:   now,
	}

	if req.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
	if req.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
}

func TestGroupChecksumRequest_Fields(t *testing.T) {
	now := time.Now()
	req := GroupChecksumRequest{
		GroupID:       3,
		Database:      "testdb",
		Collection:    "testcol",
		FromTimestamp: now.Add(-1 * time.Hour),
		ToTimestamp:   now,
	}

	if req.GroupID != 3 {
		t.Errorf("GroupID mismatch")
	}
	if req.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
}

func TestLastTimestampRequest_Fields(t *testing.T) {
	req := LastTimestampRequest{
		Database:   "testdb",
		Collection: "testcol",
		ShardID:    "shard-1",
		GroupID:    2,
	}

	if req.Database != "testdb" {
		t.Errorf("Database mismatch")
	}
	if req.Collection != "testcol" {
		t.Errorf("Collection mismatch")
	}
	if req.ShardID != "shard-1" {
		t.Errorf("ShardID mismatch")
	}
	if req.GroupID != 2 {
		t.Errorf("GroupID mismatch")
	}
}

func TestBuildInfo(t *testing.T) {
	info := BuildInfo()

	if info == nil {
		t.Fatal("BuildInfo should not return nil")
	}

	version, ok := info["version"]
	if !ok {
		t.Error("BuildInfo should contain 'version' key")
	}
	if version != Version {
		t.Errorf("Expected version=%s, got %s", Version, version)
	}

	pkg, ok := info["package"]
	if !ok {
		t.Error("BuildInfo should contain 'package' key")
	}
	if pkg != "github.com/soltixdb/soltix/internal/sync" {
		t.Errorf("Unexpected package: %s", pkg)
	}
}

func TestVersion_NotEmpty(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
	if Version != "1.0.0" {
		t.Errorf("Expected Version=1.0.0, got %s", Version)
	}
}

func TestSyncEventHandler_Type(t *testing.T) {
	var handler SyncEventHandler = func(event SyncEvent) {
		// no-op
	}

	// Verify handler is callable
	handler(SyncEvent{
		Type:      SyncEventStarted,
		Timestamp: time.Now(),
	})
}
