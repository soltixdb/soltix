package sync

import (
	"context"
	"time"

	"github.com/soltixdb/soltix/internal/storage"
	"github.com/soltixdb/soltix/internal/wal"
)

// ShardInfo represents information about a shard (legacy, kept for compatibility)
type ShardInfo struct {
	ShardID        string
	Database       string
	Collection     string
	PrimaryNode    string
	ReplicaNodes   []string
	TimeRangeStart time.Time
	TimeRangeEnd   time.Time
}

// GroupInfo represents a group and its assigned nodes for sync
// In the 4-tier architecture, sync operates at the group level:
// only replica nodes within the same group sync with each other.
type GroupInfo struct {
	GroupID      int
	PrimaryNode  string
	ReplicaNodes []string
	Databases    []string // databases that have data in this group
	Collections  []string // collections that have data in this group
}

// GetAllNodes returns primary + all replica nodes
func (g *GroupInfo) GetAllNodes() []string {
	nodes := make([]string, 0, 1+len(g.ReplicaNodes))
	nodes = append(nodes, g.PrimaryNode)
	nodes = append(nodes, g.ReplicaNodes...)
	return nodes
}

// NodeInfo represents information about a storage node
type NodeInfo struct {
	ID      string
	Address string
	Status  string
}

// SyncChecksum represents checksum information for a shard or group
type SyncChecksum struct {
	ShardID    string
	GroupID    int
	PointCount int64
	Checksum   string
	FromTime   time.Time
	ToTime     time.Time
}

// SyncProgress represents the progress of a sync operation
type SyncProgress struct {
	ShardID       string
	GroupID       int
	TotalPoints   int64
	SyncedPoints  int64
	PercentDone   float64
	StartedAt     time.Time
	EstimatedDone time.Time
}

// MetadataManager defines the interface for accessing cluster metadata from etcd
type MetadataManager interface {
	// GetMyShards returns all shards that this node is responsible for (legacy)
	GetMyShards(ctx context.Context, nodeID string) ([]ShardInfo, error)

	// GetMyGroups returns all groups that this node is responsible for
	GetMyGroups(ctx context.Context, nodeID string) ([]GroupInfo, error)

	// GetActiveReplicas returns active replica nodes for a shard (excluding the specified node)
	GetActiveReplicas(ctx context.Context, shardID string, excludeNode string) ([]NodeInfo, error)

	// GetGroupReplicas returns active replica nodes for a group (excluding the specified node)
	GetGroupReplicas(ctx context.Context, groupID int, excludeNode string) ([]NodeInfo, error)

	// GetNodeInfo returns information about a specific node
	GetNodeInfo(ctx context.Context, nodeID string) (*NodeInfo, error)

	// GetAllNodes returns all registered nodes
	GetAllNodes(ctx context.Context) ([]NodeInfo, error)
}

// LocalStorage defines the interface for accessing local storage
type LocalStorage interface {
	// GetLastTimestamp returns the last timestamp stored for a shard
	GetLastTimestamp(database, collection string, startTime, endTime time.Time) (time.Time, error)

	// WriteToWAL writes data through WAL for proper durability and replication
	// This follows the same flow as normal push data:
	// WAL -> MemoryStore -> Flush to Storage
	WriteToWAL(entries []*wal.Entry) error

	// Query reads data points from storage
	Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*storage.DataPoint, error)

	// GetChecksum calculates checksum for data in a time range
	GetChecksum(database, collection string, startTime, endTime time.Time) (string, int64, error)
}

// RemoteSyncClient defines the interface for syncing data from remote nodes
type RemoteSyncClient interface {
	// SyncShard streams data points from a remote node for a specific shard (legacy)
	SyncShard(ctx context.Context, nodeAddress string, req *SyncRequest) (<-chan *storage.DataPoint, <-chan error)

	// SyncGroup streams data points from a remote node for a specific group
	SyncGroup(ctx context.Context, nodeAddress string, req *GroupSyncRequest) (<-chan *storage.DataPoint, <-chan error)

	// GetChecksum gets the checksum of a shard from a remote node
	GetChecksum(ctx context.Context, nodeAddress string, req *ChecksumRequest) (*SyncChecksum, error)

	// GetGroupChecksum gets the checksum of a group from a remote node
	GetGroupChecksum(ctx context.Context, nodeAddress string, req *GroupChecksumRequest) (*SyncChecksum, error)

	// GetLastTimestamp gets the last timestamp of a shard from a remote node
	GetLastTimestamp(ctx context.Context, nodeAddress string, req *LastTimestampRequest) (time.Time, error)

	// Close closes the client
	Close() error
}

// SyncRequest represents a request to sync data from a remote node (legacy shard-based)
type SyncRequest struct {
	Database      string
	Collection    string
	ShardID       string
	FromTimestamp time.Time
	ToTimestamp   time.Time
	DeviceIDs     []string // Optional: specific devices to sync
}

// GroupSyncRequest represents a request to sync data for a specific group from a remote node
type GroupSyncRequest struct {
	GroupID       int
	Database      string
	Collection    string
	FromTimestamp time.Time
	ToTimestamp   time.Time
	DeviceIDs     []string // Optional: specific devices to sync within the group
}

// ChecksumRequest represents a request to get checksum from a remote node
type ChecksumRequest struct {
	Database      string
	Collection    string
	ShardID       string
	FromTimestamp time.Time
	ToTimestamp   time.Time
}

// GroupChecksumRequest represents a request to get checksum for a group from a remote node
type GroupChecksumRequest struct {
	GroupID       int
	Database      string
	Collection    string
	FromTimestamp time.Time
	ToTimestamp   time.Time
}

// LastTimestampRequest represents a request to get last timestamp from a remote node
type LastTimestampRequest struct {
	Database   string
	Collection string
	ShardID    string
	GroupID    int
}

// SyncEventType defines types of sync events
type SyncEventType int

const (
	SyncEventStarted SyncEventType = iota
	SyncEventProgress
	SyncEventCompleted
	SyncEventFailed
)

// SyncEvent represents a sync operation event
type SyncEvent struct {
	Type      SyncEventType
	ShardID   string
	GroupID   int
	NodeID    string
	Progress  *SyncProgress
	Error     error
	Timestamp time.Time
}

// SyncEventHandler is a callback for sync events
type SyncEventHandler func(event SyncEvent)
