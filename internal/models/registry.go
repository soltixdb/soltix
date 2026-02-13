package models

import "time"

// NodeInfo represents information about a storage node
type NodeInfo struct {
	ID        string      `json:"id"`
	Address   string      `json:"address"` // host:port for gRPC
	Status    string      `json:"status"`  // active, draining, down
	Version   string      `json:"version"` // software version
	Capacity  Capacity    `json:"capacity"`
	Shards    []ShardInfo `json:"shards"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Capacity represents node capacity information
type Capacity struct {
	TotalShards   int   `json:"total_shards"`
	CurrentShards int   `json:"current_shards"`
	DiskTotal     int64 `json:"disk_total"`     // bytes
	DiskUsed      int64 `json:"disk_used"`      // bytes
	DiskAvailable int64 `json:"disk_available"` // bytes
}

// ShardInfo represents information about a shard on this node
type ShardInfo struct {
	ShardID    uint32 `json:"shard_id"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
	Role       string `json:"role"`      // primary, replica
	DataSize   int64  `json:"data_size"` // bytes
}
