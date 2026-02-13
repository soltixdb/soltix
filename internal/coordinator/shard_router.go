package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// ShardRouter handles shard routing and node selection
type ShardRouter struct {
	logger          *logging.Logger
	metadataManager metadata.Manager
	nodeHasher      NodeHasher
	cfg             config.CoordinatorConfig
	groupManager    *GroupManager
}

// NewShardRouter creates a new ShardRouter instance
func NewShardRouter(logger *logging.Logger, metadataManager metadata.Manager, cfg config.CoordinatorConfig) *ShardRouter {
	sr := &ShardRouter{
		logger:          logger,
		metadataManager: metadataManager,
		nodeHasher:      NewAdaptiveHasher(cfg.HashThreshold, cfg.VNodeCount),
		cfg:             cfg,
		groupManager:    NewGroupManager(logger, metadataManager, cfg),
	}

	// Sync nodes from etcd into hash ring
	if err := sr.syncNodes(); err != nil {
		logger.Warn("Failed to sync nodes from etcd", "error", err)
	}

	return sr
}

// ShardAssignment represents a shard and its assigned nodes
type ShardAssignment struct {
	ShardID        string
	Database       string
	Collection     string
	TimeRangeStart time.Time
	TimeRangeEnd   time.Time
	PrimaryNode    string
	ReplicaNodes   []string
}

// RouteWriteByDevice routes a write to the correct group based on (db, collection, device_id)
func (r *ShardRouter) RouteWriteByDevice(ctx context.Context, database, collection, deviceID string) (*GroupAssignment, error) {
	return r.groupManager.RouteWrite(ctx, database, collection, deviceID)
}

// GetGroupManager returns the underlying GroupManager
func (r *ShardRouter) GetGroupManager() *GroupManager {
	return r.groupManager
}

// syncNodes synchronizes storage nodes from etcd into the consistent hash ring
func (r *ShardRouter) syncNodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodesData, err := r.metadataManager.GetPrefix(ctx, "/soltix/nodes/")
	if err != nil {
		return fmt.Errorf("failed to get nodes from etcd: %w", err)
	}

	// Get current nodes in hasher
	existingNodes := make(map[string]bool)
	for _, nodeID := range r.nodeHasher.GetAllNodes() {
		existingNodes[nodeID] = false // Mark as not seen
	}

	// Add/update nodes from etcd
	for _, value := range nodesData {
		var nodeInfo models.NodeInfo
		if err := json.Unmarshal([]byte(value), &nodeInfo); err != nil {
			continue
		}

		if nodeInfo.Status == "active" {
			if _, exists := existingNodes[nodeInfo.ID]; exists {
				existingNodes[nodeInfo.ID] = true // Mark as seen
			} else {
				// New node, add to hasher
				r.nodeHasher.AddNode(nodeInfo.ID)
				r.logger.Info("Added node to hasher", "node_id", nodeInfo.ID)
			}
		}
	}

	// Remove nodes that are no longer in etcd
	for nodeID, seen := range existingNodes {
		if !seen {
			r.nodeHasher.RemoveNode(nodeID)
			r.logger.Info("Removed node from hasher", "node_id", nodeID)
		}
	}

	// Log current strategy
	if ah, ok := r.nodeHasher.(*AdaptiveHasher); ok {
		r.logger.Debug("Using hashing strategy",
			"strategy", ah.GetCurrentStrategy(),
			"node_count", r.nodeHasher.GetNodeCount())
	}

	return nil
}
