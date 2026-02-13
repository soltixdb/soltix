package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdMetadataManager implements MetadataManager using etcd
type EtcdMetadataManager struct {
	client *clientv3.Client
	logger *logging.Logger
}

// NewEtcdMetadataManager creates a new etcd-based metadata manager
func NewEtcdMetadataManager(client *clientv3.Client, logger *logging.Logger) *EtcdMetadataManager {
	return &EtcdMetadataManager{
		client: client,
		logger: logger,
	}
}

// GetMyShards returns all shards that this node is responsible for
func (m *EtcdMetadataManager) GetMyShards(ctx context.Context, nodeID string) ([]ShardInfo, error) {
	// First, get node info to find which shards this node holds
	nodeKey := fmt.Sprintf("/soltix/nodes/%s", nodeID)
	resp, err := m.client.Get(ctx, nodeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var nodeInfo models.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &nodeInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node info: %w", err)
	}

	// Get shard details for each shard the node holds
	shards := make([]ShardInfo, 0, len(nodeInfo.Shards))
	for _, shard := range nodeInfo.Shards {
		shardKey := fmt.Sprintf("/soltix/shards/%s/%s/%d", shard.Database, shard.Collection, shard.ShardID)
		shardResp, err := m.client.Get(ctx, shardKey)
		if err != nil {
			m.logger.Warn("Failed to get shard info", "shard_id", shard.ShardID, "error", err)
			continue
		}

		if len(shardResp.Kvs) == 0 {
			continue
		}

		var assignment ShardAssignment
		if err := json.Unmarshal(shardResp.Kvs[0].Value, &assignment); err != nil {
			m.logger.Warn("Failed to unmarshal shard assignment", "shard_id", shard.ShardID, "error", err)
			continue
		}

		shards = append(shards, ShardInfo{
			ShardID:        assignment.ShardID,
			Database:       assignment.Database,
			Collection:     assignment.Collection,
			PrimaryNode:    assignment.PrimaryNode,
			ReplicaNodes:   assignment.ReplicaNodes,
			TimeRangeStart: assignment.TimeRangeStart,
			TimeRangeEnd:   assignment.TimeRangeEnd,
		})
	}

	return shards, nil
}

// GetActiveReplicas returns active replica nodes for a shard (excluding the specified node)
func (m *EtcdMetadataManager) GetActiveReplicas(ctx context.Context, shardID string, excludeNode string) ([]NodeInfo, error) {
	// Get all shards to find the one with matching ID
	shardsResp, err := m.client.Get(ctx, "/soltix/shards/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get shards: %w", err)
	}

	var targetShard *ShardAssignment
	for _, kv := range shardsResp.Kvs {
		var assignment ShardAssignment
		if err := json.Unmarshal(kv.Value, &assignment); err != nil {
			continue
		}
		if assignment.ShardID == shardID {
			targetShard = &assignment
			break
		}
	}

	if targetShard == nil {
		return nil, nil
	}

	// Collect all nodes for this shard
	allNodeIDs := make([]string, 0, len(targetShard.ReplicaNodes)+1)
	allNodeIDs = append(allNodeIDs, targetShard.PrimaryNode)
	allNodeIDs = append(allNodeIDs, targetShard.ReplicaNodes...)

	// Get active nodes
	activeNodes := make([]NodeInfo, 0)
	for _, nodeID := range allNodeIDs {
		if nodeID == excludeNode {
			continue
		}

		nodeInfo, err := m.GetNodeInfo(ctx, nodeID)
		if err != nil {
			m.logger.Warn("Failed to get node info", "node_id", nodeID, "error", err)
			continue
		}

		if nodeInfo != nil && nodeInfo.Status == "active" {
			activeNodes = append(activeNodes, *nodeInfo)
		}
	}

	return activeNodes, nil
}

// GetNodeInfo returns information about a specific node
func (m *EtcdMetadataManager) GetNodeInfo(ctx context.Context, nodeID string) (*NodeInfo, error) {
	nodeKey := fmt.Sprintf("/soltix/nodes/%s", nodeID)
	resp, err := m.client.Get(ctx, nodeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var node models.NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node info: %w", err)
	}

	return &NodeInfo{
		ID:      node.ID,
		Address: node.Address,
		Status:  node.Status,
	}, nil
}

// GetAllNodes returns all registered nodes
func (m *EtcdMetadataManager) GetAllNodes(ctx context.Context) ([]NodeInfo, error) {
	resp, err := m.client.Get(ctx, "/soltix/nodes/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}

	nodes := make([]NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var node models.NodeInfo
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			m.logger.Warn("Failed to unmarshal node", "key", string(kv.Key), "error", err)
			continue
		}

		nodes = append(nodes, NodeInfo{
			ID:      node.ID,
			Address: node.Address,
			Status:  node.Status,
		})
	}

	return nodes, nil
}

// ShardAssignment represents a shard assignment in etcd
// This mirrors the coordinator's ShardAssignment
type ShardAssignment struct {
	ShardID        string    `json:"shard_id"`
	Database       string    `json:"database"`
	Collection     string    `json:"collection"`
	TimeRangeStart time.Time `json:"time_range_start"`
	TimeRangeEnd   time.Time `json:"time_range_end"`
	PrimaryNode    string    `json:"primary_node"`
	ReplicaNodes   []string  `json:"replica_nodes"`
}

// GroupAssignment represents a group assignment in etcd (mirrors coordinator.GroupAssignment)
type GroupAssignment struct {
	GroupID      int      `json:"group_id"`
	PrimaryNode  string   `json:"primary_node"`
	ReplicaNodes []string `json:"replica_nodes"`
	State        string   `json:"state"`
	Epoch        int64    `json:"epoch"`
}

// GetMyGroups returns all groups that this node is responsible for
func (m *EtcdMetadataManager) GetMyGroups(ctx context.Context, nodeID string) ([]GroupInfo, error) {
	resp, err := m.client.Get(ctx, "/soltix/groups/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get groups: %w", err)
	}

	var groups []GroupInfo
	for _, kv := range resp.Kvs {
		var assignment GroupAssignment
		if err := json.Unmarshal(kv.Value, &assignment); err != nil {
			m.logger.Warn("Failed to unmarshal group", "key", string(kv.Key), "error", err)
			continue
		}

		// Check if this node is in the group (primary or replica)
		isInGroup := assignment.PrimaryNode == nodeID
		if !isInGroup {
			for _, replica := range assignment.ReplicaNodes {
				if replica == nodeID {
					isInGroup = true
					break
				}
			}
		}

		if isInGroup {
			groups = append(groups, GroupInfo{
				GroupID:      assignment.GroupID,
				PrimaryNode:  assignment.PrimaryNode,
				ReplicaNodes: assignment.ReplicaNodes,
			})
		}
	}

	return groups, nil
}

// GetGroupReplicas returns active replica nodes for a group (excluding the specified node)
func (m *EtcdMetadataManager) GetGroupReplicas(ctx context.Context, groupID int, excludeNode string) ([]NodeInfo, error) {
	groupKey := fmt.Sprintf("/soltix/groups/%d", groupID)
	resp, err := m.client.Get(ctx, groupKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get group %d: %w", groupID, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var assignment GroupAssignment
	if err := json.Unmarshal(resp.Kvs[0].Value, &assignment); err != nil {
		return nil, fmt.Errorf("failed to unmarshal group %d: %w", groupID, err)
	}

	// Collect all nodes in this group
	allNodeIDs := make([]string, 0, 1+len(assignment.ReplicaNodes))
	allNodeIDs = append(allNodeIDs, assignment.PrimaryNode)
	allNodeIDs = append(allNodeIDs, assignment.ReplicaNodes...)

	// Get active nodes (excluding the requester)
	activeNodes := make([]NodeInfo, 0)
	for _, nid := range allNodeIDs {
		if nid == excludeNode {
			continue
		}

		nodeInfo, err := m.GetNodeInfo(ctx, nid)
		if err != nil {
			m.logger.Warn("Failed to get node info", "node_id", nid, "error", err)
			continue
		}

		if nodeInfo != nil && nodeInfo.Status == "active" {
			activeNodes = append(activeNodes, *nodeInfo)
		}
	}

	return activeNodes, nil
}
