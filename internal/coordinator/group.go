package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// GroupState represents the state of a group
type GroupState string

const (
	GroupStateActive      GroupState = "active"
	GroupStateSyncing     GroupState = "syncing"
	GroupStateRebalancing GroupState = "rebalancing"
)

// GroupAssignment represents a group and its assigned nodes
type GroupAssignment struct {
	GroupID      int        `json:"group_id"`
	PrimaryNode  string     `json:"primary_node"`
	ReplicaNodes []string   `json:"replica_nodes"`
	State        GroupState `json:"state"`
	Epoch        int64      `json:"epoch"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// GetAllNodes returns primary + all replica nodes
func (g *GroupAssignment) GetAllNodes() []string {
	nodes := make([]string, 0, 1+len(g.ReplicaNodes))
	nodes = append(nodes, g.PrimaryNode)
	nodes = append(nodes, g.ReplicaNodes...)
	return nodes
}

// GroupManager manages group assignments and routing
type GroupManager struct {
	logger          *logging.Logger
	metadataManager metadata.Manager
	cfg             config.CoordinatorConfig
	nodeHasher      NodeHasher

	// Cache group assignments for fast lookups
	mu     sync.RWMutex
	groups map[int]*GroupAssignment // groupID -> assignment
}

// NewGroupManager creates a new GroupManager
func NewGroupManager(logger *logging.Logger, metadataManager metadata.Manager, cfg config.CoordinatorConfig) *GroupManager {
	gm := &GroupManager{
		logger:          logger,
		metadataManager: metadataManager,
		cfg:             cfg,
		nodeHasher:      NewAdaptiveHasher(cfg.HashThreshold, cfg.VNodeCount),
		groups:          make(map[int]*GroupAssignment),
	}

	// Load existing groups from etcd
	if err := gm.loadGroups(); err != nil {
		logger.Warn("Failed to load groups from etcd", "error", err)
	}

	// Sync nodes from etcd into hash ring
	if err := gm.syncNodes(); err != nil {
		logger.Warn("Failed to sync nodes from etcd", "error", err)
	}

	return gm
}

// CalculateGroupID computes a deterministic group ID from (db, collection, device_id)
func (gm *GroupManager) CalculateGroupID(database, collection, deviceID string) int {
	key := fmt.Sprintf("%s:%s:%s", database, collection, deviceID)
	h := fnv.New32a()
	h.Write([]byte(key))
	totalGroups := gm.cfg.TotalGroups
	if totalGroups <= 0 {
		totalGroups = 256 // Default
	}
	return int(h.Sum32() % uint32(totalGroups))
}

// RouteWrite determines which group and nodes should handle a write for a given device
func (gm *GroupManager) RouteWrite(ctx context.Context, database, collection, deviceID string) (*GroupAssignment, error) {
	groupID := gm.CalculateGroupID(database, collection, deviceID)

	// Check cache first
	gm.mu.RLock()
	group, exists := gm.groups[groupID]
	gm.mu.RUnlock()

	if exists && group.State == GroupStateActive {
		return group, nil
	}

	// Try to load from etcd
	group, err := gm.getGroupFromEtcd(ctx, groupID)
	if err == nil && group != nil {
		gm.mu.Lock()
		gm.groups[groupID] = group
		gm.mu.Unlock()
		return group, nil
	}

	// Group doesn't exist, create it
	return gm.createGroup(ctx, groupID)
}

// GetGroup returns the group assignment for a group ID
func (gm *GroupManager) GetGroup(ctx context.Context, groupID int) (*GroupAssignment, error) {
	gm.mu.RLock()
	group, exists := gm.groups[groupID]
	gm.mu.RUnlock()

	if exists {
		return group, nil
	}

	return gm.getGroupFromEtcd(ctx, groupID)
}

// ListGroups returns all group assignments
func (gm *GroupManager) ListGroups(ctx context.Context) ([]*GroupAssignment, error) {
	prefix := "/soltix/groups/"
	data, err := gm.metadataManager.GetPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}

	groups := make([]*GroupAssignment, 0, len(data))
	for _, value := range data {
		var group GroupAssignment
		if err := json.Unmarshal([]byte(value), &group); err != nil {
			gm.logger.Warn("Failed to unmarshal group", "error", err)
			continue
		}
		groups = append(groups, &group)
	}

	// Sort by group ID
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GroupID < groups[j].GroupID
	})

	return groups, nil
}

// AddNodeToGroup adds a new node to a group's replica set
// The node will be in "syncing" state until data is fully synced
func (gm *GroupManager) AddNodeToGroup(ctx context.Context, groupID int, nodeID string) (*GroupAssignment, error) {
	group, err := gm.GetGroup(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("group %d not found: %w", groupID, err)
	}

	// Check if node is already in the group
	for _, n := range group.GetAllNodes() {
		if n == nodeID {
			return nil, fmt.Errorf("node %s is already in group %d", nodeID, groupID)
		}
	}

	// Add node to replicas
	group.ReplicaNodes = append(group.ReplicaNodes, nodeID)
	group.Epoch++
	group.State = GroupStateSyncing
	group.UpdatedAt = time.Now()

	if err := gm.saveGroup(ctx, group); err != nil {
		return nil, fmt.Errorf("failed to save group: %w", err)
	}

	gm.mu.Lock()
	gm.groups[groupID] = group
	gm.mu.Unlock()

	gm.logger.Info("Added node to group",
		"group_id", groupID,
		"node_id", nodeID,
		"epoch", group.Epoch,
		"total_nodes", len(group.GetAllNodes()))

	return group, nil
}

// RemoveNodeFromGroup removes a node from a group's replica set
func (gm *GroupManager) RemoveNodeFromGroup(ctx context.Context, groupID int, nodeID string) (*GroupAssignment, error) {
	group, err := gm.GetGroup(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("group %d not found: %w", groupID, err)
	}

	// Cannot remove primary node directly - must promote another first
	if group.PrimaryNode == nodeID {
		if len(group.ReplicaNodes) == 0 {
			return nil, fmt.Errorf("cannot remove the only node from group %d", groupID)
		}
		// Promote first replica to primary
		group.PrimaryNode = group.ReplicaNodes[0]
		group.ReplicaNodes = group.ReplicaNodes[1:]
		gm.logger.Info("Promoted new primary for group",
			"group_id", groupID,
			"new_primary", group.PrimaryNode)
	} else {
		// Remove from replicas
		newReplicas := make([]string, 0, len(group.ReplicaNodes)-1)
		found := false
		for _, n := range group.ReplicaNodes {
			if n == nodeID {
				found = true
				continue
			}
			newReplicas = append(newReplicas, n)
		}
		if !found {
			return nil, fmt.Errorf("node %s not found in group %d", nodeID, groupID)
		}
		group.ReplicaNodes = newReplicas
	}

	group.Epoch++
	group.UpdatedAt = time.Now()

	if err := gm.saveGroup(ctx, group); err != nil {
		return nil, fmt.Errorf("failed to save group: %w", err)
	}

	gm.mu.Lock()
	gm.groups[groupID] = group
	gm.mu.Unlock()

	gm.logger.Info("Removed node from group",
		"group_id", groupID,
		"node_id", nodeID,
		"epoch", group.Epoch,
		"remaining_nodes", len(group.GetAllNodes()))

	return group, nil
}

// SetGroupActive marks a group as active (after sync completes)
func (gm *GroupManager) SetGroupActive(ctx context.Context, groupID int) error {
	group, err := gm.GetGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("group %d not found: %w", groupID, err)
	}

	group.State = GroupStateActive
	group.UpdatedAt = time.Now()

	if err := gm.saveGroup(ctx, group); err != nil {
		return fmt.Errorf("failed to save group: %w", err)
	}

	gm.mu.Lock()
	gm.groups[groupID] = group
	gm.mu.Unlock()

	gm.logger.Info("Group marked as active",
		"group_id", groupID,
		"epoch", group.Epoch)

	return nil
}

// UpdateReplicaFactor changes the number of replicas for a specific group
func (gm *GroupManager) UpdateReplicaFactor(ctx context.Context, groupID int, newFactor int) (*GroupAssignment, error) {
	group, err := gm.GetGroup(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("group %d not found: %w", groupID, err)
	}

	currentCount := len(group.GetAllNodes())

	if newFactor < 1 {
		return nil, fmt.Errorf("replica factor must be at least 1")
	}

	if newFactor == currentCount {
		return group, nil // No change needed
	}

	if newFactor > currentCount {
		// Need to add more nodes
		needed := newFactor - currentCount

		// Select new nodes using hasher
		if err := gm.syncNodes(); err != nil {
			return nil, fmt.Errorf("failed to sync nodes: %w", err)
		}

		// Get candidate nodes not already in this group
		existingNodes := make(map[string]bool)
		for _, n := range group.GetAllNodes() {
			existingNodes[n] = true
		}

		allNodes := gm.nodeHasher.GetAllNodes()
		var candidates []string
		for _, n := range allNodes {
			if !existingNodes[n] {
				candidates = append(candidates, n)
			}
		}

		if len(candidates) < needed {
			return nil, fmt.Errorf("not enough available nodes: need %d more, only %d available", needed, len(candidates))
		}

		// Use hash to deterministically select nodes
		groupKey := fmt.Sprintf("group:%d", groupID)
		selectedNodes := gm.nodeHasher.GetNodes(groupKey, newFactor)

		newNodes := make([]string, 0, needed)
		for _, n := range selectedNodes {
			if !existingNodes[n] {
				newNodes = append(newNodes, n)
				if len(newNodes) == needed {
					break
				}
			}
		}

		group.ReplicaNodes = append(group.ReplicaNodes, newNodes...)
		group.State = GroupStateSyncing
		group.Epoch++
		group.UpdatedAt = time.Now()

		gm.logger.Info("Increased replica factor for group",
			"group_id", groupID,
			"old_count", currentCount,
			"new_count", newFactor,
			"new_nodes", newNodes)
	} else {
		// Need to reduce replicas
		excess := currentCount - newFactor
		if excess >= len(group.ReplicaNodes) {
			// Keep only primary
			group.ReplicaNodes = nil
		} else {
			// Remove from end of replica list
			group.ReplicaNodes = group.ReplicaNodes[:len(group.ReplicaNodes)-excess]
		}
		group.Epoch++
		group.UpdatedAt = time.Now()

		gm.logger.Info("Decreased replica factor for group",
			"group_id", groupID,
			"old_count", currentCount,
			"new_count", newFactor)
	}

	if err := gm.saveGroup(ctx, group); err != nil {
		return nil, fmt.Errorf("failed to save group: %w", err)
	}

	gm.mu.Lock()
	gm.groups[groupID] = group
	gm.mu.Unlock()

	return group, nil
}

// GetGroupsForNode returns all groups that a node is part of
func (gm *GroupManager) GetGroupsForNode(ctx context.Context, nodeID string) ([]*GroupAssignment, error) {
	allGroups, err := gm.ListGroups(ctx)
	if err != nil {
		return nil, err
	}

	var result []*GroupAssignment
	for _, group := range allGroups {
		for _, n := range group.GetAllNodes() {
			if n == nodeID {
				result = append(result, group)
				break
			}
		}
	}

	return result, nil
}

// RefreshCache reloads all groups from etcd into cache
func (gm *GroupManager) RefreshCache() error {
	return gm.loadGroups()
}

// --- Internal methods ---

func (gm *GroupManager) getGroupFromEtcd(ctx context.Context, groupID int) (*GroupAssignment, error) {
	key := fmt.Sprintf("/soltix/groups/%d", groupID)
	data, err := gm.metadataManager.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get group %d: %w", groupID, err)
	}
	if data == "" {
		return nil, nil
	}

	var group GroupAssignment
	if err := json.Unmarshal([]byte(data), &group); err != nil {
		return nil, fmt.Errorf("failed to unmarshal group %d: %w", groupID, err)
	}

	return &group, nil
}

func (gm *GroupManager) createGroup(ctx context.Context, groupID int) (*GroupAssignment, error) {
	// Sync nodes from etcd to ensure hasher is up-to-date
	if err := gm.syncNodes(); err != nil {
		return nil, fmt.Errorf("failed to sync nodes: %w", err)
	}

	if gm.nodeHasher.GetNodeCount() == 0 {
		return nil, fmt.Errorf("no storage nodes available")
	}

	// Determine replication factor
	replicationFactor := gm.cfg.ReplicaFactor
	if replicationFactor <= 0 {
		replicationFactor = 3
	}
	if gm.nodeHasher.GetNodeCount() < replicationFactor {
		replicationFactor = gm.nodeHasher.GetNodeCount()
	}

	// Use hash to select nodes for this group
	groupKey := fmt.Sprintf("group:%d", groupID)
	nodes := gm.nodeHasher.GetNodes(groupKey, replicationFactor)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("node hasher returned no nodes for group %d", groupID)
	}

	primaryNode := nodes[0]
	var replicaNodes []string
	if len(nodes) > 1 {
		replicaNodes = nodes[1:]
	}

	group := &GroupAssignment{
		GroupID:      groupID,
		PrimaryNode:  primaryNode,
		ReplicaNodes: replicaNodes,
		State:        GroupStateActive,
		Epoch:        1,
		UpdatedAt:    time.Now(),
	}

	if err := gm.saveGroup(ctx, group); err != nil {
		return nil, fmt.Errorf("failed to save new group %d: %w", groupID, err)
	}

	gm.mu.Lock()
	gm.groups[groupID] = group
	gm.mu.Unlock()

	gm.logger.Info("Created new group",
		"group_id", groupID,
		"primary_node", primaryNode,
		"replica_nodes", replicaNodes)

	return group, nil
}

func (gm *GroupManager) saveGroup(ctx context.Context, group *GroupAssignment) error {
	key := fmt.Sprintf("/soltix/groups/%d", group.GroupID)
	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal group: %w", err)
	}
	return gm.metadataManager.Put(ctx, key, string(data))
}

func (gm *GroupManager) loadGroups() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := gm.metadataManager.GetPrefix(ctx, "/soltix/groups/")
	if err != nil {
		return fmt.Errorf("failed to load groups: %w", err)
	}

	gm.mu.Lock()
	defer gm.mu.Unlock()

	for _, value := range data {
		var group GroupAssignment
		if err := json.Unmarshal([]byte(value), &group); err != nil {
			gm.logger.Warn("Failed to unmarshal group", "error", err)
			continue
		}
		gm.groups[group.GroupID] = &group
	}

	gm.logger.Info("Loaded groups from etcd", "count", len(gm.groups))
	return nil
}

func (gm *GroupManager) syncNodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodesData, err := gm.metadataManager.GetPrefix(ctx, "/soltix/nodes/")
	if err != nil {
		return fmt.Errorf("failed to get nodes from etcd: %w", err)
	}

	existingNodes := make(map[string]bool)
	for _, nodeID := range gm.nodeHasher.GetAllNodes() {
		existingNodes[nodeID] = false
	}

	for _, value := range nodesData {
		var nodeInfo models.NodeInfo
		if err := json.Unmarshal([]byte(value), &nodeInfo); err != nil {
			continue
		}
		if nodeInfo.Status == "active" {
			if _, exists := existingNodes[nodeInfo.ID]; exists {
				existingNodes[nodeInfo.ID] = true
			} else {
				gm.nodeHasher.AddNode(nodeInfo.ID)
			}
		}
	}

	for nodeID, seen := range existingNodes {
		if !seen {
			gm.nodeHasher.RemoveNode(nodeID)
		}
	}

	return nil
}
