package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// ============================================================================
// GroupAutoAssigner
// ============================================================================
//
// Automatically distributes groups to nodes when the cluster membership changes.
//
// Flow:
//  1. Router starts → GroupAutoAssigner.Start(ctx)
//  2. Periodically polls etcd /soltix/nodes/ for active nodes
//  3. When a new node appears (or a node disappears):
//     a. Compute ideal group distribution across all active nodes
//     b. Assign unassigned groups to the least-loaded nodes
//     c. Optionally rebalance overloaded nodes
//  4. Groups are auto-created on first write (RouteWrite), but this ensures
//     every node has a fair share of groups pre-assigned for sync & queries.
//
// ============================================================================

// AutoAssignerConfig configures the automatic group assignment behavior
type AutoAssignerConfig struct {
	// Enabled toggles the auto-assigner on/off
	Enabled bool `mapstructure:"enabled"`

	// PollInterval is how often to check for cluster changes
	PollInterval time.Duration `mapstructure:"poll_interval"`

	// RebalanceOnJoin triggers rebalancing when a new node joins
	RebalanceOnJoin bool `mapstructure:"rebalance_on_join"`

	// RebalanceThreshold is the max group count difference between nodes
	// before rebalancing is triggered (0 = always rebalance)
	RebalanceThreshold int `mapstructure:"rebalance_threshold"`
}

// DefaultAutoAssignerConfig returns sensible defaults
func DefaultAutoAssignerConfig() AutoAssignerConfig {
	return AutoAssignerConfig{
		Enabled:            true,
		PollInterval:       15 * time.Second,
		RebalanceOnJoin:    true,
		RebalanceThreshold: 10,
	}
}

// GroupAutoAssigner watches for cluster membership changes and
// automatically distributes groups across available nodes.
type GroupAutoAssigner struct {
	logger          *logging.Logger
	metadataManager metadata.Manager
	groupManager    *GroupManager
	config          AutoAssignerConfig

	mu          sync.Mutex
	knownNodes  map[string]bool // nodeID -> seen
	lastNodeSet string          // hash of sorted node IDs for change detection

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewGroupAutoAssigner creates a new auto-assigner
func NewGroupAutoAssigner(
	logger *logging.Logger,
	metadataManager metadata.Manager,
	groupManager *GroupManager,
	config AutoAssignerConfig,
) *GroupAutoAssigner {
	return &GroupAutoAssigner{
		logger:          logger,
		metadataManager: metadataManager,
		groupManager:    groupManager,
		config:          config,
		knownNodes:      make(map[string]bool),
		stopCh:          make(chan struct{}),
	}
}

// Start begins watching for node changes in a background goroutine
func (a *GroupAutoAssigner) Start(ctx context.Context) {
	if !a.config.Enabled {
		a.logger.Info("Group auto-assigner is disabled")
		return
	}

	a.logger.Info("Starting group auto-assigner",
		"poll_interval", a.config.PollInterval,
		"rebalance_on_join", a.config.RebalanceOnJoin,
		"rebalance_threshold", a.config.RebalanceThreshold)

	// Do an initial assignment check immediately
	a.wg.Add(1)
	go a.pollLoop(ctx)
}

// Stop stops the auto-assigner
func (a *GroupAutoAssigner) Stop() {
	close(a.stopCh)
	a.wg.Wait()
	a.logger.Info("Group auto-assigner stopped")
}

// pollLoop periodically checks for node membership changes
func (a *GroupAutoAssigner) pollLoop(ctx context.Context) {
	defer a.wg.Done()

	// Initial check after a short delay (let nodes register first)
	select {
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		return
	case <-a.stopCh:
		return
	}

	if err := a.checkAndAssign(ctx); err != nil {
		a.logger.Error("Initial group assignment check failed", "error", err)
	}

	ticker := time.NewTicker(a.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := a.checkAndAssign(ctx); err != nil {
				a.logger.Error("Group assignment check failed", "error", err)
			}
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		}
	}
}

// checkAndAssign checks current cluster state and assigns/rebalances groups
func (a *GroupAutoAssigner) checkAndAssign(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 1. Get current active nodes
	activeNodes, err := a.getActiveNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active nodes: %w", err)
	}

	if len(activeNodes) == 0 {
		a.logger.Debug("No active nodes found, skipping group assignment")
		return nil
	}

	// 2. Check if node set changed
	nodeSetKey := a.buildNodeSetKey(activeNodes)
	if nodeSetKey == a.lastNodeSet {
		return nil // No change
	}

	// Node set changed
	oldNodeSet := a.lastNodeSet
	a.lastNodeSet = nodeSetKey

	// Determine what changed
	newNodes, removedNodes := a.detectChanges(activeNodes)

	if len(newNodes) > 0 {
		a.logger.Info("New nodes detected",
			"new_nodes", newNodes,
			"total_active", len(activeNodes))
	}
	if len(removedNodes) > 0 {
		a.logger.Info("Nodes removed",
			"removed_nodes", removedNodes,
			"total_active", len(activeNodes))
	}

	// Update known nodes
	a.knownNodes = make(map[string]bool)
	for _, n := range activeNodes {
		a.knownNodes[n] = true
	}

	// 3. Assign groups
	if oldNodeSet == "" {
		// First run - do full assignment
		a.logger.Info("First run - performing full group assignment",
			"nodes", len(activeNodes))
		return a.fullAssignment(ctx, activeNodes)
	}

	if len(newNodes) > 0 && a.config.RebalanceOnJoin {
		a.logger.Info("Rebalancing groups for new nodes",
			"new_nodes", newNodes)
		return a.rebalanceForNewNodes(ctx, activeNodes, newNodes)
	}

	if len(removedNodes) > 0 {
		a.logger.Info("Reassigning groups from removed nodes",
			"removed_nodes", removedNodes)
		return a.reassignFromRemovedNodes(ctx, activeNodes, removedNodes)
	}

	return nil
}

// getActiveNodes returns all active node IDs from etcd
func (a *GroupAutoAssigner) getActiveNodes(ctx context.Context) ([]string, error) {
	data, err := a.metadataManager.GetPrefix(ctx, "/soltix/nodes/")
	if err != nil {
		return nil, err
	}

	var nodes []string
	for _, value := range data {
		var nodeInfo models.NodeInfo
		if err := json.Unmarshal([]byte(value), &nodeInfo); err != nil {
			continue
		}
		if nodeInfo.Status == "active" {
			nodes = append(nodes, nodeInfo.ID)
		}
	}

	sort.Strings(nodes)
	return nodes, nil
}

// buildNodeSetKey creates a deterministic string key from sorted node IDs
func (a *GroupAutoAssigner) buildNodeSetKey(nodes []string) string {
	sorted := make([]string, len(nodes))
	copy(sorted, nodes)
	sort.Strings(sorted)
	return fmt.Sprintf("%v", sorted)
}

// detectChanges compares current nodes against known nodes
func (a *GroupAutoAssigner) detectChanges(activeNodes []string) (newNodes, removedNodes []string) {
	activeSet := make(map[string]bool)
	for _, n := range activeNodes {
		activeSet[n] = true
		if !a.knownNodes[n] {
			newNodes = append(newNodes, n)
		}
	}
	for n := range a.knownNodes {
		if !activeSet[n] {
			removedNodes = append(removedNodes, n)
		}
	}
	return
}

// fullAssignment performs initial group assignment across all nodes.
// Groups are created lazily on first write (via RouteWrite), so this
// only pre-distributes existing groups or ensures fairness.
func (a *GroupAutoAssigner) fullAssignment(ctx context.Context, activeNodes []string) error {
	// Get all existing groups
	groups, err := a.groupManager.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list groups: %w", err)
	}

	if len(groups) == 0 {
		a.logger.Info("No groups exist yet, they will be auto-created on first write")
		return nil
	}

	// Count groups per node
	nodeGroupCount := a.countGroupsPerNode(groups)

	// Check for unassigned or orphaned groups
	var orphanedGroups []*GroupAssignment
	for _, g := range groups {
		if !a.isNodeActive(g.PrimaryNode, activeNodes) {
			orphanedGroups = append(orphanedGroups, g)
		}
	}

	if len(orphanedGroups) > 0 {
		a.logger.Info("Found orphaned groups (primary node not active)",
			"count", len(orphanedGroups))
		for _, g := range orphanedGroups {
			if err := a.reassignGroup(ctx, g, activeNodes, nodeGroupCount); err != nil {
				a.logger.Error("Failed to reassign orphaned group",
					"group_id", g.GroupID, "error", err)
			}
		}
	}

	// Ensure all active nodes are replicas for some groups (fill up to ReplicaFactor)
	return a.ensureReplicaCoverage(ctx, groups, activeNodes)
}

// rebalanceForNewNodes adds new nodes to group replica sets to balance load
func (a *GroupAutoAssigner) rebalanceForNewNodes(ctx context.Context, activeNodes, newNodes []string) error {
	groups, err := a.groupManager.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list groups: %w", err)
	}

	if len(groups) == 0 {
		a.logger.Info("No groups to rebalance")
		return nil
	}

	// Count current groups per node
	nodeGroupCount := a.countGroupsPerNode(groups)

	// Calculate ideal groups per node
	replicaFactor := a.groupManager.cfg.ReplicaFactor
	if replicaFactor <= 0 {
		replicaFactor = 3
	}
	totalSlots := len(groups) * replicaFactor
	idealPerNode := totalSlots / len(activeNodes)

	a.logger.Info("Rebalance calculation",
		"total_groups", len(groups),
		"replica_factor", replicaFactor,
		"total_slots", totalSlots,
		"active_nodes", len(activeNodes),
		"ideal_per_node", idealPerNode)

	// Find groups that can accept new replicas (under replica factor)
	for _, g := range groups {
		currentNodes := len(g.GetAllNodes())
		if currentNodes >= replicaFactor {
			continue
		}

		// This group needs more replicas - assign to least loaded new node
		for _, newNode := range newNodes {
			if currentNodes >= replicaFactor {
				break
			}
			if a.nodeInGroup(newNode, g) {
				continue
			}

			_, err := a.groupManager.AddNodeToGroup(ctx, g.GroupID, newNode)
			if err != nil {
				a.logger.Warn("Failed to add new node to group",
					"node_id", newNode, "group_id", g.GroupID, "error", err)
				continue
			}
			nodeGroupCount[newNode]++
			currentNodes++
			a.logger.Debug("Added new node to group",
				"node_id", newNode, "group_id", g.GroupID)
		}
	}

	// If new nodes still have fewer groups than threshold, steal from overloaded
	if a.config.RebalanceThreshold > 0 {
		for _, newNode := range newNodes {
			if nodeGroupCount[newNode] >= idealPerNode {
				continue
			}

			// Find overloaded nodes to steal replicas from
			for _, g := range groups {
				if nodeGroupCount[newNode] >= idealPerNode {
					break
				}
				if a.nodeInGroup(newNode, g) {
					continue
				}

				// Find the most overloaded replica in this group
				overloadedNode := a.findMostOverloaded(g, nodeGroupCount, idealPerNode+a.config.RebalanceThreshold)
				if overloadedNode == "" {
					continue
				}

				// Replace overloaded replica with new node
				if _, err := a.groupManager.RemoveNodeFromGroup(ctx, g.GroupID, overloadedNode); err != nil {
					continue
				}
				nodeGroupCount[overloadedNode]--

				if _, err := a.groupManager.AddNodeToGroup(ctx, g.GroupID, newNode); err != nil {
					// Rollback
					_, _ = a.groupManager.AddNodeToGroup(ctx, g.GroupID, overloadedNode)
					nodeGroupCount[overloadedNode]++
					continue
				}
				nodeGroupCount[newNode]++

				a.logger.Info("Rebalanced group replica",
					"group_id", g.GroupID,
					"from_node", overloadedNode,
					"to_node", newNode)
			}
		}
	}

	return nil
}

// reassignFromRemovedNodes handles groups whose primary or replicas have gone down
func (a *GroupAutoAssigner) reassignFromRemovedNodes(ctx context.Context, activeNodes, removedNodes []string) error {
	groups, err := a.groupManager.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list groups: %w", err)
	}

	removedSet := make(map[string]bool)
	for _, n := range removedNodes {
		removedSet[n] = true
	}

	nodeGroupCount := a.countGroupsPerNode(groups)

	for _, g := range groups {
		needsUpdate := false

		// Check if primary node is removed
		if removedSet[g.PrimaryNode] {
			needsUpdate = true
		}

		// Check if any replica is removed
		for _, r := range g.ReplicaNodes {
			if removedSet[r] {
				needsUpdate = true
				break
			}
		}

		if !needsUpdate {
			continue
		}

		// Remove dead nodes from this group
		for _, rn := range removedNodes {
			if a.nodeInGroup(rn, g) {
				_, err := a.groupManager.RemoveNodeFromGroup(ctx, g.GroupID, rn)
				if err != nil {
					a.logger.Error("Failed to remove dead node from group",
						"node_id", rn, "group_id", g.GroupID, "error", err)
					continue
				}
				nodeGroupCount[rn]--
				a.logger.Info("Removed dead node from group",
					"node_id", rn, "group_id", g.GroupID)
			}
		}

		// Re-fetch group after removals
		updatedGroup, err := a.groupManager.GetGroup(ctx, g.GroupID)
		if err != nil {
			a.logger.Error("Failed to get updated group", "group_id", g.GroupID, "error", err)
			continue
		}

		// Fill back to replica factor
		replicaFactor := a.groupManager.cfg.ReplicaFactor
		if replicaFactor <= 0 {
			replicaFactor = 3
		}

		for len(updatedGroup.GetAllNodes()) < replicaFactor && len(activeNodes) > len(updatedGroup.GetAllNodes()) {
			// Find least loaded active node not in this group
			leastLoaded := a.findLeastLoaded(activeNodes, updatedGroup, nodeGroupCount)
			if leastLoaded == "" {
				break
			}

			result, err := a.groupManager.AddNodeToGroup(ctx, g.GroupID, leastLoaded)
			if err != nil {
				a.logger.Warn("Failed to add replacement node",
					"node_id", leastLoaded, "group_id", g.GroupID, "error", err)
				break
			}
			nodeGroupCount[leastLoaded]++
			updatedGroup = result

			a.logger.Info("Added replacement node to group",
				"node_id", leastLoaded, "group_id", g.GroupID)
		}
	}

	return nil
}

// ensureReplicaCoverage ensures all groups have at least ReplicaFactor nodes
func (a *GroupAutoAssigner) ensureReplicaCoverage(ctx context.Context, groups []*GroupAssignment, activeNodes []string) error {
	replicaFactor := a.groupManager.cfg.ReplicaFactor
	if replicaFactor <= 0 {
		replicaFactor = 3
	}

	nodeGroupCount := a.countGroupsPerNode(groups)

	for _, g := range groups {
		currentCount := len(g.GetAllNodes())
		if currentCount >= replicaFactor {
			continue
		}

		needed := replicaFactor - currentCount
		if needed > len(activeNodes)-currentCount {
			needed = len(activeNodes) - currentCount
		}

		for i := 0; i < needed; i++ {
			leastLoaded := a.findLeastLoaded(activeNodes, g, nodeGroupCount)
			if leastLoaded == "" {
				break
			}

			_, err := a.groupManager.AddNodeToGroup(ctx, g.GroupID, leastLoaded)
			if err != nil {
				a.logger.Warn("Failed to add replica node",
					"node_id", leastLoaded, "group_id", g.GroupID, "error", err)
				continue
			}
			nodeGroupCount[leastLoaded]++

			a.logger.Debug("Added replica to group",
				"node_id", leastLoaded, "group_id", g.GroupID)
		}
	}

	return nil
}

// --- Helper methods ---

func (a *GroupAutoAssigner) countGroupsPerNode(groups []*GroupAssignment) map[string]int {
	counts := make(map[string]int)
	for _, g := range groups {
		for _, n := range g.GetAllNodes() {
			counts[n]++
		}
	}
	return counts
}

func (a *GroupAutoAssigner) isNodeActive(nodeID string, activeNodes []string) bool {
	for _, n := range activeNodes {
		if n == nodeID {
			return true
		}
	}
	return false
}

func (a *GroupAutoAssigner) nodeInGroup(nodeID string, g *GroupAssignment) bool {
	for _, n := range g.GetAllNodes() {
		if n == nodeID {
			return true
		}
	}
	return false
}

func (a *GroupAutoAssigner) findLeastLoaded(activeNodes []string, group *GroupAssignment, nodeGroupCount map[string]int) string {
	var bestNode string
	bestCount := int(^uint(0) >> 1) // max int

	for _, n := range activeNodes {
		if a.nodeInGroup(n, group) {
			continue
		}
		count := nodeGroupCount[n]
		if count < bestCount {
			bestCount = count
			bestNode = n
		}
	}
	return bestNode
}

func (a *GroupAutoAssigner) findMostOverloaded(group *GroupAssignment, nodeGroupCount map[string]int, threshold int) string {
	var worstNode string
	worstCount := 0

	// Only consider replicas (not primary) for removal
	for _, n := range group.ReplicaNodes {
		count := nodeGroupCount[n]
		if count > threshold && count > worstCount {
			worstCount = count
			worstNode = n
		}
	}
	return worstNode
}

func (a *GroupAutoAssigner) reassignGroup(ctx context.Context, group *GroupAssignment, activeNodes []string, nodeGroupCount map[string]int) error {
	// Primary is dead — promote first active replica or assign to least loaded
	promoted := false
	for _, r := range group.ReplicaNodes {
		if a.isNodeActive(r, activeNodes) {
			// Remove old primary and this replica, then re-add properly
			group.PrimaryNode = r
			newReplicas := make([]string, 0)
			for _, rr := range group.ReplicaNodes {
				if rr != r {
					newReplicas = append(newReplicas, rr)
				}
			}
			group.ReplicaNodes = newReplicas
			group.Epoch++
			group.State = GroupStateSyncing
			group.UpdatedAt = time.Now()

			if err := a.groupManager.saveGroup(ctx, group); err != nil {
				return fmt.Errorf("failed to promote replica for group %d: %w", group.GroupID, err)
			}
			a.logger.Info("Promoted replica to primary",
				"group_id", group.GroupID,
				"new_primary", r)
			promoted = true
			break
		}
	}

	if !promoted {
		// No active replicas — assign to least loaded node
		leastLoaded := a.findLeastLoadedFromAll(activeNodes, nodeGroupCount)
		if leastLoaded == "" {
			return fmt.Errorf("no active nodes to assign group %d", group.GroupID)
		}

		group.PrimaryNode = leastLoaded
		group.ReplicaNodes = nil
		group.Epoch++
		group.State = GroupStateSyncing
		group.UpdatedAt = time.Now()

		if err := a.groupManager.saveGroup(ctx, group); err != nil {
			return fmt.Errorf("failed to reassign group %d: %w", group.GroupID, err)
		}
		nodeGroupCount[leastLoaded]++

		a.logger.Info("Reassigned orphaned group",
			"group_id", group.GroupID,
			"new_primary", leastLoaded)
	}

	return nil
}

func (a *GroupAutoAssigner) findLeastLoadedFromAll(activeNodes []string, nodeGroupCount map[string]int) string {
	var bestNode string
	bestCount := int(^uint(0) >> 1)

	for _, n := range activeNodes {
		count := nodeGroupCount[n]
		if count < bestCount {
			bestCount = count
			bestNode = n
		}
	}
	return bestNode
}
