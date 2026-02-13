package coordinator

import "sync"

// NodeHasher defines the interface for node hashing strategies
type NodeHasher interface {
	// AddNode adds a node to the hasher
	AddNode(nodeID string)

	// RemoveNode removes a node from the hasher
	RemoveNode(nodeID string)

	// GetNode returns the primary node for a given key
	GetNode(key string) string

	// GetNodes returns N nodes for a given key (for replication)
	GetNodes(key string, count int) []string

	// GetNodeCount returns the number of nodes in the hasher
	GetNodeCount() int

	// GetAllNodes returns all node IDs
	GetAllNodes() []string

	// Clear removes all nodes
	Clear()
}

// AdaptiveHasher automatically switches between Rendezvous and Consistent hashing
// based on the number of nodes in the cluster.
//
// Optimization: lazy initialization — only builds the ConsistentHash ring when
// the node count crosses the threshold. Add/Remove always update RendezvousHash
// (O(1)), but ConsistentHash is only synced on strategy switch.
type AdaptiveHasher struct {
	mu              sync.RWMutex
	threshold       int // Node count threshold to switch strategies
	vnodeCount      int // vnodes per node for consistent hash
	rendezvousHash  *RendezvousHash
	consistentHash  *ConsistentHash
	currentStrategy string // "rendezvous" or "consistent"
	needsSync       bool   // ConsistentHash needs rebuild from RendezvousHash
}

// NewAdaptiveHasher creates a new adaptive hasher
// threshold: node count to switch from rendezvous to consistent (default: 20)
// vnodeCount: virtual nodes per physical node for consistent hash (default: 200)
func NewAdaptiveHasher(threshold, vnodeCount int) *AdaptiveHasher {
	if threshold <= 0 {
		threshold = 20 // Default threshold
	}
	if vnodeCount <= 0 {
		vnodeCount = 200
	}

	ah := &AdaptiveHasher{
		threshold:       threshold,
		vnodeCount:      vnodeCount,
		rendezvousHash:  NewRendezvousHash(),
		consistentHash:  NewConsistentHash(vnodeCount),
		currentStrategy: "rendezvous",
	}

	return ah
}

// selectStrategy determines which strategy to use based on node count.
// When switching to consistent hash, performs a one-time bulk sync from
// rendezvous hash if needed (lazy initialization).
func (ah *AdaptiveHasher) selectStrategy() NodeHasher {
	nodeCount := ah.rendezvousHash.GetNodeCount()

	if nodeCount < ah.threshold {
		ah.currentStrategy = "rendezvous"
		return ah.rendezvousHash
	}

	// Need consistent hash — sync if dirty
	if ah.needsSync {
		ah.consistentHash.Clear()
		for _, node := range ah.rendezvousHash.GetAllNodes() {
			ah.consistentHash.AddNode(node)
		}
		ah.needsSync = false
	}

	ah.currentStrategy = "consistent"
	return ah.consistentHash
}

// GetCurrentStrategy returns the currently active strategy name
func (ah *AdaptiveHasher) GetCurrentStrategy() string {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.selectStrategy() // Update strategy based on current node count
	return ah.currentStrategy
}

// AddNode adds a node. Always updates RendezvousHash (O(1)).
// If currently using ConsistentHash, also adds to it directly.
// Otherwise marks needsSync for deferred bulk rebuild on strategy switch.
func (ah *AdaptiveHasher) AddNode(nodeID string) {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.rendezvousHash.AddNode(nodeID)

	if ah.currentStrategy == "consistent" {
		ah.consistentHash.AddNode(nodeID)
	} else {
		ah.needsSync = true
	}
}

// RemoveNode removes a node. Always updates RendezvousHash (O(1)).
// If currently using ConsistentHash, also removes from it directly.
// Otherwise marks needsSync for deferred bulk rebuild on strategy switch.
func (ah *AdaptiveHasher) RemoveNode(nodeID string) {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.rendezvousHash.RemoveNode(nodeID)

	if ah.currentStrategy == "consistent" {
		ah.consistentHash.RemoveNode(nodeID)
	} else {
		ah.needsSync = true
	}
}

// GetNode returns the primary node using the appropriate strategy
func (ah *AdaptiveHasher) GetNode(key string) string {
	ah.mu.Lock()
	strategy := ah.selectStrategy()
	ah.mu.Unlock()
	return strategy.GetNode(key)
}

// GetNodes returns N nodes using the appropriate strategy
func (ah *AdaptiveHasher) GetNodes(key string, count int) []string {
	ah.mu.Lock()
	strategy := ah.selectStrategy()
	ah.mu.Unlock()
	return strategy.GetNodes(key, count)
}

// GetNodeCount returns the number of nodes
func (ah *AdaptiveHasher) GetNodeCount() int {
	ah.mu.RLock()
	defer ah.mu.RUnlock()
	return ah.rendezvousHash.GetNodeCount()
}

// GetAllNodes returns all node IDs
func (ah *AdaptiveHasher) GetAllNodes() []string {
	ah.mu.RLock()
	defer ah.mu.RUnlock()
	return ah.rendezvousHash.GetAllNodes()
}

// Clear removes all nodes from both hashers
func (ah *AdaptiveHasher) Clear() {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.rendezvousHash.Clear()
	ah.consistentHash.Clear()
	ah.currentStrategy = "rendezvous"
	ah.needsSync = false
}
