package coordinator

import (
	"hash/fnv"
	"sort"
	"sync"
)

// RendezvousHash implements rendezvous hashing (HRW - Highest Random Weight).
// Best for small clusters (< 20 nodes) due to O(N) lookup complexity.
//
// Optimizations:
//   - sync.RWMutex for thread safety (was missing, concurrent map writes would panic)
//   - Cached nodeList slice rebuilt on Add/Remove (avoids map iteration overhead)
//   - GetNode fast path for count=1: O(N) max scan, no sort, no slice allocation
//   - Reuse FNV hasher via Reset() in GetNodes
type RendezvousHash struct {
	mu       sync.RWMutex
	nodes    map[string]bool
	nodeList []string // cached sorted list, rebuilt on Add/Remove
}

// NewRendezvousHash creates a new rendezvous hash instance
func NewRendezvousHash() *RendezvousHash {
	return &RendezvousHash{
		nodes: make(map[string]bool),
	}
}

// AddNode adds a node to the pool
func (rh *RendezvousHash) AddNode(nodeID string) {
	rh.mu.Lock()
	defer rh.mu.Unlock()

	if rh.nodes[nodeID] {
		return
	}
	rh.nodes[nodeID] = true
	rh.rebuildNodeList()
}

// RemoveNode removes a node from the pool
func (rh *RendezvousHash) RemoveNode(nodeID string) {
	rh.mu.Lock()
	defer rh.mu.Unlock()

	delete(rh.nodes, nodeID)
	rh.rebuildNodeList()
}

// rebuildNodeList rebuilds the cached node list from the map.
// Must be called with mu held.
func (rh *RendezvousHash) rebuildNodeList() {
	rh.nodeList = rh.nodeList[:0]
	for nodeID := range rh.nodes {
		rh.nodeList = append(rh.nodeList, nodeID)
	}
	// Sort for deterministic iteration order
	sort.Strings(rh.nodeList)
}

// GetNode returns the node with highest weight for the given key.
// Fast path: O(N) scan for max weight — no sort, no slice allocation.
func (rh *RendezvousHash) GetNode(key string) string {
	rh.mu.RLock()
	defer rh.mu.RUnlock()

	if len(rh.nodeList) == 0 {
		return ""
	}

	h := fnv.New64a()
	keyBytes := []byte(key)
	sep := []byte{':'}

	var maxNode string
	var maxWeight uint64

	for _, nodeID := range rh.nodeList {
		h.Reset()
		h.Write(keyBytes)
		h.Write(sep)
		h.Write([]byte(nodeID))
		w := h.Sum64()

		if maxNode == "" || w > maxWeight {
			maxWeight = w
			maxNode = nodeID
		}
	}

	return maxNode
}

// GetNodes returns N nodes with highest weights for the given key.
// Uses cached nodeList for deterministic iteration, reuses FNV hasher.
func (rh *RendezvousHash) GetNodes(key string, count int) []string {
	rh.mu.RLock()
	defer rh.mu.RUnlock()

	if len(rh.nodeList) == 0 {
		return []string{}
	}

	type nodeWeight struct {
		node   string
		weight uint64
	}

	// Reuse single hasher
	h := fnv.New64a()
	keyBytes := []byte(key)
	sep := []byte{':'}

	// Calculate weight for each node using cached nodeList
	weights := make([]nodeWeight, len(rh.nodeList))
	for i, nodeID := range rh.nodeList {
		h.Reset()
		h.Write(keyBytes)
		h.Write(sep)
		h.Write([]byte(nodeID))
		weights[i] = nodeWeight{nodeID, h.Sum64()}
	}

	// Sort by weight descending (highest first)
	sort.Slice(weights, func(i, j int) bool {
		return weights[i].weight > weights[j].weight
	})

	// Return top N nodes
	resultCount := count
	if resultCount > len(weights) {
		resultCount = len(weights)
	}

	result := make([]string, resultCount)
	for i := 0; i < resultCount; i++ {
		result[i] = weights[i].node
	}

	return result
}

// GetNodeCount returns the number of nodes
func (rh *RendezvousHash) GetNodeCount() int {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return len(rh.nodes)
}

// GetAllNodes returns all node IDs (sorted)
func (rh *RendezvousHash) GetAllNodes() []string {
	rh.mu.RLock()
	defer rh.mu.RUnlock()

	result := make([]string, len(rh.nodeList))
	copy(result, rh.nodeList)
	return result
}

// Clear removes all nodes
func (rh *RendezvousHash) Clear() {
	rh.mu.Lock()
	defer rh.mu.Unlock()

	rh.nodes = make(map[string]bool)
	rh.nodeList = rh.nodeList[:0]
}
