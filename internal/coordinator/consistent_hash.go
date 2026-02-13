package coordinator

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHash implements consistent hashing with virtual nodes.
//
// Optimizations over naive implementation:
//   - AddNode: reuses FNV hasher via Reset(), strconv.AppendInt replaces fmt.Sprintf,
//     pre-grows ring, binary insert maintains sorted order (no full re-sort)
//   - RemoveNode: tracks vnodes per node for O(V log N) targeted removal,
//     in-place shrink (zero allocation)
//   - GetNode: fast path for single node — no seen map, no slice allocation
type ConsistentHash struct {
	mu         sync.RWMutex
	ring       []uint32            // Sorted array of vnode hashes
	vnodeMap   map[uint32]string   // vnode hash → physical node ID
	nodes      map[string]bool     // Track physical nodes
	nodeVnodes map[string][]uint32 // nodeID → vnode hashes (for fast remove)
	vnodeCount int                 // Number of virtual nodes per physical node
}

// NewConsistentHash creates a new consistent hash ring
func NewConsistentHash(vnodeCount int) *ConsistentHash {
	if vnodeCount <= 0 {
		vnodeCount = 200 // Default: 200 virtual nodes per physical node
	}

	return &ConsistentHash{
		ring:       make([]uint32, 0),
		vnodeMap:   make(map[uint32]string),
		nodes:      make(map[string]bool),
		nodeVnodes: make(map[string][]uint32),
		vnodeCount: vnodeCount,
	}
}

// hash generates a 32-bit hash for a given key
func (ch *ConsistentHash) hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// AddNode adds a physical node to the hash ring.
//
// Optimizations:
//   - Reuses single FNV hasher via Reset() — eliminates V hasher allocations
//   - strconv.AppendInt with shared buffer — eliminates V fmt.Sprintf allocations
//   - Pre-grows ring slice capacity
//   - Binary insert maintains sorted order — avoids O(N log N) full sort
//   - Tracks vnodes per node in nodeVnodes for fast RemoveNode
func (ch *ConsistentHash) AddNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[nodeID] {
		return // Node already exists
	}

	// Pre-allocate ring capacity
	if cap(ch.ring)-len(ch.ring) < ch.vnodeCount {
		newRing := make([]uint32, len(ch.ring), len(ch.ring)+ch.vnodeCount)
		copy(newRing, ch.ring)
		ch.ring = newRing
	}

	// Reuse hasher + buffer to avoid per-vnode allocations
	h := fnv.New32a()
	buf := make([]byte, 0, len(nodeID)+10)
	buf = append(buf, nodeID...)
	buf = append(buf, ':')
	prefixLen := len(buf)

	vnodes := make([]uint32, 0, ch.vnodeCount)

	for i := 0; i < ch.vnodeCount; i++ {
		buf = buf[:prefixLen]
		buf = strconv.AppendInt(buf, int64(i), 10)

		h.Reset()
		h.Write(buf)
		vnodeHash := h.Sum32()

		// Binary insert to maintain sorted order
		idx := sort.Search(len(ch.ring), func(j int) bool {
			return ch.ring[j] >= vnodeHash
		})
		ch.ring = append(ch.ring, 0)
		copy(ch.ring[idx+1:], ch.ring[idx:])
		ch.ring[idx] = vnodeHash

		ch.vnodeMap[vnodeHash] = nodeID
		vnodes = append(vnodes, vnodeHash)
	}

	ch.nodes[nodeID] = true
	ch.nodeVnodes[nodeID] = vnodes
}

// RemoveNode removes a physical node from the hash ring.
//
// Optimizations:
//   - Uses nodeVnodes for direct vnode hash lookup — O(V) instead of O(N) scan
//   - Binary search for each vnode position — O(V × log N) total
//   - In-place removal via copy — zero slice allocation
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[nodeID] {
		return // Node doesn't exist
	}

	vnodes := ch.nodeVnodes[nodeID]

	// Sort vnodes descending so removal from back to front
	// doesn't invalidate earlier indices
	sort.Slice(vnodes, func(i, j int) bool {
		return vnodes[i] > vnodes[j]
	})

	for _, vh := range vnodes {
		idx := sort.Search(len(ch.ring), func(i int) bool {
			return ch.ring[i] >= vh
		})
		if idx < len(ch.ring) && ch.ring[idx] == vh {
			// In-place remove: shift left, shrink slice
			ch.ring = append(ch.ring[:idx], ch.ring[idx+1:]...)
		}
		delete(ch.vnodeMap, vh)
	}

	delete(ch.nodes, nodeID)
	delete(ch.nodeVnodes, nodeID)
}

// GetNode returns the primary node for a given key.
// Fast path: binary search + direct lookup — no seen map, no slice allocation.
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	keyHash := ch.hash(key)

	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= keyHash
	})
	if idx >= len(ch.ring) {
		idx = 0
	}

	return ch.vnodeMap[ch.ring[idx]]
}

// GetNodes returns N distinct physical nodes for a given key (for replication)
func (ch *ConsistentHash) GetNodes(key string, count int) []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return []string{}
	}

	keyHash := ch.hash(key)

	// Binary search to find the first vnode >= keyHash
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= keyHash
	})

	// Wrap around if we've gone past the end of the ring
	if idx >= len(ch.ring) {
		idx = 0
	}

	// Collect distinct physical nodes
	seen := make(map[string]bool, count)
	result := make([]string, 0, count)

	// Walk the ring clockwise to find N distinct physical nodes
	attempts := 0
	maxAttempts := len(ch.ring) // Prevent infinite loop

	for len(result) < count && attempts < maxAttempts {
		vnodeHash := ch.ring[idx]
		nodeID := ch.vnodeMap[vnodeHash]

		if !seen[nodeID] {
			result = append(result, nodeID)
			seen[nodeID] = true
		}

		idx++
		if idx >= len(ch.ring) {
			idx = 0 // Wrap around
		}
		attempts++
	}

	return result
}

// GetNodeCount returns the number of physical nodes in the ring
func (ch *ConsistentHash) GetNodeCount() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.nodes)
}

// GetAllNodes returns all physical node IDs
func (ch *ConsistentHash) GetAllNodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	nodes := make([]string, 0, len(ch.nodes))
	for nodeID := range ch.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// Clear removes all nodes from the ring
func (ch *ConsistentHash) Clear() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.ring = ch.ring[:0]
	ch.vnodeMap = make(map[uint32]string)
	ch.nodes = make(map[string]bool)
	ch.nodeVnodes = make(map[string][]uint32)
}
