package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const DefaultVnodesPerNode = 150

type vnode struct {
	hash uint64
	nodeID string
}

// Ring is a consistent hash ring with virtual nodes.
// Adding/removing one node remaps ~1/N keys instead of ~(N-1)/N.
// With 150 vnodes per node, distribution variance is ~±5%.
type Ring struct {
	mu sync.RWMutex
	vnodes []vnode
	nodes map[string]struct{}
	vnodesPer int
}

func NewRing(vnodesPerNode int) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = DefaultVnodesPerNode
	}
	return &Ring{
		nodes:     make(map[string]struct{}),
		vnodesPer: vnodesPerNode,
	}
}

func (r *Ring) Add(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.nodes[nodeID]; exists {
		return
	}
	r.nodes[nodeID] = struct{}{}
	for i := 0; i < r.vnodesPer; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", nodeID, i))
		r.vnodes = append(r.vnodes, vnode{hash: h, nodeID: nodeID})
	}
	sort.Slice(r.vnodes, func(i, j int) bool { return r.vnodes[i].hash < r.vnodes[j].hash })
}

func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
	filtered := r.vnodes[:0]
	for _, v := range r.vnodes {
		if v.nodeID != nodeID {
			filtered = append(filtered, v)
		}
	}
	r.vnodes = filtered
}

func (r *Ring) Lookup(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.vnodes) == 0 {
		return "", fmt.Errorf("ring is empty")
	}
	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].hash >= h })
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].nodeID, nil
}

func (r *Ring) LookupN(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.vnodes) == 0 {
		return nil, fmt.Errorf("ring is empty")
	}
	h := hashKey(key)
	start := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].hash >= h })
	seen := make(map[string]struct{})
	var result []string
	for i := 0; i < len(r.vnodes) && len(result) < n; i++ {
		idx := (start + i) % len(r.vnodes)
		id := r.vnodes[idx].nodeID
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result, nil
}

func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (r *Ring) Distribution() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dist := make(map[string]int)
	for _, v := range r.vnodes {
		dist[v.nodeID]++
	}
	return dist
}

func hashKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}
