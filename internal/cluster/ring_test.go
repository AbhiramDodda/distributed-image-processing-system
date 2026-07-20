package cluster_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
)

func TestRing_emptyLookupReturnsError(t *testing.T) {
	r := cluster.NewRing(150)
	_, err := r.Lookup("anykey")
	if err == nil {
		t.Fatal("empty ring Lookup() should return error")
	}
}

func TestRing_singleNodeReceivesAllKeys(t *testing.T) {
	r := cluster.NewRing(150)
	r.Add("node-1")
	for i := 0; i < 200; i++ {
		node, err := r.Lookup(fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("Lookup returned error with one node: %v", err)
		}
		if node != "node-1" {
			t.Fatalf("Lookup() = %q, want node-1", node)
		}
	}
}

func TestRing_allNodesReceiveKeys(t *testing.T) {
	r := cluster.NewRing(150)
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	for _, n := range nodes {
		r.Add(n)
	}
	hits := make(map[string]int)
	const samples = 10000
	for i := 0; i < samples; i++ {
		node, err := r.Lookup(fmt.Sprintf("shard-%04d", i))
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		hits[node]++
	}
	for _, n := range nodes {
		if hits[n] == 0 {
			t.Errorf("node %q received 0 keys in %d samples", n, samples)
		}
	}
	if len(hits) != len(nodes) {
		t.Errorf("only %d of %d nodes received keys", len(hits), len(nodes))
	}
}

func TestRing_distribution_variance(t *testing.T) {
	r := cluster.NewRing(150)
	nodes := []string{"node-1", "node-2", "node-3"}
	for _, n := range nodes {
		r.Add(n)
	}
	hits := make(map[string]int)
	const samples = 30000
	for i := 0; i < samples; i++ {
		node, _ := r.Lookup(fmt.Sprintf("img_%08d.jpg", i))
		hits[node]++
	}
	expected := samples / len(nodes)
	// 150 vnodes over 3 nodes keeps peak deviation under ~10% (it grows to
	// ~20-25% at dozens of nodes -- see the coefficient-of-variation note on Ring).
	tolerance := expected / 10
	for _, n := range nodes {
		diff := hits[n] - expected
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("node %q: %d hits, expected ~%d (tolerance ±%d)", n, hits[n], expected, tolerance)
		}
	}
}

func TestRing_removeRedistributesKeys(t *testing.T) {
	r := cluster.NewRing(150)
	r.Add("node-1")
	r.Add("node-2")

	const samples = 1000
	before := make(map[string]string, samples)
	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Lookup(key)
		before[key] = node
	}

	r.Remove("node-2")

	for key := range before {
		node, err := r.Lookup(key)
		if err != nil {
			t.Fatalf("Lookup(%q) after Remove: %v", key, err)
		}
		if node != "node-1" {
			t.Errorf("after Remove(node-2): Lookup(%q) = %q, want node-1", key, node)
		}
	}
}

func TestRing_lookupN_distinctNodes(t *testing.T) {
	r := cluster.NewRing(150)
	for i := 1; i <= 5; i++ {
		r.Add(fmt.Sprintf("node-%d", i))
	}
	nodes, err := r.LookupN("some-shard", 3)
	if err != nil {
		t.Fatalf("LookupN: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("LookupN(3) returned %d nodes, want 3", len(nodes))
	}
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("LookupN returned duplicate node %q", n)
		}
		seen[n] = true
	}
}

func TestRing_lookupN_cappedAtNodeCount(t *testing.T) {
	r := cluster.NewRing(150)
	r.Add("node-1")
	r.Add("node-2")
	nodes, err := r.LookupN("key", 5)
	if err != nil {
		t.Fatalf("LookupN: %v", err)
	}
	if len(nodes) > 2 {
		t.Fatalf("LookupN with 2 nodes returned %d, want at most 2", len(nodes))
	}
}

func TestRing_nodeCount(t *testing.T) {
	r := cluster.NewRing(150)
	if r.NodeCount() != 0 {
		t.Fatalf("empty ring NodeCount() = %d, want 0", r.NodeCount())
	}
	r.Add("a")
	r.Add("b")
	if r.NodeCount() != 2 {
		t.Fatalf("NodeCount() = %d, want 2", r.NodeCount())
	}
	r.Remove("a")
	if r.NodeCount() != 1 {
		t.Fatalf("NodeCount() after Remove = %d, want 1", r.NodeCount())
	}
}

func TestRing_addDuplicate_noEffect(t *testing.T) {
	r := cluster.NewRing(150)
	r.Add("node-1")
	r.Add("node-1") // second Add must be idempotent
	if r.NodeCount() != 1 {
		t.Fatalf("adding same node twice: NodeCount() = %d, want 1", r.NodeCount())
	}
}

func TestRing_distribution_matchesVnodeCount(t *testing.T) {
	r := cluster.NewRing(150)
	r.Add("a")
	r.Add("b")
	dist := r.Distribution()
	if len(dist) != 2 {
		t.Fatalf("Distribution() has %d nodes, want 2", len(dist))
	}
	for node, vnodes := range dist {
		if vnodes != 150 {
			t.Errorf("node %q has %d vnodes, want 150", node, vnodes)
		}
	}
}

// TestRing_concurrentAccess runs Add/Remove/Lookup/NodeCount simultaneously.
// Its purpose is to surface data races under -race; correctness is covered
// by the single-threaded tests above.
func TestRing_concurrentAccess(t *testing.T) {
	r := cluster.NewRing(50)
	r.Add("seed") // ensure Lookup never hits an empty ring mid-run

	var wg sync.WaitGroup

	// Mutators: churn nodes in and out
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			node := fmt.Sprintf("node-%d", id)
			for i := 0; i < 200; i++ {
				r.Add(node)
				r.Remove(node)
			}
		}(w)
	}

	// Readers: Lookup + NodeCount concurrently
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_, _ = r.Lookup(fmt.Sprintf("key-%d-%d", id, i))
				_ = r.NodeCount()
			}
		}(w)
	}

	wg.Wait()

	// "seed" was never removed, so it must still own all keys.
	if _, err := r.Lookup("anything"); err != nil {
		t.Fatalf("Lookup after concurrent churn failed: %v", err)
	}
}
