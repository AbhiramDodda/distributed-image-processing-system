package consensus

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func startCluster(t *testing.T, ids ...uint64) (*InmemNetwork, map[uint64]*Node, map[uint64]*KVStore) {
	t.Helper()
	net := NewInmemNetwork()
	nodes := make(map[uint64]*Node)
	stores := make(map[uint64]*KVStore)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range ids {
		kv := NewKVStore()
		n, err := Start(Config{
			ID: id,
			Peers: ids,
			FSM: kv,
			Transport: net.Transport(id),
			TickInterval: 15 * time.Millisecond,
			ElectionTicks: 10,
			HeartbeatTicks: 1,
			Logger: log,
		}, net)
		if err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		nodes[id] = n
		stores[id] = kv
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return net, nodes, stores
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout after %s waiting for %s", timeout, desc)
}

func leaders(nodes map[uint64]*Node) []uint64 {
	var out []uint64
	for id, n := range nodes {
		if n.IsLeader() {
			out = append(out, id)
		}
	}
	return out
}

func waitLeader(t *testing.T, nodes map[uint64]*Node) uint64 {
	t.Helper()
	var id uint64
	waitFor(t, 8*time.Second, "single leader", func() bool {
		if ls := leaders(nodes); len(ls) == 1 {
			id = ls[0]
			return true
		}
		return false
	})
	return id
}

// proposeUntilApplied retries a proposal until it lands in the given store.
// Proposals can be dropped on a leadership change, and re-applying "key=value" is
// idempotent, so retrying to convergence is the correct client contract.
func proposeUntilApplied(t *testing.T, n *Node, s *KVStore, key, val string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_ = n.Propose(ctx, Command(key, val))
		cancel()
		if v, ok := s.Get(key); ok && v == val {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("proposal %s=%s never applied", key, val)
}

func TestCluster_electsSingleLeader(t *testing.T) {
	_, nodes, _ := startCluster(t, 1, 2, 3)
	leader := waitLeader(t, nodes)
	waitFor(t, 5*time.Second, "all nodes agree on leader", func() bool {
		for _, n := range nodes {
			if n.Leader() != leader {
				return false
			}
		}
		return true
	})
}

func TestCluster_replicatesProposals(t *testing.T) {
	_, nodes, stores := startCluster(t, 1, 2, 3)
	leader := waitLeader(t, nodes)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		proposeUntilApplied(t, nodes[leader], stores[leader], k, "v-"+k)
	}

	waitFor(t, 5*time.Second, "all FSMs converge", func() bool {
		for _, s := range stores {
			if s.Len() != len(keys) {
				return false
			}
		}
		return true
	})
	for id, s := range stores {
		for _, k := range keys {
			if v, ok := s.Get(k); !ok || v != "v-"+k {
				t.Fatalf("node %d key %q = %q (ok=%v), want v-%s", id, k, v, ok, k)
			}
		}
	}
}

// A proposal issued to a follower must still commit: Raft forwards it to the
// leader.
func TestCluster_followerProposalForwarded(t *testing.T) {
	_, nodes, stores := startCluster(t, 1, 2, 3)
	leader := waitLeader(t, nodes)

	var follower uint64
	for id := range nodes {
		if id != leader {
			follower = id
			break
		}
	}
	proposeUntilApplied(t, nodes[follower], stores[follower], "fwd", "ok")

	waitFor(t, 5*time.Second, "leader replicated follower's proposal", func() bool {
		v, ok := stores[leader].Get("fwd")
		return ok && v == "ok"
	})
}

// The headline property: kill the leader and the surviving majority elects a new
// one and keeps committing, with no split brain.
func TestCluster_leaderFailover(t *testing.T) {
	net, nodes, stores := startCluster(t, 1, 2, 3)
	old := waitLeader(t, nodes)
	proposeUntilApplied(t, nodes[old], stores[old], "before", "1")

	// Partition and stop the leader.
	net.SetDown(old, true)
	nodes[old].Stop()

	alive := make(map[uint64]*Node)
	for id, n := range nodes {
		if id != old {
			alive[id] = n
		}
	}

	newLeader := waitLeader(t, alive)
	if newLeader == old {
		t.Fatalf("new leader %d must differ from the stopped leader %d", newLeader, old)
	}

	// The two-node majority keeps making progress.
	proposeUntilApplied(t, nodes[newLeader], stores[newLeader], "after", "2")

	var survivor uint64
	for id := range alive {
		if id != newLeader {
			survivor = id
		}
	}
	waitFor(t, 5*time.Second, "post-failover replication to survivor", func() bool {
		v, ok := stores[survivor].Get("after")
		return ok && v == "2"
	})
	// The value committed before the failover survived the leader change.
	if v, ok := stores[survivor].Get("before"); !ok || v != "1" {
		t.Fatalf("pre-failover entry lost on survivor: %q (ok=%v)", v, ok)
	}
}
