package raftgrpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus"
	"google.golang.org/grpc"
)

// Transport must satisfy the consensus transport contract.
var _ consensus.Transport = (*Transport)(nil)

type grpcCluster struct {
	nodes map[uint64]*consensus.Node
	fsms map[uint64]*consensus.CommitFSM
	transports map[uint64]*Transport
	servers map[uint64]*grpc.Server
	stopped map[uint64]bool
}

// startGRPCCluster brings up a Raft cluster whose members talk over real gRPC on
// localhost, each with a replicated CommitFSM. Listeners are created first so
// every node knows every peer's address before it starts.
func startGRPCCluster(t *testing.T, ids ...uint64) *grpcCluster {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	lns := make(map[uint64]net.Listener, len(ids))
	peers := make(map[uint64]string, len(ids))
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[id] = ln
		peers[id] = ln.Addr().String()
	}

	c := &grpcCluster{
		nodes: map[uint64]*consensus.Node{},
		fsms: map[uint64]*consensus.CommitFSM{},
		transports: map[uint64]*Transport{},
		servers: map[uint64]*grpc.Server{},
		stopped: map[uint64]bool{},
	}
	for _, id := range ids {
		tr := NewTransport(id, peers, log)
		fsm := consensus.NewCommitFSM()
		node, err := consensus.Start(consensus.Config{
			ID: id,
			Peers: ids,
			FSM: fsm,
			Transport: tr,
			TickInterval: 15 * time.Millisecond,
			ElectionTicks: 10,
			HeartbeatTicks: 1,
			Logger: log,
		}, nil)
		if err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		srv := grpc.NewServer()
		RegisterRaftTransportServer(srv, NewServer(node))
		go srv.Serve(lns[id])
		c.nodes[id], c.fsms[id], c.transports[id], c.servers[id] = node, fsm, tr, srv
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *grpcCluster) stop(id uint64) {
	if c.stopped[id] {
		return
	}
	c.stopped[id] = true
	c.servers[id].Stop()
	c.nodes[id].Stop()
	c.transports[id].Close()
}

func (c *grpcCluster) aliveNodes() map[uint64]*consensus.Node {
	out := map[uint64]*consensus.Node{}
	for id, n := range c.nodes {
		if !c.stopped[id] {
			out[id] = n
		}
	}
	return out
}

func waitForCond(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timeout after %s waiting for %s", timeout, desc)
}

func waitLeaderAmong(t *testing.T, nodes map[uint64]*consensus.Node) uint64 {
	t.Helper()
	var leader uint64
	waitForCond(t, 10*time.Second, "single leader", func() bool {
		var found []uint64
		for id, n := range nodes {
			if n.IsLeader() {
				found = append(found, id)
			}
		}
		if len(found) == 1 {
			leader = found[0]
			return true
		}
		return false
	})
	return leader
}

// proposeCommitUntilApplied retries a commit proposal until it lands in the
// leader's FSM (proposals can be dropped on a leadership change; the FSM is
// idempotent, so retry-to-convergence is the correct contract).
func proposeCommitUntilApplied(t *testing.T, n *consensus.Node, fsm *consensus.CommitFSM, cmd consensus.CommitCommand) {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_ = n.Propose(ctx, data)
		cancel()
		if _, ok := fsm.Committed(cmd.TaskID); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("commit %q never applied", cmd.TaskID)
}

// The transport carries real Raft traffic between processes: a 3-node cluster
// over gRPC elects a leader and replicates commit decisions to every FSM.
func TestTransport_replicatesAcrossProcesses(t *testing.T) {
	c := startGRPCCluster(t, 1, 2, 3)
	leader := waitLeaderAmong(t, c.nodes)

	for i := 0; i < 5; i++ {
		proposeCommitUntilApplied(t, c.nodes[leader], c.fsms[leader], consensus.CommitCommand{
			TaskID: fmt.Sprintf("t%d", i), JobID: "j", Generation: 1, FinalKey: fmt.Sprintf("results/j/%d.json", i),
		})
	}

	waitForCond(t, 8*time.Second, "all FSMs converge to 5 commits", func() bool {
		for _, f := range c.fsms {
			if f.Len() != 5 {
				return false
			}
		}
		return true
	})
}

// The headline HA property over a real transport: kill the leader, a survivor
// takes over, the pre-failover commit is still agreed everywhere, and new commits
// keep landing -- so a committed task is never lost or re-litigated across a
// coordinator failure.
func TestTransport_survivesLeaderFailover(t *testing.T) {
	c := startGRPCCluster(t, 1, 2, 3)
	old := waitLeaderAmong(t, c.nodes)
	proposeCommitUntilApplied(t, c.nodes[old], c.fsms[old], consensus.CommitCommand{
		TaskID: "before", JobID: "j", Generation: 1, FinalKey: "results/j/before.json",
	})

	c.stop(old)

	alive := c.aliveNodes()
	newLeader := waitLeaderAmong(t, alive)
	if newLeader == old {
		t.Fatalf("new leader %d must differ from stopped leader %d", newLeader, old)
	}

	proposeCommitUntilApplied(t, c.nodes[newLeader], c.fsms[newLeader], consensus.CommitCommand{
		TaskID: "after", JobID: "j", Generation: 1, FinalKey: "results/j/after.json",
	})

	// A surviving follower has both the pre- and post-failover commit.
	var survivor uint64
	for id := range alive {
		if id != newLeader {
			survivor = id
		}
	}
	waitForCond(t, 8*time.Second, "survivor has post-failover commit", func() bool {
		_, ok := c.fsms[survivor].Committed("after")
		return ok
	})
	if _, ok := c.fsms[survivor].Committed("before"); !ok {
		t.Fatal("pre-failover commit lost on survivor")
	}
}
