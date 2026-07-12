package coordinator

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/consensus/raftgrpc"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
	"google.golang.org/grpc"
)

// raftFixture is a Raft cluster over real gRPC on localhost, each node holding a
// CommitFSM -- the substrate the raftCommitDecider commits through.
type raftFixture struct {
	nodes map[uint64]*consensus.Node
	fsms map[uint64]*consensus.CommitFSM
	transports map[uint64]*raftgrpc.Transport
	servers map[uint64]*grpc.Server
	stopped map[uint64]bool
}

func startRaftFixture(t *testing.T, ids ...uint64) *raftFixture {
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

	f := &raftFixture{
		nodes: map[uint64]*consensus.Node{},
		fsms: map[uint64]*consensus.CommitFSM{},
		transports: map[uint64]*raftgrpc.Transport{},
		servers: map[uint64]*grpc.Server{},
		stopped: map[uint64]bool{},
	}
	for _, id := range ids {
		tr := raftgrpc.NewTransport(id, peers, log)
		fsm := consensus.NewCommitFSM()
		node, err := consensus.Start(consensus.Config{
			ID: id, Peers: ids, FSM: fsm, Transport: tr,
			TickInterval: 15 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 1, Logger: log,
		}, nil)
		if err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		srv := grpc.NewServer()
		raftgrpc.RegisterRaftTransportServer(srv, raftgrpc.NewServer(node))
		go srv.Serve(lns[id])
		f.nodes[id], f.fsms[id], f.transports[id], f.servers[id] = node, fsm, tr, srv
	}
	t.Cleanup(func() {
		for id := range f.nodes {
			f.stop(id)
		}
	})
	return f
}

func (f *raftFixture) stop(id uint64) {
	if f.stopped[id] {
		return
	}
	f.stopped[id] = true
	f.servers[id].Stop()
	f.nodes[id].Stop()
	f.transports[id].Close()
}

func (f *raftFixture) waitLeader(t *testing.T) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var found []uint64
		for id, n := range f.nodes {
			if !f.stopped[id] && n.IsLeader() {
				found = append(found, id)
			}
		}
		if len(found) == 1 {
			return found[0]
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("no single leader elected")
	return 0
}

func (f *raftFixture) allFSMsHave(taskID string) bool {
	for id, fsm := range f.fsms {
		if f.stopped[id] {
			continue
		}
		if _, ok := fsm.Committed(taskID); !ok {
			return false
		}
	}
	return true
}

// The production commit path (raftCommitDecider.Decide) agrees a task's commit
// through Raft so it lands on every FSM, is idempotent under a duplicate report,
// and fences a stale (lower-generation) attempt.
func TestRaftCommitDecider_agreesIdempotentlyAndFences(t *testing.T) {
	f := startRaftFixture(t, 1, 2, 3)
	leader := f.waitLeader(t)
	dec := &raftCommitDecider{node: f.nodes[leader], fsm: f.fsms[leader], log: slog.Default()}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	won, err := dec.Decide(ctx, scheduler.CommitDecision{TaskID: "t1", JobID: "j", Generation: 2, FinalKey: "results/j/aa.json"})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if won.Generation != 2 || won.FinalKey != "results/j/aa.json" {
		t.Fatalf("winner = %+v, want gen 2 / results/j/aa.json", won)
	}

	waitFixture(t, 8*time.Second, "all FSMs agree on the commit", func() bool { return f.allFSMsHave("t1") })

	// Duplicate report: same decision returns, no second entry.
	if _, err := dec.Decide(ctx, scheduler.CommitDecision{TaskID: "t1", JobID: "j", Generation: 2, FinalKey: "results/j/aa.json"}); err != nil {
		t.Fatalf("duplicate Decide: %v", err)
	}
	// Stale attempt (lower generation) loses to the recorded generation-2 commit.
	stale, err := dec.Decide(ctx, scheduler.CommitDecision{TaskID: "t1", JobID: "j", Generation: 1, FinalKey: "results/j/stale.json"})
	if err != nil {
		t.Fatalf("stale Decide: %v", err)
	}
	if stale.Generation != 2 || stale.FinalKey != "results/j/aa.json" {
		t.Fatalf("stale attempt won: %+v, want the recorded gen-2 decision", stale)
	}
	for id, fsm := range f.fsms {
		if fsm.Len() != 1 {
			t.Fatalf("node %d FSM has %d commits, want 1 (idempotent+fenced)", id, fsm.Len())
		}
	}
}

// A commit agreed before a leader failure is still recorded everywhere after a
// new leader takes over, and the new leader can agree further commits -- so a
// committed task is never re-litigated across a coordinator failover.
func TestRaftCommitDecider_survivesFailover(t *testing.T) {
	f := startRaftFixture(t, 1, 2, 3)
	old := f.waitLeader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	oldDec := &raftCommitDecider{node: f.nodes[old], fsm: f.fsms[old], log: slog.Default()}
	if _, err := oldDec.Decide(ctx, scheduler.CommitDecision{TaskID: "before", JobID: "j", Generation: 1, FinalKey: "results/j/before.json"}); err != nil {
		t.Fatalf("pre-failover Decide: %v", err)
	}
	waitFixture(t, 8*time.Second, "pre-failover commit replicated", func() bool { return f.allFSMsHave("before") })

	f.stop(old)
	newLeader := f.waitLeader(t)
	if newLeader == old {
		t.Fatalf("new leader %d equals stopped leader %d", newLeader, old)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	newDec := &raftCommitDecider{node: f.nodes[newLeader], fsm: f.fsms[newLeader], log: slog.Default()}
	if _, err := newDec.Decide(ctx2, scheduler.CommitDecision{TaskID: "after", JobID: "j", Generation: 1, FinalKey: "results/j/after.json"}); err != nil {
		t.Fatalf("post-failover Decide: %v", err)
	}

	// The new leader's FSM holds both the pre- and post-failover commit.
	if _, ok := f.fsms[newLeader].Committed("before"); !ok {
		t.Fatal("pre-failover commit lost after failover")
	}
	if _, ok := f.fsms[newLeader].Committed("after"); !ok {
		t.Fatal("post-failover commit not recorded")
	}
}

func waitFixture(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
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
