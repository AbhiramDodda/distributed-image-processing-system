package consensus

import (
	"context"
	"fmt"
	"io"
	golog "log"
	"log/slog"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Config parameterises a Node. TickInterval is the wall-clock period of one Raft
// logical tick; election and heartbeat timeouts are expressed as tick counts, so
// the real timeouts are TickInterval*ElectionTicks etc. Keeping heartbeat one
// tick and election ten (the etcd default ratio) means a leader heartbeats ~10x
// per election timeout, so a healthy leader is never spuriously replaced.
type Config struct {
	ID uint64
	Peers []uint64
	FSM FSM
	Transport Transport
	TickInterval time.Duration
	ElectionTicks int
	HeartbeatTicks int
	Logger *slog.Logger
}

// Node is one member of a Raft cluster: it runs the protocol, persists log
// entries to in-memory storage, ships outbound messages via the Transport, and
// applies committed entries to the FSM.
type Node struct {
	id uint64
	raft raft.Node
	storage *raft.MemoryStorage
	fsm FSM
	transport Transport
	log *slog.Logger
	tickInterval time.Duration

	recvc chan *raftpb.Message
	stopc chan struct{}
	donec chan struct{}

	mu sync.RWMutex
	applied uint64
}

// Start brings up a Node and begins running the protocol in a background
// goroutine. When a network is provided, the node registers itself so peers can
// reach it. All nodes in a cluster must be started with the same Peers set.
func Start(cfg Config, net *InmemNetwork) (*Node, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("consensus: node id must be non-zero")
	}
	if cfg.FSM == nil {
		return nil, fmt.Errorf("consensus: FSM is required")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.ElectionTicks <= 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks <= 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	storage := raft.NewMemoryStorage()
	rc := &raft.Config{
		ID: cfg.ID,
		ElectionTick: cfg.ElectionTicks,
		HeartbeatTick: cfg.HeartbeatTicks,
		Storage: storage,
		MaxSizePerMsg: 1 << 20,
		MaxInflightMsgs: 256,
		// PreVote avoids a partitioned node forcing an election (and a term bump)
		// when it rejoins; CheckQuorum makes a leader that cannot reach a majority
		// step down instead of serving stale reads.
		PreVote: true,
		CheckQuorum: true,
		// Silence the library's own logger; this package logs through slog.
		Logger: &raft.DefaultLogger{Logger: golog.New(io.Discard, "", 0)},
	}

	peers := make([]raft.Peer, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = raft.Peer{ID: p}
	}

	n := &Node{
		id: cfg.ID,
		raft: raft.StartNode(rc, peers),
		storage: storage,
		fsm: cfg.FSM,
		transport: cfg.Transport,
		log: cfg.Logger.With("raft_id", cfg.ID),
		tickInterval: cfg.TickInterval,
		recvc: make(chan *raftpb.Message, 1024),
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	if net != nil {
		net.add(n)
	}
	go n.run()
	return n, nil
}

func (n *Node) run() {
	defer close(n.donec)
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.raft.Tick()
		case m := <-n.recvc:
			_ = n.raft.Step(context.Background(), m)
		case rd := <-n.raft.Ready():
			// Order matters: HardState and new entries must be durable BEFORE
			// their messages leave, so a peer never sees an entry this node could
			// forget after a crash.
			if !raft.IsEmptyHardState(rd.HardState) {
				_ = n.storage.SetHardState(rd.HardState)
			}
			if len(rd.Entries) > 0 {
				_ = n.storage.Append(rd.Entries)
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = n.storage.ApplySnapshot(rd.Snapshot)
			}
			n.transport.Send(rd.Messages)
			for _, ent := range rd.CommittedEntries {
				n.applyCommitted(ent)
			}
			n.raft.Advance()
		case <-n.stopc:
			n.raft.Stop()
			return
		}
	}
}

func (n *Node) applyCommitted(ent *raftpb.Entry) {
	switch ent.GetType() {
	case raftpb.EntryNormal:
		// An empty entry is the no-op a new leader commits to establish its term;
		// there is nothing for the FSM to apply.
		if len(ent.GetData()) > 0 {
			if err := n.fsm.Apply(ent.GetData()); err != nil {
				n.log.Error("fsm apply failed", "index", ent.GetIndex(), "err", err)
			}
		}
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := proto.Unmarshal(ent.GetData(), &cc); err == nil {
			n.raft.ApplyConfChange(&cc)
		}
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := proto.Unmarshal(ent.GetData(), &cc); err == nil {
			n.raft.ApplyConfChange(&cc)
		}
	}
	n.mu.Lock()
	if idx := ent.GetIndex(); idx > n.applied {
		n.applied = idx
	}
	n.mu.Unlock()
}

// receive hands an inbound message to the run loop. It never blocks: if the
// buffer is full the message is dropped, and Raft will retransmit. This is what
// prevents a routing cycle from deadlocking the in-process network.
func (n *Node) receive(m *raftpb.Message) {
	select {
	case n.recvc <- m:
	default:
	}
}

// Receive is the exported entry point a Transport uses to deliver an inbound Raft
// message (e.g. a gRPC transport server decoding a peer's raftpb.Message). Like
// the in-process path it never blocks: a full buffer drops the message and Raft
// retransmits, so a burst from a peer can never stall the run loop.
func (n *Node) Receive(m *raftpb.Message) { n.receive(m) }

// Propose submits data to be replicated. It only succeeds on (or via) the
// leader; Raft forwards a follower's proposal to the leader, but a proposal can
// still be dropped silently on a leadership change, so callers must retry until
// they observe the effect in the FSM.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.raft.Propose(ctx, data)
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	return n.raft.Status().RaftState == raft.StateLeader
}

// Leader returns the id this node thinks is leader (0 if none is known).
func (n *Node) Leader() uint64 {
	return n.raft.Status().Lead
}

// ID returns this node's Raft id.
func (n *Node) ID() uint64 { return n.id }

// Applied returns the highest log index this node has applied to its FSM.
func (n *Node) Applied() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.applied
}

// Stop halts the protocol goroutine and blocks until it exits.
func (n *Node) Stop() {
	select {
	case <-n.stopc:
	default:
		close(n.stopc)
	}
	<-n.donec
}
