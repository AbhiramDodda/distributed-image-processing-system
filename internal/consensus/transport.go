package consensus

import (
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

// Transport delivers a node's outbound Raft messages to their destinations. Raft
// tolerates message loss -- it retries -- so a Transport may drop rather than
// block, which is what keeps the delivery path deadlock-free.
type Transport interface {
	Send(msgs []*raftpb.Message)
}

// InmemNetwork routes Raft messages between Nodes in one process. It is the test
// harness for the cluster; SetDown simulates a node partition or crash by
// silently dropping every message to or from that node, which is exactly what a
// real network partition looks like to Raft.
type InmemNetwork struct {
	mu sync.RWMutex
	nodes map[uint64]*Node
	down map[uint64]bool
}

func NewInmemNetwork() *InmemNetwork {
	return &InmemNetwork{
		nodes: make(map[uint64]*Node),
		down: make(map[uint64]bool),
	}
}

// Transport returns the Transport a node with the given id should use.
func (nw *InmemNetwork) Transport(id uint64) Transport {
	return &inmemTransport{net: nw, from: id}
}

func (nw *InmemNetwork) add(n *Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[n.id] = n
}

// SetDown partitions (true) or heals (false) a node. A downed node neither sends
// nor receives, modelling a crash or an isolating partition.
func (nw *InmemNetwork) SetDown(id uint64, down bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.down[id] = down
}

func (nw *InmemNetwork) route(from uint64, msgs []*raftpb.Message) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	if nw.down[from] {
		return
	}
	for _, m := range msgs {
		if m.To == nil {
			continue
		}
		to := *m.To
		if nw.down[to] {
			continue
		}
		if target, ok := nw.nodes[to]; ok {
			target.receive(m)
		}
	}
}

type inmemTransport struct {
	net *InmemNetwork
	from uint64
}

func (t *inmemTransport) Send(msgs []*raftpb.Message) {
	t.net.route(t.from, msgs)
}
