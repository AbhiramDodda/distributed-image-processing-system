package cluster

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Membership tracks which nodes are alive using a two-stage failure detector.
//
// Transition rules:
//   Active  -> Suspect   if no heartbeat for suspectTimeout
//   Suspect -> Dead      if no heartbeat for deadTimeout
//   Suspect -> Active    if heartbeat arrives (recovery)
//   Dead    -> Active    if heartbeat arrives (restart)
type Membership struct {
	mu sync.RWMutex
	nodes map[string]*NodeInfo
	ring *Ring
	suspectTimeout time.Duration
	deadTimeout time.Duration
	events chan FailureEvent
	log *slog.Logger
}

func NewMembership(ring *Ring, suspectTimeout, deadTimeout time.Duration, log *slog.Logger) *Membership {
	return &Membership{
		nodes: make(map[string]*NodeInfo),
		ring: ring,
		suspectTimeout: suspectTimeout,
		deadTimeout: deadTimeout,
		events: make(chan FailureEvent, 128),
		log: log,
	}
}

func (m *Membership) Register(req RegisterRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.nodes[req.ID]; exists {
		m.nodes[req.ID].State = NodeActive
		m.nodes[req.ID].LastSeen = time.Now()
		return nil
	}
	m.nodes[req.ID] = &NodeInfo{
		ID: req.ID,
		Address: req.Address,
		State: NodeActive,
		LastSeen: time.Now(),
		JoinedAt: time.Now(),
	}
	m.ring.Add(req.ID)
	m.log.Info("node registered", "id", req.ID, "address", req.Address)
	return nil
}

func (m *Membership) Heartbeat(hb Heartbeat) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[hb.NodeID]
	if !ok {
		return fmt.Errorf("unknown node: %s", hb.NodeID)
	}
	prev := n.State
	n.LastSeen = time.Now()
	n.Metrics = hb.Metrics
	if n.State != NodeActive {
		n.State = NodeActive
		m.ring.Add(n.ID)
		m.emit(FailureEvent{NodeID: n.ID, OldState: prev, NewState: NodeActive, At: time.Now()})
		m.log.Info("node recovered", "id", n.ID, "from", prev)
	}
	return nil
}

func (m *Membership) Tick() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range m.nodes {
		age := now.Sub(n.LastSeen)
		switch n.State {
		case NodeActive:
			if age > m.suspectTimeout {
				n.State = NodeSuspect
				m.emit(FailureEvent{NodeID: n.ID, OldState: NodeActive, NewState: NodeSuspect, At: now})
				m.log.Warn("node suspected", "id", n.ID, "silent_for", age)
			}
		case NodeSuspect:
			if age > m.deadTimeout {
				n.State = NodeDead
				m.ring.Remove(n.ID)
				m.emit(FailureEvent{NodeID: n.ID, OldState: NodeSuspect, NewState: NodeDead, At: now})
				m.log.Error("node declared dead", "id", n.ID, "silent_for", age)
			}
		}
	}
}

func (m *Membership) Events() <-chan FailureEvent { return m.events }

func (m *Membership) ActiveNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*NodeInfo
	for _, n := range m.nodes {
		if n.State == NodeActive {
			out = append(out, n)
		}
	}
	return out
}

func (m *Membership) AllNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, n)
	}
	return out
}

func (m *Membership) NodeAddress(id string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return "", fmt.Errorf("node not found: %s", id)
	}
	return n.Address, nil
}

func (m *Membership) emit(e FailureEvent) {
	select {
	case m.events <- e:
	default:
	}
}
