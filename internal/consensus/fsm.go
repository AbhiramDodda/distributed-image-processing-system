// Package consensus wraps the etcd Raft library to give the coordinator a
// replicated, leader-elected control plane (Level 6) -- retiring the
// single-coordinator single-point-of-failure noted back in Level 2. A Node runs
// the Raft protocol over a Transport; committed log entries are applied in order
// to an FSM, so every node's FSM converges to the same state. Only the leader
// accepts writes, which is how the coordinator will gate scheduling: schedule
// only while IsLeader() is true.
//
// This package is intentionally transport-agnostic. Tests drive a 3-node cluster
// over an in-memory network (transport.go); production would supply a gRPC
// transport carrying raftpb.Message between coordinators.
package consensus

import (
	"fmt"
	"strings"
	"sync"
)

// FSM is the application state machine Raft replicates. Apply is invoked once per
// committed log entry, in log order, on every node. It must be deterministic:
// the same entry applied on each node must produce the same state, which is what
// makes the replicas agree.
type FSM interface {
	Apply(data []byte) error
}

// KVStore is a minimal replicated key-value FSM. It doubles as the reference
// implementation and the test workload: a command is the bytes "key=value".
// A real coordinator FSM would instead decode scheduler commands (submit,
// assign, complete) -- the same shape the WAL already records.
type KVStore struct {
	mu sync.RWMutex
	data map[string]string
	applied int
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

func (k *KVStore) Apply(data []byte) error {
	key, val, ok := strings.Cut(string(data), "=")
	if !ok {
		return fmt.Errorf("consensus: malformed command %q, want key=value", data)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = val
	k.applied++
	return nil
}

func (k *KVStore) Get(key string) (string, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.data[key]
	return v, ok
}

// Len returns the number of distinct keys applied.
func (k *KVStore) Len() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.data)
}

// Command encodes a key/value into the wire form Apply expects.
func Command(key, value string) []byte {
	return []byte(key + "=" + value)
}
