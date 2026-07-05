package scheduler

import (
	"encoding/json"
	"fmt"
)

// RecordStore is the durable sink the scheduler appends state to. *pipeline.WAL
// satisfies it. Kept as a narrow interface so the scheduler does not depend on
// the pipeline package.
type RecordStore interface {
	Append([]byte) error
}

// schedulerState is the full serialized scheduler state. Every mutation appends
// one of these, and recovery restores from the most recent (last-writer-wins),
// so replay needs no per-event logic — the trade-off is a larger record per
// write, which is cheap at this scale (<=256 tasks + a handful of jobs) and is
// compacted by periodic checkpoints.
type schedulerState struct {
	Jobs []*Job `json:"jobs"`
	Tasks []*Task `json:"tasks"`
	Pending []string `json:"pending"`
}

// AttachStore enables durability: subsequent mutations are appended to store.
// Must be called before the scheduler serves traffic (e.g. right after Restore).
func (s *Scheduler) AttachStore(store RecordStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

// Snapshot returns the current full state for checkpointing.
func (s *Scheduler) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.encodeStateLocked()
}

// Restore rebuilds scheduler state from a WAL recovery. Records win over the
// snapshot (they are newer); the most recent record — or the snapshot if there
// are none — is the authoritative full state. A fully empty recovery is a no-op.
func (s *Scheduler) Restore(snapshot []byte, records [][]byte) error {
	blob := snapshot
	if n := len(records); n > 0 {
		blob = records[n-1]
	}
	if blob == nil {
		return nil
	}
	var st schedulerState
	if err := json.Unmarshal(blob, &st); err != nil {
		return fmt.Errorf("scheduler: decode restored state: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = make(map[string]*Job, len(st.Jobs))
	s.tasks = make(map[string]*Task, len(st.Tasks))
	for _, j := range st.Jobs {
		s.jobs[j.ID] = j
	}
	for _, t := range st.Tasks {
		s.tasks[t.ID] = t
	}
	s.pendingQ = st.Pending
	s.log.Info("scheduler state restored", "jobs", len(s.jobs), "tasks", len(s.tasks), "pending", len(s.pendingQ))
	return nil
}

// encodeStateLocked serializes the current state. Caller holds s.mu (read or
// write). Map iteration order is irrelevant; pendingQ order is preserved.
func (s *Scheduler) encodeStateLocked() ([]byte, error) {
	st := schedulerState{Pending: s.pendingQ}
	for _, j := range s.jobs {
		st.Jobs = append(st.Jobs, j)
	}
	for _, t := range s.tasks {
		st.Tasks = append(st.Tasks, t)
	}
	b, err := json.Marshal(st)
	if err != nil {
		return nil, fmt.Errorf("scheduler: encode state: %w", err)
	}
	return b, nil
}

// persistLocked appends the current state to the store if one is attached.
// Caller holds s.mu (write). A persistence failure is logged but does not fail
// the in-memory operation: the live state remains authoritative, and at worst
// the last change is not durable across a crash.
func (s *Scheduler) persistLocked() {
	if s.store == nil {
		return
	}
	b, err := s.encodeStateLocked()
	if err != nil {
		s.log.Error("scheduler: encode state for wal", "err", err)
		return
	}
	if err := s.store.Append(b); err != nil {
		s.log.Error("scheduler: append to wal", "err", err)
	}
}
