package scheduler_test

import (
	"sync"
	"testing"

	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// memStore captures appended records, standing in for a WAL.
type memStore struct {
	mu sync.Mutex
	records [][]byte
}

func (m *memStore) Append(b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(b))
	copy(cp, b)
	m.records = append(m.records, cp)
	return nil
}

func (m *memStore) snapshot() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, len(m.records))
	copy(out, m.records)
	return out
}

func TestScheduler_persistAndRestore(t *testing.T) {
	store := &memStore{}
	s := newScheduler(2)
	s.AttachStore(store)

	job, err := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa", "bb", "cc"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	// Assign and complete one task; leave the rest pending.
	a, err := s.PollTasks("worker-1")
	if err != nil || a == nil {
		t.Fatalf("PollTasks: %v (a=%v)", err, a)
	}
	if err := s.ReportResult(a.TaskID, scheduler.ResultRequest{WorkerID: "worker-1", ImagesProcessed: 10}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if store.records == nil {
		t.Fatal("no records were persisted")
	}

	// Restore into a fresh scheduler purely from the recorded WAL stream.
	restored := newScheduler(2)
	if err := restored.Restore(nil, store.snapshot()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	rj, err := restored.GetJob(job.ID)
	if err != nil {
		t.Fatalf("restored GetJob: %v", err)
	}
	if rj.TotalTasks != 3 || rj.DoneTasks != 1 {
		t.Fatalf("restored job: TotalTasks=%d DoneTasks=%d, want 3/1", rj.TotalTasks, rj.DoneTasks)
	}
	if got := restored.PendingCount(); got != 2 {
		t.Fatalf("restored PendingCount = %d, want 2", got)
	}

	// The restored scheduler must be live: the remaining pending tasks dispatch.
	if a2, err := restored.PollTasks("worker-1"); err != nil || a2 == nil {
		t.Fatalf("restored PollTasks: %v (a=%v)", err, a2)
	}
	if got := restored.PendingCount(); got != 1 {
		t.Fatalf("after restored poll PendingCount = %d, want 1", got)
	}
}

func TestScheduler_restoreEmptyIsNoop(t *testing.T) {
	s := newScheduler(2)
	if err := s.Restore(nil, nil); err != nil {
		t.Fatalf("Restore(nil,nil): %v", err)
	}
	if s.PendingCount() != 0 {
		t.Fatalf("PendingCount = %d, want 0", s.PendingCount())
	}
}

func TestScheduler_snapshotWinsWhenNoRecords(t *testing.T) {
	store := &memStore{}
	s := newScheduler(2)
	s.AttachStore(store)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "d", Algorithm: "a", Shards: []string{"00", "01"},
	})
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := newScheduler(2)
	if err := restored.Restore(snap, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := restored.PendingCount(); got != 2 {
		t.Fatalf("restored from snapshot PendingCount = %d, want 2", got)
	}
}

func TestScheduler_noStoreNoPanic(t *testing.T) {
	// Without AttachStore the mutation path must be a no-op, not a nil deref.
	s := newScheduler(2)
	if _, err := s.Submit(scheduler.SubmitJobRequest{Dataset: "d", Algorithm: "a", Shards: []string{"00"}}); err != nil {
		t.Fatalf("Submit without store: %v", err)
	}
}
