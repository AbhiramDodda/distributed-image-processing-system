package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// fakeSideEffect records every Apply call: the keys delivered and, deduping on
// key, the distinct effects that "landed". failNext models a downstream that
// rejects one delivery so the task stays retriable.
type fakeSideEffect struct {
	mu sync.Mutex
	calls []string // every key delivered, in order (may contain duplicates)
	landed map[string]scheduler.CommitDecision // deduped by key
	failNext bool
}

func newFakeSideEffect() *fakeSideEffect {
	return &fakeSideEffect{landed: make(map[string]scheduler.CommitDecision)}
}

func (e *fakeSideEffect) Apply(_ context.Context, key string, d scheduler.CommitDecision) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failNext {
		e.failNext = false
		return errors.New("downstream unavailable")
	}
	e.calls = append(e.calls, key)
	e.landed[key] = d // idempotent on key: a redelivery overwrites with equal data
	return nil
}

func (e *fakeSideEffect) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *fakeSideEffect) landedCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.landed)
}

// SideEffectKey is the same for every re-execution of the same logical unit of
// work -- it must not depend on attempt, worker, or lease generation -- and
// differs once the range differs. This attempt-independence is what lets a
// downstream deduplicate.
func TestSideEffectKey_deterministicAndAttemptIndependent(t *testing.T) {
	whole := scheduler.Range{Start: 0, End: -1, Split: false}
	k1 := scheduler.SideEffectKey("job1", "ff", whole)
	k2 := scheduler.SideEffectKey("job1", "ff", whole)
	if k1 != k2 {
		t.Fatalf("key not deterministic: %q vs %q", k1, k2)
	}
	// A different job, shard, or range must yield a different key.
	if scheduler.SideEffectKey("job2", "ff", whole) == k1 {
		t.Error("key collided across jobs")
	}
	if scheduler.SideEffectKey("job1", "a3", whole) == k1 {
		t.Error("key collided across shards")
	}
	split := scheduler.Range{Start: 0, End: 500, Split: true}
	if scheduler.SideEffectKey("job1", "ff", split) == k1 {
		t.Error("key collided across ranges (split vs whole)")
	}
}

// A successful report fires the side effect once, stamped with the task's
// SideEffectKey; a duplicate at-least-once report does not fire it again.
func TestReportResult_firesSideEffectOncePerCommit(t *testing.T) {
	s := newScheduler(3)
	fe := newFakeSideEffect()
	s.AttachCommitter(newFakeCommitter())
	s.AttachSideEffect(fe)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)

	for i := 0; i < 3; i++ {
		if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err != nil {
			t.Fatalf("ReportResult #%d: %v", i, err)
		}
	}
	// Only the first report reaches a not-yet-done task, so the effect fires once.
	if fe.callCount() != 1 {
		t.Fatalf("side effect fired %d times across 3 reports, want 1", fe.callCount())
	}
	// The delivered key is exactly the task's deterministic SideEffectKey.
	wantKey := scheduler.SideEffectKey(job.ID, "ff", scheduler.Range{Start: a.RangeStart, End: a.RangeEnd, Split: a.Split})
	if _, ok := fe.landed[wantKey]; !ok {
		t.Fatalf("side effect not stamped with SideEffectKey %q; landed=%v", wantKey, fe.landed)
	}
}

// If the side effect fails, ReportResult fails and the task stays not-done, so it
// is retried; a later delivery re-fires under the SAME key, and a key-deduping
// receiver still records exactly one effect.
func TestReportResult_sideEffectFailureLeavesTaskRetriable(t *testing.T) {
	s := newScheduler(3)
	fe := newFakeSideEffect()
	fe.failNext = true
	s.AttachCommitter(newFakeCommitter())
	s.AttachSideEffect(fe)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)

	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err == nil {
		t.Fatal("ReportResult succeeded despite a failed side effect")
	}
	if got, _ := s.GetJob(job.ID); got.Status == scheduler.JobCompleted {
		t.Error("job marked completed despite an undelivered side effect")
	}
	// Retry: the same key is delivered again and the effect lands exactly once.
	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err != nil {
		t.Fatalf("retry ReportResult: %v", err)
	}
	if fe.landedCount() != 1 {
		t.Fatalf("effect landed %d times, want exactly 1", fe.landedCount())
	}
	if got, _ := s.GetJob(job.ID); got.Status != scheduler.JobCompleted {
		t.Errorf("job status after retry = %q, want completed", got.Status)
	}
}
