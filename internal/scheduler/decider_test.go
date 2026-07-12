package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// fakeDecider stands in for the consensus-backed commit ledger: it records the
// winning decision per task, is idempotent + fenced (a lower generation loses),
// and can be made to fail (a leader that cannot reach a quorum) via failNext.
type fakeDecider struct {
	mu sync.Mutex
	decided map[string]scheduler.CommitDecision
	calls int
	failNext bool
}

func newFakeDecider() *fakeDecider {
	return &fakeDecider{decided: make(map[string]scheduler.CommitDecision)}
}

func (d *fakeDecider) Decide(_ context.Context, dec scheduler.CommitDecision) (scheduler.CommitDecision, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failNext {
		d.failNext = false
		return scheduler.CommitDecision{}, errors.New("no quorum")
	}
	d.calls++
	if cur, ok := d.decided[dec.TaskID]; ok && dec.Generation <= cur.Generation {
		return cur, nil // fenced / idempotent
	}
	d.decided[dec.TaskID] = dec
	return dec, nil
}

func (d *fakeDecider) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.decided)
}

func (d *fakeDecider) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// A successful report routes the terminal commit through the decider exactly once
// and marks the task done, so the completed task is never re-dispatched.
func TestReportResult_commitsThroughDeciderAndDoesNotRedispatch(t *testing.T) {
	s := newScheduler(3)
	fc := newFakeCommitter()
	fd := newFakeDecider()
	s.AttachCommitter(fc)
	s.AttachCommitDecider(fd)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)
	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if fd.count() != 1 || fd.callCount() != 1 {
		t.Fatalf("decider recorded=%d calls=%d, want 1/1", fd.count(), fd.callCount())
	}
	if got, _ := s.GetJob(job.ID); got.Status != scheduler.JobCompleted {
		t.Errorf("job status = %q, want completed", got.Status)
	}
	// The committed task is terminal: a further poll must not hand it back out.
	if b, _ := s.PollTasks("worker-2"); b != nil {
		t.Errorf("committed task re-dispatched: %+v", b)
	}
}

// A duplicate report for an already-decided task does not decide again -- the
// exactly-once commit decision holds across at-least-once delivery.
func TestReportResult_duplicateDeciderCommitIsNoop(t *testing.T) {
	s := newScheduler(3)
	fd := newFakeDecider()
	s.AttachCommitter(newFakeCommitter())
	s.AttachCommitDecider(fd)

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
	if fd.callCount() != 1 {
		t.Fatalf("decider called %d times across 3 reports, want 1", fd.callCount())
	}
}

// If the commit decision cannot be agreed (no quorum), ReportResult fails and the
// task stays not-done, so it is retried rather than silently lost -- the decision
// is never faked locally.
func TestReportResult_deciderFailureLeavesTaskRetriable(t *testing.T) {
	s := newScheduler(3)
	fd := newFakeDecider()
	fd.failNext = true
	s.AttachCommitter(newFakeCommitter())
	s.AttachCommitDecider(fd)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)

	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err == nil {
		t.Fatal("ReportResult succeeded despite a failed commit decision")
	}
	if got, _ := s.GetJob(job.ID); got.Status == scheduler.JobCompleted {
		t.Error("job marked completed despite an un-agreed commit")
	}
	// A retry now succeeds and commits exactly once.
	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err != nil {
		t.Fatalf("retry ReportResult: %v", err)
	}
	if fd.count() != 1 {
		t.Fatalf("decider recorded %d decisions, want 1", fd.count())
	}
	if got, _ := s.GetJob(job.ID); got.Status != scheduler.JobCompleted {
		t.Errorf("job status after retry = %q, want completed", got.Status)
	}
}
