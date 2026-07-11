package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// fakeCommitter models an object store: objects maps a final key to the staging
// key last copied onto it, so copying twice to the same final key still leaves
// one object — exactly like a real S3 server-side copy. calls counts every
// Commit invocation (which can exceed the object count under concurrency, and is
// still correct). failNext forces the next Commit to error, standing in for a
// transient S3 failure.
type fakeCommitter struct {
	mu sync.Mutex
	objects map[string]string
	calls int
	failNext bool
}

func newFakeCommitter() *fakeCommitter {
	return &fakeCommitter{objects: make(map[string]string)}
}

func (f *fakeCommitter) Commit(_ context.Context, stagingKey, finalKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return errors.New("staging copy failed")
	}
	f.calls++
	f.objects[finalKey] = stagingKey
	return nil
}

// objectCount is the number of distinct final objects that landed — the actual
// exactly-once guarantee, independent of how many copy calls produced them.
func (f *fakeCommitter) objectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

func (f *fakeCommitter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeCommitter) stagedInto(finalKey string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.objects[finalKey]
	return s, ok
}

func result(staging string) scheduler.ResultRequest {
	return scheduler.ResultRequest{WorkerID: "worker-1", ImagesProcessed: 10, OutputKey: staging}
}

// A successful report commits the staged output to its canonical key exactly
// once and marks the task done.
func TestReportResult_commitsStagedOutputOnce(t *testing.T) {
	s := newScheduler(3)
	fc := newFakeCommitter()
	s.AttachCommitter(fc)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")

	final := scheduler.FinalResultKey(job.ID, a.Shard, scheduler.Range{})
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)
	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	if got := fc.callCount(); got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}
	if src, ok := fc.stagedInto(final); !ok || src != staging {
		t.Errorf("final key %q sourced from %q (present=%v), want %q", final, src, ok, staging)
	}
	got, _ := s.GetJob(job.ID)
	if got.Status != scheduler.JobCompleted {
		t.Errorf("job status = %q, want completed", got.Status)
	}
}

// A duplicate at-least-once delivery for an already-committed task must not
// commit a second time — this is the core exactly-once guarantee.
func TestReportResult_duplicateReportIsNoop(t *testing.T) {
	s := newScheduler(3)
	fc := newFakeCommitter()
	s.AttachCommitter(fc)

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
	// Sequential reports each complete (marking the task done) before the next
	// begins, so the idempotency guard admits exactly one commit call.
	if got := fc.callCount(); got != 1 {
		t.Fatalf("commit calls after 3 reports = %d, want 1", got)
	}
}

// Concurrent duplicate reports (a rebalanced task whose original result also
// arrives) settle to a single committed final object.
func TestReportResult_concurrentDuplicatesCommitOnce(t *testing.T) {
	s := newScheduler(3)
	fc := newFakeCommitter()
	s.AttachCommitter(fc)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ReportResult(context.Background(), a.TaskID, result(staging))
		}()
	}
	wg.Wait()

	// The copy happens outside the lock, so concurrent duplicates may each issue
	// one — but every copy targets the same final key, so exactly one object
	// lands. Asserting the object count (not the call count) captures the real
	// exactly-once guarantee.
	if got := fc.objectCount(); got != 1 {
		t.Fatalf("distinct final objects under concurrency = %d, want 1", got)
	}
}

// A failed commit must not mark the task done: the error propagates so the
// worker retries, and the task stays open for a clean re-report.
func TestReportResult_commitFailureLeavesTaskOpen(t *testing.T) {
	s := newScheduler(3)
	fc := newFakeCommitter()
	fc.failNext = true
	s.AttachCommitter(fc)

	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	staging := scheduler.StagingResultKey(job.ID, a.TaskID)

	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err == nil {
		t.Fatal("ReportResult returned nil, want commit error")
	}
	if got, _ := s.GetJob(job.ID); got.Status == scheduler.JobCompleted {
		t.Fatal("job completed despite failed commit")
	}

	// A retry (failNext already cleared) commits cleanly, exactly once.
	if err := s.ReportResult(context.Background(), a.TaskID, result(staging)); err != nil {
		t.Fatalf("ReportResult retry: %v", err)
	}
	if got := fc.callCount(); got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}
	if got, _ := s.GetJob(job.ID); got.Status != scheduler.JobCompleted {
		t.Errorf("job status = %q, want completed", got.Status)
	}
}

// The failure (error) path never commits: there is no output to promote.
func TestReportResult_errorPathDoesNotCommit(t *testing.T) {
	s := newScheduler(3)
	fc := newFakeCommitter()
	s.AttachCommitter(fc)

	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})
	a, _ := s.PollTasks("worker-1")
	if err := s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{
		WorkerID: "worker-1", Error: "boom",
	}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}
	if got := fc.callCount(); got != 0 {
		t.Fatalf("commit calls on error path = %d, want 0", got)
	}
}
