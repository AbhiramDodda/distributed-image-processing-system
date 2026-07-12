package scheduler_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

var testLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func newScheduler(maxRetries int) *scheduler.Scheduler {
	ring := cluster.NewRing(10)
	ring.Add("worker-1")
	ring.Add("worker-2")
	return scheduler.New(ring, maxRetries, testLog)
}

func TestScheduler_submitCreatesOneTasKPerShard(t *testing.T) {
	s := newScheduler(2)
	job, err := s.Submit(scheduler.SubmitJobRequest{
		Dataset:   "train",
		Algorithm: "resnet",
		Shards:    []string{"00", "01", "02"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if job.TotalTasks != 3 {
		t.Errorf("TotalTasks = %d, want 3", job.TotalTasks)
	}
	if s.PendingCount() != 3 {
		t.Errorf("PendingCount() = %d, want 3", s.PendingCount())
	}
}

func TestScheduler_pollReturnsAssignment(t *testing.T) {
	s := newScheduler(2)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa"},
	})
	a, err := s.PollTasks("worker-1")
	if err != nil {
		t.Fatalf("PollTasks: %v", err)
	}
	if a == nil {
		t.Fatal("PollTasks returned nil with 1 pending task")
	}
	if a.Shard != "aa" {
		t.Errorf("assignment shard = %q, want aa", a.Shard)
	}
	if a.Dataset != "train" {
		t.Errorf("assignment dataset = %q, want train", a.Dataset)
	}
	if s.PendingCount() != 0 {
		t.Errorf("PendingCount after poll = %d, want 0", s.PendingCount())
	}
}

func TestScheduler_pollEmptyQueueReturnsNil(t *testing.T) {
	s := newScheduler(2)
	a, err := s.PollTasks("worker-1")
	if err != nil {
		t.Fatalf("PollTasks on empty queue: %v", err)
	}
	if a != nil {
		t.Fatal("expected nil assignment on empty queue")
	}
}

func TestScheduler_fullTaskLifecycle_jobCompletes(t *testing.T) {
	s := newScheduler(2)
	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"00", "01"},
	})

	// Poll and complete both tasks
	for i := 0; i < 2; i++ {
		a, _ := s.PollTasks("worker-1")
		if a == nil {
			t.Fatalf("poll %d returned nil", i)
		}
		s.StartTask(a.TaskID, "worker-1")
		s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{
			WorkerID:        "worker-1",
			ImagesProcessed: 100,
		})
	}

	got, err := s.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != scheduler.JobCompleted {
		t.Errorf("job status = %q, want completed", got.Status)
	}
	if got.DoneTasks != 2 {
		t.Errorf("DoneTasks = %d, want 2", got.DoneTasks)
	}
}

func TestScheduler_taskError_requeues(t *testing.T) {
	s := newScheduler(2)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})

	a, _ := s.PollTasks("worker-1")
	s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{
		WorkerID: "worker-1",
		Error:    "network timeout",
	})

	// Task should be back in the queue
	if s.PendingCount() != 1 {
		t.Errorf("PendingCount after error = %d, want 1 (re-queued)", s.PendingCount())
	}
}

func TestScheduler_maxRetries_taskFails(t *testing.T) {
	s := newScheduler(1) // only 1 retry allowed
	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"ff"},
	})

	// First attempt
	a, _ := s.PollTasks("worker-1")
	s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{WorkerID: "worker-1", Error: "fail"})

	// Second attempt (the retry)
	a, _ = s.PollTasks("worker-1")
	s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{WorkerID: "worker-1", Error: "fail again"})

	// No more retries — task and job should be failed
	if s.PendingCount() != 0 {
		t.Errorf("PendingCount after max retries = %d, want 0", s.PendingCount())
	}
	got, _ := s.GetJob(job.ID)
	if got.Status != scheduler.JobFailed {
		t.Errorf("job status = %q, want failed", got.Status)
	}
	if got.FailedTasks != 1 {
		t.Errorf("FailedTasks = %d, want 1", got.FailedTasks)
	}
}

func TestScheduler_rebalanceWorker_requeuesAssignedTasks(t *testing.T) {
	s := newScheduler(3)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"00", "01", "02"},
	})

	// Poll 2 tasks for worker-1 but don't report results (simulate dead worker)
	a1, _ := s.PollTasks("worker-1")
	a2, _ := s.PollTasks("worker-1")
	_ = a1
	_ = a2

	if s.PendingCount() != 1 {
		t.Errorf("PendingCount before rebalance = %d, want 1", s.PendingCount())
	}

	s.RebalanceWorker("worker-1")

	// All 3 tasks should be pending again
	if s.PendingCount() != 3 {
		t.Errorf("PendingCount after rebalance = %d, want 3", s.PendingCount())
	}
}

func TestScheduler_drainPending(t *testing.T) {
	s := newScheduler(2)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"00", "01", "02", "03", "04"},
	})

	drained := s.DrainPending(3)
	if len(drained) != 3 {
		t.Fatalf("DrainPending(3) returned %d assignments, want 3", len(drained))
	}
	if s.PendingCount() != 2 {
		t.Errorf("PendingCount after DrainPending(3) = %d, want 2", s.PendingCount())
	}
}

func TestScheduler_drainPending_clampedToQueueSize(t *testing.T) {
	s := newScheduler(2)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"00", "01"},
	})

	drained := s.DrainPending(100) // request more than available
	if len(drained) != 2 {
		t.Fatalf("DrainPending(100) with 2 tasks returned %d, want 2", len(drained))
	}
}

func TestScheduler_pendingCount(t *testing.T) {
	s := newScheduler(2)
	if s.PendingCount() != 0 {
		t.Fatalf("initial PendingCount = %d, want 0", s.PendingCount())
	}
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa", "bb", "cc"},
	})
	if s.PendingCount() != 3 {
		t.Fatalf("PendingCount after submit = %d, want 3", s.PendingCount())
	}
	s.PollTasks("worker-1")
	if s.PendingCount() != 2 {
		t.Fatalf("PendingCount after 1 poll = %d, want 2", s.PendingCount())
	}
}

func TestScheduler_getJob_notFound(t *testing.T) {
	s := newScheduler(2)
	_, err := s.GetJob("nonexistent-id")
	if err == nil {
		t.Fatal("GetJob for nonexistent ID should return error")
	}
}

func TestScheduler_listJobs(t *testing.T) {
	s := newScheduler(2)
	if len(s.ListJobs()) != 0 {
		t.Fatal("ListJobs on empty scheduler should return empty slice")
	}
	s.Submit(scheduler.SubmitJobRequest{Dataset: "d1", Algorithm: "a1", Shards: []string{"00"}})
	s.Submit(scheduler.SubmitJobRequest{Dataset: "d2", Algorithm: "a2", Shards: []string{"ff"}})
	if len(s.ListJobs()) != 2 {
		t.Fatalf("ListJobs returned %d jobs, want 2", len(s.ListJobs()))
	}
}
