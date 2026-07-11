package scheduler_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// TestScheduler_concurrentPoll_noDoubleAssignment is the core correctness test
// for the platform: when many workers poll simultaneously, the scheduler must
// never hand the same pending task to two workers at once. Run with -race.
func TestScheduler_concurrentPoll_noDoubleAssignment(t *testing.T) {
	ring := cluster.NewRing(50)
	const workers = 16
	for i := 0; i < workers; i++ {
		ring.Add(fmt.Sprintf("w%d", i))
	}
	s := scheduler.New(ring, 0, testLog)

	// 256 shards = 256 tasks, one per shard
	job, _ := s.Submit(scheduler.SubmitJobRequest{
		Dataset:   "train",
		Algorithm: "resnet",
	})
	if job.TotalTasks != 256 {
		t.Fatalf("expected 256 tasks (full dataset), got %d", job.TotalTasks)
	}

	var mu sync.Mutex
	assignedTo := make(map[string]string) // taskID -> workerID

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				a, err := s.PollTasks(workerID)
				if err != nil {
					t.Errorf("PollTasks: %v", err)
					return
				}
				if a == nil {
					return // queue drained
				}
				mu.Lock()
				if prev, dup := assignedTo[a.TaskID]; dup {
					t.Errorf("task %s assigned twice: to %s and %s", a.TaskID, prev, workerID)
				}
				assignedTo[a.TaskID] = workerID
				mu.Unlock()
			}
		}(fmt.Sprintf("w%d", i))
	}
	wg.Wait()

	if len(assignedTo) != 256 {
		t.Fatalf("expected all 256 tasks assigned exactly once, got %d", len(assignedTo))
	}
	if s.PendingCount() != 0 {
		t.Errorf("PendingCount after full drain = %d, want 0", s.PendingCount())
	}
}

// TestScheduler_concurrentLifecycle exercises poll + report from many workers
// while the job drives to completion, under the race detector.
func TestScheduler_concurrentLifecycle(t *testing.T) {
	ring := cluster.NewRing(50)
	const workers = 8
	for i := 0; i < workers; i++ {
		ring.Add(fmt.Sprintf("w%d", i))
	}
	s := scheduler.New(ring, 2, testLog)

	job, _ := s.Submit(scheduler.SubmitJobRequest{Dataset: "ds", Algorithm: "a"})

	var completed sync.WaitGroup
	completed.Add(256)

	var doneOnce sync.Map // guards against double-counting on duplicate completion

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				a, err := s.PollTasks(workerID)
				if err != nil {
					t.Errorf("PollTasks: %v", err)
					return
				}
				if a == nil {
					return
				}
				if err := s.StartTask(a.TaskID, workerID); err != nil {
					t.Errorf("StartTask: %v", err)
				}
				if err := s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{
					WorkerID:        workerID,
					ImagesProcessed: 10,
				}); err != nil {
					t.Errorf("ReportResult: %v", err)
				}
				if _, loaded := doneOnce.LoadOrStore(a.TaskID, true); !loaded {
					completed.Done()
				}
			}
		}(fmt.Sprintf("w%d", i))
	}
	wg.Wait()
	completed.Wait()

	got, _ := s.GetJob(job.ID)
	if got.Status != scheduler.JobCompleted {
		t.Errorf("job status = %q, want completed", got.Status)
	}
	if got.DoneTasks != 256 {
		t.Errorf("DoneTasks = %d, want 256", got.DoneTasks)
	}
}

// TestScheduler_concurrentRebalance runs RebalanceWorker concurrently with
// polling to surface races between failure handling and task dispatch.
func TestScheduler_concurrentRebalance(t *testing.T) {
	ring := cluster.NewRing(50)
	ring.Add("w0")
	ring.Add("w1")
	s := scheduler.New(ring, 5, testLog)
	s.Submit(scheduler.SubmitJobRequest{Dataset: "ds", Algorithm: "a", Shards: shardRange(64)})

	var wg sync.WaitGroup
	// Poller
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			a, _ := s.PollTasks("w0")
			if a != nil {
				s.StartTask(a.TaskID, "w0")
			}
		}
	}()
	// Rebalancer firing repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			s.RebalanceWorker("w0")
		}
	}()
	wg.Wait()
	// No assertion on exact counts — the value here is -race catching data races.
}

func shardRange(n int) []string {
	shards := make([]string, n)
	for i := 0; i < n; i++ {
		shards[i] = fmt.Sprintf("%02x", i)
	}
	return shards
}
