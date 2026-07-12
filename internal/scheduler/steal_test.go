package scheduler_test

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// submitOneShard submits a single-shard job and returns the job.
func submitOneShard(t *testing.T, s *scheduler.Scheduler, shard string) string {
	t.Helper()
	job, err := s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{shard},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return job.ID
}

// tasksForJob returns this job's tasks sorted by RangeStart.
func tasksForJob(s *scheduler.Scheduler, jobID string) []scheduler.Task {
	var out []scheduler.Task
	for _, tk := range s.Tasks() {
		if tk.JobID == jobID {
			out = append(out, tk)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RangeStart < out[j].RangeStart })
	return out
}

// assertTiles verifies the tasks' ranges partition [0, total) exactly: contiguous,
// non-overlapping, fully covering. This is the core work-stealing safety property
// — no image is processed twice (overlap) and none is skipped (gap).
func assertTiles(t *testing.T, tasks []scheduler.Task, total int64) {
	t.Helper()
	if len(tasks) == 0 {
		t.Fatal("no tasks to tile")
	}
	var cursor int64
	for _, tk := range tasks {
		if tk.RangeEnd < tk.RangeStart {
			t.Fatalf("inverted range [%d,%d)", tk.RangeStart, tk.RangeEnd)
		}
		if tk.RangeStart != cursor {
			t.Fatalf("gap/overlap at offset %d: next range starts at %d", cursor, tk.RangeStart)
		}
		// Invariant: leased region [RangeStart, Granted) stays within the owned
		// range, so a steal never reclaimed work the victim was cleared to do.
		if tk.Granted < tk.RangeStart || tk.Granted > tk.RangeEnd {
			t.Fatalf("Granted %d outside owned range [%d,%d)", tk.Granted, tk.RangeStart, tk.RangeEnd)
		}
		cursor = tk.RangeEnd
	}
	if cursor != total {
		t.Fatalf("ranges cover [0,%d), want [0,%d)", cursor, total)
	}
}

// RenewLease fixes the shard size on first report and grants the next chunk.
func TestRenewLease_fixesSizeAndGrants(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	// Assignment grants the first chunk up front.
	if a.Bound != 10 {
		t.Fatalf("initial Bound = %d, want 10", a.Bound)
	}

	r, err := s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{
		WorkerID: "w0", Generation: a.Generation, Frontier: 8, Total: 100,
	})
	if err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if r.Bound != 18 { // frontier 8 + chunk 10
		t.Errorf("Bound after renew = %d, want 18", r.Bound)
	}
	if r.Stolen {
		t.Error("Stolen = true, want false (no steal happened)")
	}

	tk := tasksForJob(s, job)[0]
	if tk.RangeEnd != 100 {
		t.Errorf("RangeEnd = %d, want 100 (size fixed from Total)", tk.RangeEnd)
	}
}

// An idle worker steals the un-granted tail of the busy worker's task; the two
// ranges are disjoint and contiguous, and the split is at or beyond the victim's
// granted bound (so nothing the victim may have processed is reassigned).
func TestPollTasks_stealsUngrantedTail(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Total: 100})

	stolen, _ := s.PollTasks("w1")
	if stolen == nil {
		t.Fatal("second poll returned nil, want a stolen sub-task")
	}
	if !stolen.Split {
		t.Error("stolen assignment Split = false, want true")
	}
	// Granted for w0's task is 10, tail = 90, split = 10 + 45 = 55.
	if stolen.RangeStart != 55 || stolen.RangeEnd != 100 {
		t.Errorf("stolen range = [%d,%d), want [55,100)", stolen.RangeStart, stolen.RangeEnd)
	}

	tasks := tasksForJob(s, job)
	assertTiles(t, tasks, 100)

	var victim scheduler.Task
	for _, tk := range tasks {
		if tk.ID == a.TaskID {
			victim = tk
		}
	}
	if victim.RangeEnd != 55 {
		t.Errorf("victim RangeEnd = %d, want 55 (shrunk to split)", victim.RangeEnd)
	}
	if victim.Generation != 1 {
		t.Errorf("victim Generation = %d, want 1 (bumped by steal)", victim.Generation)
	}
	if stolen.RangeStart < victim.Granted {
		t.Errorf("split %d is behind victim Granted %d: risks double-processing", stolen.RangeStart, victim.Granted)
	}
}

// Repeated stealing splits one shard across many workers while always keeping the
// ranges a gap-free, overlap-free tiling of the shard.
func TestPollTasks_repeatedStealsTileShard(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Total: 100})

	// Idle workers keep stealing until no tail is worth splitting.
	steals := 0
	for i := 1; i < 100; i++ {
		got, _ := s.PollTasks(fmt.Sprintf("w%d", i))
		if got == nil {
			break
		}
		steals++
	}
	if steals == 0 {
		t.Fatal("no steals occurred")
	}
	assertTiles(t, tasksForJob(s, job), 100)
}

// A worker whose tail was stolen learns its shrunk bound and the steal on its
// next renewal.
func TestRenewLease_victimLearnsNewBound(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Total: 100})
	s.PollTasks("w1") // steals, bumping w0's task generation to 1 and RangeEnd to 55

	r, err := s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{
		WorkerID: "w0", Generation: a.Generation, Frontier: 12, Total: 100,
	})
	if err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if !r.Stolen {
		t.Error("Stolen = false, want true (generation is behind)")
	}
	if r.Bound != 22 { // normal next-chunk grant: frontier 12 + chunk 10
		t.Errorf("Bound = %d, want 22", r.Bound)
	}

	// The grant can never advance past the shrunk end: a renewal near the tail
	// clamps at 55, so the victim provably stops where the steal cut it.
	r2, _ := s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{
		WorkerID: "w0", Generation: r.Generation, Frontier: 50, Total: 100,
	})
	if r2.Bound != 55 {
		t.Errorf("Bound near tail = %d, want 55 (clamped to shrunk end)", r2.Bound)
	}
	_ = job
}

// A shard that fits in a single lease chunk has no un-granted tail, so it is
// never stolen: the second poll finds no work.
func TestPollTasks_noStealWhenFullyGranted(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(1000)
	submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Total: 100})

	if got, _ := s.PollTasks("w1"); got != nil {
		t.Fatalf("stole [%d,%d) from a fully-granted shard, want no steal", got.RangeStart, got.RangeEnd)
	}
}

// The steal targets the task with the largest un-granted tail.
func TestSteal_picksBusiest(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa", "bb"},
	})

	a1, _ := s.PollTasks("w0")
	a2, _ := s.PollTasks("w1")
	// aa is large (tail ~990), bb is small (tail ~40).
	s.RenewLease(a1.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a1.Generation, Total: 1000})
	s.RenewLease(a2.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w1", Generation: a2.Generation, Total: 50})

	stolen, _ := s.PollTasks("w2")
	if stolen == nil {
		t.Fatal("no steal")
	}
	if stolen.Shard != a1.Shard {
		t.Errorf("stole from shard %q, want the busier %q", stolen.Shard, a1.Shard)
	}
}

// A requeued task (here via dead-worker rebalance) resets its lease to the start
// of its owned range, so the next worker reprocesses the whole range rather than
// inheriting the dead worker's frontier.
func TestRebalance_resetsLease(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	// w0 makes progress, then is declared dead and rebalanced.
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Frontier: 40, Total: 100})
	s.RebalanceWorker("w0")

	tk := tasksForJob(s, job)[0]
	if tk.Status != scheduler.TaskPending {
		t.Fatalf("status = %q, want pending after rebalance", tk.Status)
	}
	if tk.Frontier != tk.RangeStart || tk.Granted != tk.RangeStart {
		t.Errorf("lease not reset: Frontier=%d Granted=%d, want both %d", tk.Frontier, tk.Granted, tk.RangeStart)
	}
}

// Concurrent idle workers stealing against one shard never produce overlapping
// ranges — the lock plus generation bump serialize splits.
func TestPollTasks_concurrentStealsNoOverlap(t *testing.T) {
	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Total: 1000})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s.PollTasks(fmt.Sprintf("w%d", n))
		}(i)
	}
	wg.Wait()

	assertTiles(t, tasksForJob(s, job), 1000)
}
