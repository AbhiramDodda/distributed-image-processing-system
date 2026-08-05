package scheduler_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/diag"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/trace"
)

// TestChaos_schedulerUpholdsInvariantsUnderConcurrentFaults is the active
// counterpart to `go test -race`: instead of waiting for a bad interleaving to
// happen, it provokes one. A pool of worker goroutines hammers one big shard
// with the full operation mix -- poll, lease-renew with progress, report,
// idle-poll (which triggers a steal of the busiest task's untouched tail) --
// while faults are injected: transient failures that force retries, duplicate
// and late result reports, and workers that die mid-task without reporting.
//
// diag is on (so the lease-ordering and steal-no-reclaim assertions fire on any
// violation) and trace is on (so a failure can print the causal chain that
// produced the offending work). After the storm, a deterministic drain finishes
// whatever is left, and the run asserts the properties that a correct scheduler
// must keep no matter how the operations interleaved:
//
//   - zero diag invariant violations (RangeStart<=Frontier<=Granted<=RangeEnd,
//     no steal reclaimed granted work, generation monotonic);
//   - the tasks' ranges tile [0,total) exactly (no gap => nothing skipped, no
//     overlap => nothing processed twice);
//   - every task reached a terminal state;
//   - the committer holds exactly one object per committed task (idempotent
//     commit collapsed every duplicate/late report to a single result).
//
// Run under -race for the data-race half of the guarantee. Deterministic seeds
// keep each run's fault pattern reproducible.
func TestChaos_schedulerUpholdsInvariantsUnderConcurrentFaults(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 99} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runChaos(t, seed)
		})
	}
}

const (
	chaosTotal = 1200 // items in the single shard
	chaosChunk = 8 // lease chunk: small, so many renews and steals
	chaosWorkers = 12
	chaosBudget = 600 * time.Millisecond
)

func runChaos(t *testing.T, seed int64) {
	diag.Enable(nil, diag.Config{WaitWarn: time.Hour, HoldWarn: time.Hour})
	t.Cleanup(diag.Disable)
	trace.Enable()
	t.Cleanup(trace.Disable)
	beforeViol := diag.ViolationCount()

	s := newScheduler(8) // generous retries so transient faults don't exhaust them
	s.SetLeaseChunk(chaosChunk)
	fc := newFakeCommitter()
	s.AttachCommitter(fc)
	job := submitOneShard(t, s, "ff")

	// Dead workers coordinate with the drain phase only: a worker that dies stops
	// touching the scheduler entirely, so its abandoned task is safe to rebalance
	// once every goroutine has stopped (never mid-flight, which would violate the
	// dead-worker precondition and manufacture a false double-assignment).
	var deadMu sync.Mutex
	dead := map[string]bool{}
	markDead := func(w string) { deadMu.Lock(); dead[w] = true; deadMu.Unlock() }

	deadline := time.Now().Add(chaosBudget)
	var wg sync.WaitGroup
	for i := 0; i < chaosWorkers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wid := fmt.Sprintf("w%d", idx)
			rng := rand.New(rand.NewSource(seed*1000 + int64(idx)))
			flaky := idx%4 == 0 // a quarter of workers may die mid-task
			alive := true
			for alive && time.Now().Before(deadline) {
				a, err := s.PollTasks(wid)
				if err != nil || a == nil {
					continue
				}
				alive = processChaosTask(s, wid, rng, flaky, markDead, a)
			}
		}(i)
	}
	wg.Wait()

	// Drain (single-threaded, deterministic). Every goroutine has stopped, so any
	// task still held by a (now-quiescent) worker is safe to requeue. Rebalancing
	// all worker ids returns abandoned tasks to the pending queue; then complete
	// whatever remains until the whole shard is terminal.
	for i := 0; i < chaosWorkers; i++ {
		s.RebalanceWorker(fmt.Sprintf("w%d", i))
	}
	drainToCompletion(t, s, job)

	// --- assertions ---
	if got := diag.ViolationCount() - beforeViol; got != 0 {
		dumpTrace(t, job)
		t.Fatalf("seed %d: %d invariant violation(s) under chaos: %+v",
			seed, got, diag.RecentViolations())
	}

	tasks := tasksForJob(s, job)
	assertTiles(t, tasks, chaosTotal)

	// Coverage guard: the whole point is the concurrent split path. If the single
	// shard was never stolen into sub-tasks, the run proved nothing about stealing
	// and the green result is a false comfort -- fail loudly instead.
	if len(tasks) < 2 {
		t.Fatalf("seed %d: chaos exercised no work-stealing (only %d task); harness is not testing the split path",
			seed, len(tasks))
	}

	committed := 0
	for _, tk := range tasks {
		if tk.Status != scheduler.TaskDone && tk.Status != scheduler.TaskFailed {
			dumpTrace(t, job)
			t.Fatalf("seed %d: task %s left non-terminal in status %v", seed, tk.ID, tk.Status)
		}
		if tk.Status == scheduler.TaskDone {
			committed++
		}
	}
	// Idempotent commit: every duplicate/late report collapsed to one object, so
	// the committer holds exactly one object per committed task -- never two.
	if fc.objectCount() != committed {
		dumpTrace(t, job)
		t.Fatalf("seed %d: committer holds %d objects, want one per committed task (%d)",
			seed, fc.objectCount(), committed)
	}
}

// processChaosTask plays one worker attempt on assignment a. It advances a local
// frontier within the granted bound (never past it), renews to extend the lease,
// and injects faults. It returns false when the worker "died" (abandoned the task
// without reporting), true otherwise.
func processChaosTask(s *scheduler.Scheduler, wid string, rng *rand.Rand, flaky bool, markDead func(string), a *scheduler.TaskAssignment) bool {
	frontier := a.RangeStart
	bound := a.Bound
	stuck := 0
	for i := 0; i < chaosTotal+100; i++ {
		if flaky && rng.Float64() < 0.01 {
			markDead(wid)
			return false // die mid-task: no report, drain+rebalance will recover it
		}
		if frontier < bound {
			frontier += int64(rng.Intn(chaosChunk) + 1)
			if frontier > bound {
				frontier = bound
			}
		}
		r, err := s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{
			WorkerID: wid, Generation: a.Generation, Frontier: frontier, Total: chaosTotal,
		})
		if err != nil {
			return true
		}
		// Transient failure: report an error, which requeues the task for retry.
		if rng.Float64() < 0.03 {
			s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{
				WorkerID: wid, Error: "transient chaos fault",
			})
			return true
		}
		prevBound := bound
		bound = r.Bound
		if r.Stolen {
			frontier = bound // tail was stolen; finish only the shrunk piece
			break
		}
		if frontier >= bound && bound == prevBound {
			if stuck++; stuck >= 2 {
				break // lease stopped growing at RangeEnd: the piece is done
			}
		} else {
			stuck = 0
		}
	}
	out := scheduler.StagingResultKey(a.JobID, a.TaskID)
	req := scheduler.ResultRequest{
		WorkerID: wid, ImagesProcessed: frontier - a.RangeStart, OutputKey: out,
	}
	s.ReportResult(context.Background(), a.TaskID, req)
	// Duplicate / late report: must collapse to the same single committed object.
	if rng.Float64() < 0.15 {
		s.ReportResult(context.Background(), a.TaskID, req)
	}
	return true
}

// drainToCompletion finishes every remaining task single-threaded. With all chaos
// goroutines stopped and abandoned tasks rebalanced back to pending, one worker
// polling and completing each task in turn converges (no concurrent in-flight
// task exists for it to steal from).
func drainToCompletion(t *testing.T, s *scheduler.Scheduler, jobID string) {
	t.Helper()
	guard := 0
	for {
		if allTerminal(tasksForJob(s, jobID)) {
			return
		}
		if guard++; guard > 200000 {
			dumpTrace(t, jobID)
			t.Fatalf("drain did not converge: %+v", tasksForJob(s, jobID))
		}
		a, err := s.PollTasks("drain")
		if err != nil || a == nil {
			continue
		}
		frontier := a.RangeStart
		bound := a.Bound
		stuck := 0
		for {
			if frontier < bound {
				frontier = bound
			}
			r, err := s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{
				WorkerID: "drain", Generation: a.Generation, Frontier: frontier, Total: chaosTotal,
			})
			if err != nil {
				break
			}
			prevBound := bound
			bound = r.Bound
			if r.Stolen {
				frontier = bound
				break
			}
			if frontier >= bound && bound == prevBound {
				if stuck++; stuck >= 2 {
					break
				}
			}
		}
		s.ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{
			WorkerID: "drain", ImagesProcessed: frontier - a.RangeStart,
			OutputKey: scheduler.StagingResultKey(a.JobID, a.TaskID),
		})
	}
}

func allTerminal(tasks []scheduler.Task) bool {
	for _, tk := range tasks {
		if tk.Status != scheduler.TaskDone && tk.Status != scheduler.TaskFailed {
			return false
		}
	}
	return len(tasks) > 0
}

// dumpTrace prints the causal event trace for the job on a failure, so the
// interleaving that broke an invariant is legible instead of guessed at. This is
// the concrete payoff of pairing the chaos harness with event tracing.
func dumpTrace(t *testing.T, jobID string) {
	t.Helper()
	var shown int
	for _, e := range trace.Events() {
		if e.JobID != jobID {
			continue
		}
		t.Logf("trace %-13s task=%s gen=%d worker=%s parent=%s attrs=%v",
			e.Kind, e.TaskID, e.Generation, e.WorkerID, e.Parent, e.Attrs)
		if shown++; shown >= 60 {
			t.Logf("... (trace truncated)")
			break
		}
	}
}
