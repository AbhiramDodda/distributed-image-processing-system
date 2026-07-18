package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/diag"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

type Scheduler struct {
	mu diag.RWMutex
	jobs map[string]*Job
	tasks map[string]*Task
	pendingQ []string
	ring *cluster.Ring
	maxRetries int
	store RecordStore
	committer Committer
	commitDecider CommitDecider
	sideEffect SideEffect
	leaseChunk int64
	onJobDone func(jobID string)
	log *slog.Logger
}

// defaultLeaseChunk is how many items a worker is granted per lease renewal. It
// caps how far a worker can run ahead of its last progress report, which bounds
// the un-granted tail a steal can safely reclaim. Larger = fewer renewals but a
// coarser steal granularity.
const defaultLeaseChunk = 1000

// minStealItems is the smallest un-granted tail worth splitting: below this a
// steal cannot give each side at least one item.
const minStealItems = 2

func New(ring *cluster.Ring, maxRetries int, log *slog.Logger) *Scheduler {
	s := &Scheduler{
		jobs: make(map[string]*Job),
		tasks: make(map[string]*Task),
		ring: ring,
		maxRetries: maxRetries,
		leaseChunk: defaultLeaseChunk,
		log: log,
	}
	s.mu.SetName("scheduler.mu")
	return s
}

// SetJobDoneHook registers a callback fired exactly once when a job first
// reaches a terminal state (completed or failed). The coordinator uses it to
// release the job's admission ticket. It runs while the scheduler lock is held,
// so the callback must be non-blocking and must not call back into the scheduler
// (releasing an admission ticket takes only the controller's own lock, giving a
// fixed scheduler.mu -> admission.mu order with no cycle).
func (s *Scheduler) SetJobDoneHook(fn func(jobID string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onJobDone = fn
}

// SetLeaseChunk overrides the per-lease grant size (see defaultLeaseChunk). A
// non-positive value is ignored. Used to tune steal granularity and to drive
// deterministic splits in tests.
func (s *Scheduler) SetLeaseChunk(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaseChunk = n
}

func (s *Scheduler) Submit(req SubmitJobRequest) (*Job, error) {
	shards := req.Shards
	if len(shards) == 0 {
		shards = storage.AllShards()
	}

	job := &Job{
		ID: uuid.New().String(),
		Dataset: req.Dataset,
		Algorithm: req.Algorithm,
		Config: req.Config,
		Priority: req.Priority,
		Status: JobPending,
		Shards: shards,
		TotalTasks: len(shards),
		CreatedAt: time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
	for _, shard := range shards {
		t := &Task{
			ID: uuid.New().String(),
			JobID: job.ID,
			Shard: shard,
			Status: TaskPending,
			Priority: job.Priority,
			MaxRetries: s.maxRetries,
			RangeEnd: -1, // shard size unknown until a worker reports it
		}
		s.tasks[t.ID] = t
		s.pendingQ = append(s.pendingQ, t.ID)
	}

	now := time.Now()
	job.Status = JobRunning
	job.StartedAt = &now

	s.persistLocked()
	s.log.Info("job submitted", "job_id", job.ID, "dataset", job.Dataset, "tasks", job.TotalTasks)
	return job, nil
}

// PollTasks returns the next pending task for the given worker, preferring
// shards the ring assigns to that worker (data locality).
func (s *Scheduler) PollTasks(workerID string) (*TaskAssignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Prefer tasks where this worker is the ring's preferred node.
	// Fall back to any available task if none match.
	preferred := -1
	fallback := -1
	fallbackPri := 0
	for i, tid := range s.pendingQ {
		t, ok := s.tasks[tid]
		if !ok || t.Status != TaskPending {
			continue
		}
		owner, _ := s.ring.Lookup(t.Shard)
		if owner == workerID {
			preferred = i
			break
		}
		// No local task yet: hand idle capacity to the highest-priority pending
		// work, so an urgent job jumps the queue on non-owning workers.
		if fallback < 0 || t.Priority > fallbackPri {
			fallback = i
			fallbackPri = t.Priority
		}
	}

	idx := preferred
	if idx < 0 {
		idx = fallback
	}
	if idx < 0 {
		// No pending task: try to steal the un-granted tail of the busiest
		// in-flight task so this otherwise-idle worker has something to do.
		if t := s.stealLocked(); t != nil {
			a := s.assignLocked(t, workerID)
			s.persistLocked()
			return a, nil
		}
		return nil, nil
	}

	tid := s.pendingQ[idx]
	s.pendingQ = append(s.pendingQ[:idx], s.pendingQ[idx+1:]...)

	a := s.assignLocked(s.tasks[tid], workerID)
	s.persistLocked()
	return a, nil
}

// assignLocked marks t assigned to workerID, grants it a first chunk of work,
// and returns its assignment. Caller holds s.mu and is responsible for removing
// t from pendingQ if it was queued.
func (s *Scheduler) assignLocked(t *Task, workerID string) *TaskAssignment {
	now := time.Now()
	t.Status = TaskAssigned
	t.WorkerID = workerID
	t.AssignedAt = &now
	s.grantLocked(t)
	return s.assignmentFor(t)
}

// resetLeaseLocked returns a task's lease to the start of its owned range, so a
// reassignment after a retry or a dead-worker rebalance reprocesses the whole
// range [RangeStart, RangeEnd) from scratch rather than trusting the previous
// (possibly dead) worker's progress. Idempotent result commit makes the
// reprocessing safe. Caller holds s.mu.
func (s *Scheduler) resetLeaseLocked(t *Task) {
	t.Frontier = t.RangeStart
	t.Granted = t.RangeStart
}

// grantLocked extends a task's lease to cover its next chunk of items, never
// past a known end. It is the only place Granted advances: because a worker may
// process only up to Granted, the region [Granted, RangeEnd) is guaranteed
// untouched and therefore safe to steal. Caller holds s.mu.
func (s *Scheduler) grantLocked(t *Task) {
	g := t.Frontier + s.leaseChunk
	if t.RangeEnd >= 0 && g > t.RangeEnd {
		g = t.RangeEnd
	}
	if g > t.Granted {
		t.Granted = g
	}
	s.checkLeaseInvariant(t)
}

// checkLeaseInvariant asserts the core work-stealing safety invariant at runtime
// whenever diagnostics are on: RangeStart <= Frontier <= Granted <= RangeEnd.
// If it ever fails, a worker was leased past where it may safely run, or a steal
// reclaimed granted work — exactly the logical race that would otherwise cause
// silent double-processing. The Enabled() guard keeps this free in production.
// Caller holds s.mu (the fields are read without further locking).
func (s *Scheduler) checkLeaseInvariant(t *Task) {
	if !diag.Enabled() {
		return
	}
	ok := t.RangeStart <= t.Frontier && t.Frontier <= t.Granted
	if t.RangeEnd >= 0 {
		ok = ok && t.Granted <= t.RangeEnd
	}
	diag.Assert(ok, "lease ordering RangeStart<=Frontier<=Granted<=RangeEnd violated",
		"task", t.ID, "shard", t.Shard, "start", t.RangeStart, "frontier", t.Frontier,
		"granted", t.Granted, "end", t.RangeEnd, "generation", t.Generation)
}

// assignmentFor builds the wire assignment for a task, carrying its work-stealing
// range. Caller holds s.mu.
func (s *Scheduler) assignmentFor(t *Task) *TaskAssignment {
	job := s.jobs[t.JobID]
	return &TaskAssignment{
		TaskID: t.ID,
		JobID: t.JobID,
		Shard: t.Shard,
		Dataset: job.Dataset,
		Algorithm: job.Algorithm,
		Config: job.Config,
		RangeStart: t.RangeStart,
		RangeEnd: t.RangeEnd,
		Bound: t.Granted,
		Generation: t.Generation,
		Split: t.Split,
	}
}

// StartTask marks a task as running.
func (s *Scheduler) StartTask(taskID, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[taskID]
	if !ok {
		return fmt.Errorf("task not found: %s", taskID)
	}
	now := time.Now()
	t.Status = TaskRunning
	t.StartedAt = &now
	s.persistLocked()
	return nil
}

// ReportResult records a task completion or failure. On success it runs the
// two-phase commit: the worker's staged output (req.OutputKey) is promoted to
// the task's canonical key before the task is durably marked TaskDone. The
// commit is idempotent, so a duplicate or late report leaves exactly one final
// object and is otherwise a no-op.
func (s *Scheduler) ReportResult(ctx context.Context, taskID string, req ResultRequest) error {
	// Phase 1 (locked): validate and short-circuit an already-committed task, so
	// a duplicate at-least-once delivery does no work and cannot re-open a task.
	s.mu.Lock()
	t, ok := s.tasks[taskID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("task not found: %s", taskID)
	}
	if t.Status == TaskDone {
		s.mu.Unlock()
		return nil
	}
	jobID, shard := t.JobID, t.Shard
	rng := Range{Start: t.RangeStart, End: t.RangeEnd, Split: t.Split}
	gen := t.Generation
	s.mu.Unlock()

	finalKey := FinalResultKey(jobID, shard, rng)

	// Phase 2 (unlocked): promote the staged output to its canonical key. This is
	// deliberately outside the lock — a server-side copy is slow relative to
	// polling — and safe to run concurrently for the same task because Commit is
	// idempotent in the final key, so a duplicate or late report re-copies
	// identical bytes.
	if req.Error == "" && req.OutputKey != "" && s.committer != nil {
		if err := s.committer.Commit(ctx, req.OutputKey, finalKey); err != nil {
			return fmt.Errorf("commit task %s output: %w", taskID, err)
		}
	}

	// Phase 3 (unlocked): agree the terminal commit through consensus, when a
	// decider is configured. This is the atomic, replicated commit point that
	// upgrades the local WAL mark below: once Decide returns, the task is committed
	// as a majority-agreed fact (no split-brain), fenced by lease generation
	// against a stale attempt, so a failover leader never re-dispatches it.
	decision := CommitDecision{TaskID: taskID, JobID: jobID, Generation: gen, FinalKey: finalKey}
	if req.Error == "" && s.commitDecider != nil {
		winner, err := s.commitDecider.Decide(ctx, decision)
		if err != nil {
			return fmt.Errorf("commit task %s decision: %w", taskID, err)
		}
		decision = winner // a newer attempt may have won; fire the effect for it.
	}

	// Phase 3.5 (unlocked): fire the task's external side effect, stamped with its
	// deterministic idempotency key. It runs only after the commit is agreed (so it
	// never fires for a task that isn't truly committed) and before the WAL mark
	// below, which makes delivery at-least-once: a crash here re-reports and
	// re-delivers under the SAME key, which a key-deduping receiver collapses to a
	// single observable effect. This is the residual exactly-once gap made safe --
	// two heterogeneous systems can't be committed atomically, so the platform
	// generates a stable idempotency key and delivers at-least-once instead (see
	// design.md §3.1 and internal/effect).
	if req.Error == "" && s.sideEffect != nil {
		key := SideEffectKey(jobID, shard, rng)
		if err := s.sideEffect.Apply(ctx, key, decision); err != nil {
			return fmt.Errorf("apply task %s side effect: %w", taskID, err)
		}
	}

	// Phase 4 (locked): record the terminal state and persist it.
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok = s.tasks[taskID]
	if !ok {
		return fmt.Errorf("task not found: %s", taskID)
	}
	if t.Status == TaskDone {
		return nil // committed by a concurrent duplicate report while unlocked.
	}
	now := time.Now()
	t.FinishedAt = &now
	t.WorkerID = req.WorkerID

	if req.Error != "" {
		t.Error = req.Error
		if t.Retries < t.MaxRetries {
			t.Retries++
			t.Status = TaskPending
			t.WorkerID = ""
			s.resetLeaseLocked(t)
			s.pendingQ = append(s.pendingQ, t.ID)
			s.log.Warn("task queued for retry", "task_id", taskID, "retry", t.Retries)
		} else {
			t.Status = TaskFailed
			s.updateJob(t.JobID)
		}
		s.persistLocked()
		return nil
	}

	t.Status = TaskDone
	s.updateJob(t.JobID)
	s.persistLocked()
	return nil
}

// RenewLease records a worker's progress on a task and extends its lease. It is
// the mechanism that makes work-stealing safe: a worker may process only up to
// the returned Bound, so its true progress can never exceed the bound the
// scheduler last granted. The scheduler therefore knows the un-granted tail
// [Granted, RangeEnd) is untouched and can hand it to another worker.
//
// The worker reports its Frontier (offset of its next unprocessed item) and, on
// first call, Total (the shard's item count, which fixes RangeEnd for a
// whole-shard task and makes it splittable). The response's Stolen flag tells a
// worker whose tail was reassigned to wind down promptly.
func (s *Scheduler) RenewLease(taskID string, req RenewLeaseRequest) (LeaseRenewal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[taskID]
	if !ok {
		return LeaseRenewal{}, fmt.Errorf("task not found: %s", taskID)
	}

	// Fix the shard size on first report so the whole-shard task becomes
	// splittable. Total is the absolute item count; a stolen task already has a
	// concrete RangeEnd, so this only applies to the original whole-shard task.
	if t.RangeEnd < 0 && req.Total > 0 {
		t.RangeEnd = req.Total
	}

	// Advance the reported frontier monotonically; a late/stale report cannot
	// move it backwards. Clamp to the (possibly shrunk) end.
	if req.Frontier > t.Frontier {
		t.Frontier = req.Frontier
	}
	if t.RangeEnd >= 0 && t.Frontier > t.RangeEnd {
		t.Frontier = t.RangeEnd
	}

	// A worker can never legitimately hold a lease generation ahead of the
	// scheduler's: that would mean a steal it never saw, i.e. a lost bump. Flag it.
	diag.Assert(req.Generation <= t.Generation, "worker lease generation ahead of scheduler",
		"task", t.ID, "worker_gen", req.Generation, "task_gen", t.Generation)

	// Grant the next chunk: the bound the worker may now reach before renewing.
	s.grantLocked(t)
	s.persistLocked()

	return LeaseRenewal{
		Generation: t.Generation,
		Bound: t.Granted,
		Stolen: req.Generation < t.Generation,
	}, nil
}

// stealLocked finds the in-flight task with the largest un-granted tail and
// splits that tail into a new pending sub-task, returning it ready to assign.
// The split point lies at or beyond the victim's Granted bound, so no item the
// victim may have processed is reassigned (no double-processing) and the two
// ranges stay contiguous (no gap). Shrinking the victim's RangeEnd and bumping
// its Generation makes the victim stop at the split on its next renewal. Returns
// nil when no in-flight task has a tail worth splitting. Caller holds s.mu.
func (s *Scheduler) stealLocked() *Task {
	var victim *Task
	var bestTail int64
	for _, t := range s.tasks {
		if t.Status != TaskAssigned && t.Status != TaskRunning {
			continue
		}
		if t.RangeEnd < 0 { // shard size not yet reported; not splittable
			continue
		}
		tail := t.RangeEnd - t.Granted // the region the worker has not been leased
		if tail > bestTail {
			bestTail, victim = tail, t
		}
	}
	if victim == nil || bestTail < minStealItems {
		return nil
	}

	split := victim.Granted + bestTail/2
	if split <= victim.Granted || split >= victim.RangeEnd {
		return nil // nothing safely splittable
	}

	stolen := &Task{
		ID: uuid.New().String(),
		JobID: victim.JobID,
		Shard: victim.Shard,
		Status: TaskPending,
		MaxRetries: victim.MaxRetries,
		RangeStart: split,
		RangeEnd: victim.RangeEnd,
		Frontier: split,
		Granted: split,
		Split: true,
	}
	victim.RangeEnd = split
	victim.Generation++
	victim.Split = true

	// The two guarantees that make the steal safe, checked at runtime: the split
	// point is strictly past what the victim was leased to touch (no reclaim of
	// possibly-processed work), and the ranges stay contiguous (no gap/overlap).
	diag.Assert(stolen.RangeStart >= victim.Granted, "steal reclaimed granted work",
		"victim", victim.ID, "granted", victim.Granted, "split", split)
	diag.Assert(victim.RangeEnd == stolen.RangeStart, "steal left a gap or overlap",
		"victim_end", victim.RangeEnd, "stolen_start", stolen.RangeStart)

	s.tasks[stolen.ID] = stolen
	if j := s.jobs[victim.JobID]; j != nil {
		j.TotalTasks++
	}
	s.log.Info("stole task tail",
		"shard", victim.Shard, "victim", victim.ID, "stolen", stolen.ID,
		"split", split, "stolen_range", fmt.Sprintf("[%d,%d)", stolen.RangeStart, stolen.RangeEnd))
	return stolen
}

// RebalanceWorker re-queues all Assigned/Running tasks owned by a dead worker.
func (s *Scheduler) RebalanceWorker(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var requeued int
	for _, t := range s.tasks {
		if t.WorkerID != workerID {
			continue
		}
		if t.Status != TaskAssigned && t.Status != TaskRunning {
			continue
		}
		if t.Retries < t.MaxRetries {
			t.Retries++
			t.Status = TaskPending
			t.WorkerID = ""
			s.resetLeaseLocked(t)
			s.pendingQ = append(s.pendingQ, t.ID)
			requeued++
		} else {
			t.Status = TaskFailed
			s.updateJob(t.JobID)
		}
	}
	if requeued > 0 {
		s.persistLocked()
		s.log.Info("rebalanced tasks from dead worker", "worker_id", workerID, "requeued", requeued)
	}
}

func (s *Scheduler) GetJob(id string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job not found: %s", id)
	}
	return j, nil
}

// PendingCount returns the number of tasks waiting to be dispatched.
// Used by the operator to determine how many K8s Jobs to create, and
// exposed as a Prometheus metric for the HPA custom metric adapter.
func (s *Scheduler) PendingCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pendingQ)
}

// DrainPending removes and returns up to n pending task assignments.
// The operator calls this to get tasks it should create K8s Jobs for.
func (s *Scheduler) DrainPending(n int) []TaskAssignment {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > len(s.pendingQ) {
		n = len(s.pendingQ)
	}
	if n == 0 {
		return nil
	}
	ids := s.pendingQ[:n]
	s.pendingQ = s.pendingQ[n:]
	var assignments []TaskAssignment
	for _, tid := range ids {
		t, ok := s.tasks[tid]
		if !ok || t.Status != TaskPending {
			continue
		}
		if _, ok := s.jobs[t.JobID]; !ok {
			continue
		}
		now := time.Now()
		t.Status = TaskAssigned
		t.AssignedAt = &now
		s.grantLocked(t)
		assignments = append(assignments, *s.assignmentFor(t))
	}
	s.persistLocked()
	return assignments
}

// Tasks returns a snapshot copy of all tasks. Copies (not pointers) so callers
// — a status endpoint, or a test asserting steal invariants — cannot mutate live
// scheduler state.
func (s *Scheduler) Tasks() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, *t)
	}
	return out
}

func (s *Scheduler) ListJobs() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

// updateJob transitions a job to completed/failed once all tasks are settled.
// Caller must hold s.mu.
func (s *Scheduler) updateJob(jobID string) {
	j, ok := s.jobs[jobID]
	if !ok {
		return
	}
	var pending, done, failed int
	for _, t := range s.tasks {
		if t.JobID != jobID {
			continue
		}
		switch t.Status {
		case TaskDone:
			done++
		case TaskFailed:
			failed++
		default:
			pending++
		}
	}
	j.DoneTasks = done
	j.FailedTasks = failed
	if pending > 0 {
		return
	}
	now := time.Now()
	j.FinishedAt = &now
	if failed > 0 {
		j.Status = JobFailed
		j.Error = fmt.Sprintf("%d tasks failed", failed)
	} else {
		j.Status = JobCompleted
	}
	s.log.Info("job finished", "job_id", jobID, "status", j.Status)
	if s.onJobDone != nil {
		s.onJobDone(jobID)
	}
}
