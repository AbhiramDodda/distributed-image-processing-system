package scheduler

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/storage"
)

type Scheduler struct {
	mu sync.RWMutex
	jobs map[string]*Job
	tasks map[string]*Task
	pendingQ []string
	ring *cluster.Ring
	maxRetries int
	log *slog.Logger
}

func New(ring *cluster.Ring, maxRetries int, log *slog.Logger) *Scheduler {
	return &Scheduler{
		jobs:       make(map[string]*Job),
		tasks:      make(map[string]*Task),
		ring:       ring,
		maxRetries: maxRetries,
		log:        log,
	}
}

func (s *Scheduler) Submit(req SubmitJobRequest) (*Job, error) {
	shards := req.Shards
	if len(shards) == 0 {
		shards = storage.AllShards()
	}

	job := &Job{
		ID:         uuid.New().String(),
		Dataset:    req.Dataset,
		Algorithm:  req.Algorithm,
		Config:     req.Config,
		Status:     JobPending,
		Shards:     shards,
		TotalTasks: len(shards),
		CreatedAt:  time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
	for _, shard := range shards {
		t := &Task{
			ID:         uuid.New().String(),
			JobID:      job.ID,
			Shard:      shard,
			Status:     TaskPending,
			MaxRetries: s.maxRetries,
		}
		s.tasks[t.ID] = t
		s.pendingQ = append(s.pendingQ, t.ID)
	}

	now := time.Now()
	job.Status = JobRunning
	job.StartedAt = &now

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
		if fallback < 0 {
			fallback = i
		}
	}

	idx := preferred
	if idx < 0 {
		idx = fallback
	}
	if idx < 0 {
		return nil, nil
	}

	tid := s.pendingQ[idx]
	s.pendingQ = append(s.pendingQ[:idx], s.pendingQ[idx+1:]...)

	t := s.tasks[tid]
	now := time.Now()
	t.Status = TaskAssigned
	t.WorkerID = workerID
	t.AssignedAt = &now

	job := s.jobs[t.JobID]
	return &TaskAssignment{
		TaskID:    t.ID,
		JobID:     t.JobID,
		Shard:     t.Shard,
		Dataset:   job.Dataset,
		Algorithm: job.Algorithm,
		Config:    job.Config,
	}, nil
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
	return nil
}

// ReportResult records a task completion or failure.
func (s *Scheduler) ReportResult(taskID string, req ResultRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[taskID]
	if !ok {
		return fmt.Errorf("task not found: %s", taskID)
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
			s.pendingQ = append(s.pendingQ, t.ID)
			s.log.Warn("task queued for retry", "task_id", taskID, "retry", t.Retries)
		} else {
			t.Status = TaskFailed
			s.updateJob(t.JobID)
		}
		return nil
	}

	t.Status = TaskDone
	s.updateJob(t.JobID)
	return nil
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
			s.pendingQ = append(s.pendingQ, t.ID)
			requeued++
		} else {
			t.Status = TaskFailed
			s.updateJob(t.JobID)
		}
	}
	if requeued > 0 {
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
	ids := s.pendingQ[:n]
	s.pendingQ = s.pendingQ[n:]
	var assignments []TaskAssignment
	for _, tid := range ids {
		t, ok := s.tasks[tid]
		if !ok || t.Status != TaskPending {
			continue
		}
		j, ok := s.jobs[t.JobID]
		if !ok {
			continue
		}
		now := time.Now()
		t.Status = TaskAssigned
		t.AssignedAt = &now
		assignments = append(assignments, TaskAssignment{
			TaskID:    t.ID,
			JobID:     t.JobID,
			Shard:     t.Shard,
			Dataset:   j.Dataset,
			Algorithm: j.Algorithm,
			Config:    j.Config,
		})
	}
	return assignments
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
}
