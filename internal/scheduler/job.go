package scheduler

import "time"

type JobStatus string

const (
	JobPending JobStatus = "pending"
	JobRunning JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed JobStatus = "failed"
	JobCancelled JobStatus = "cancelled"
)

type TaskStatus string

const (
	TaskPending TaskStatus = "pending"
	TaskAssigned TaskStatus = "assigned"
	TaskRunning TaskStatus = "running"
	TaskDone TaskStatus = "done"
	TaskFailed TaskStatus = "failed"
)

type Job struct {
	ID string `json:"id"`
	Dataset string `json:"dataset"`
	Algorithm string `json:"algorithm"`
	Config map[string]string `json:"config"`
	Status JobStatus `json:"status"`
	Shards []string `json:"shards"`
	TotalTasks int `json:"total_tasks"`
	DoneTasks int `json:"done_tasks"`
	FailedTasks int `json:"failed_tasks"`
	CreatedAt time.Time `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error string `json:"error,omitempty"`
}

type Task struct {
	ID string `json:"id"`
	JobID string `json:"job_id"`
	Shard string `json:"shard"`
	WorkerID string `json:"worker_id,omitempty"`
	Status TaskStatus `json:"status"`
	Retries int `json:"retries"`
	MaxRetries int `json:"max_retries"`
	AssignedAt *time.Time `json:"assigned_at,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error string `json:"error,omitempty"`

	// Work-stealing range. Offsets are into the shard's filename-sorted object
	// list; the task owns the half-open slice [RangeStart, RangeEnd). RangeEnd is
	// -1 until a worker reports the shard size (see RenewLease). Frontier is the
	// offset of the next unprocessed item the worker has reported; Granted is the
	// exclusive bound the worker is currently leased to process to. The steal is
	// safe because it only ever hands off the un-granted tail [Granted, RangeEnd),
	// which the worker provably has not touched. Invariant across all of these:
	// RangeStart <= Frontier <= Granted <= RangeEnd (once RangeEnd is known).
	RangeStart int64 `json:"range_start"`
	RangeEnd int64 `json:"range_end"`
	Frontier int64 `json:"frontier"`
	Granted int64 `json:"granted"`
	Generation int64 `json:"generation"`
	// Split is set once a task's range becomes a strict sub-range of its shard
	// (either it was stolen, or it had its tail stolen). It selects a range-scoped
	// result key so a shard's pieces don't collide; unsplit tasks keep the flat
	// per-shard key, so jobs that never split are byte-for-byte unchanged.
	Split bool `json:"split,omitempty"`
}

type TaskResult struct {
	TaskID string `json:"task_id"`
	JobID string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	ImagesProcessed int64 `json:"images_processed"`
	BytesRead int64 `json:"bytes_read"`
	OutputKey string `json:"output_key"`
	Duration time.Duration `json:"duration_ns"`
	Error string `json:"error,omitempty"`
}

type TaskAssignment struct {
	TaskID string `json:"task_id"`
	JobID string `json:"job_id"`
	Shard string `json:"shard"`
	Dataset string `json:"dataset"`
	Algorithm string `json:"algorithm"`
	Config map[string]string `json:"config"`

	// Range the worker should process within the shard. RangeStart is the
	// inclusive start offset; RangeEnd is the exclusive end (-1 = to end of shard,
	// discovered by the worker and reported back). Split marks a sub-range
	// assignment so the worker (and the commit) use a range-scoped result key.
	// Generation is the lease generation the worker echoes on RenewLease.
	RangeStart int64 `json:"range_start"`
	RangeEnd int64 `json:"range_end"`
	// Bound is the exclusive offset the worker may process up to before it must
	// RenewLease; it starts one lease chunk ahead of RangeStart so work can begin
	// without a round-trip.
	Bound int64 `json:"bound"`
	Generation int64 `json:"generation"`
	Split bool `json:"split,omitempty"`
}

// RenewLeaseRequest is a worker's periodic progress report. Frontier is the
// absolute offset of its next unprocessed item; Total is the shard's item count
// as the worker discovered it (recorded once, to make the task splittable);
// Generation is the lease generation the worker last held.
type RenewLeaseRequest struct {
	WorkerID string `json:"worker_id"`
	Generation int64 `json:"generation"`
	Frontier int64 `json:"frontier"`
	Total int64 `json:"total"`
}

// LeaseRenewal is the scheduler's response to a progress report. Bound is the
// exclusive offset the worker is now leased to process up to; it may only extend
// past Bound by renewing again. Stolen is true when the worker's tail was
// reassigned since it last renewed (its Generation is behind), so it should stop
// promptly at Bound.
type LeaseRenewal struct {
	Generation int64 `json:"generation"`
	Bound int64 `json:"bound"`
	Stolen bool `json:"stolen"`
}

type SubmitJobRequest struct {
	Dataset string `json:"dataset"`
	Algorithm string `json:"algorithm"`
	Config map[string]string `json:"config,omitempty"`
	Shards []string `json:"shards,omitempty"`
}

type SubmitJobResponse struct {
	JobID string `json:"job_id"`
	TotalTasks int `json:"total_tasks"`
}

type PollResponse struct {
	Assignment *TaskAssignment `json:"assignment"`
	HasWork bool `json:"has_work"`
}

type StartTaskRequest struct {
	WorkerID string `json:"worker_id"`
}

type ResultRequest struct {
	WorkerID string `json:"worker_id"`
	ImagesProcessed int64 `json:"images_processed"`
	BytesRead int64 `json:"bytes_read"`
	OutputKey string `json:"output_key"`
	Duration time.Duration `json:"duration_ns"`
	Error string `json:"error,omitempty"`
}
