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
