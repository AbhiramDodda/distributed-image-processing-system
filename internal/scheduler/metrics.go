package scheduler

import (
	"sort"
	"time"
)

// Metrics is an aggregate snapshot of scheduler task state and task-processing
// latency, for the /v1/metrics/tasks endpoint. Latency percentiles are computed
// over tasks that have reached TaskDone, measured StartedAt -> FinishedAt (the
// worker's actual processing time, excluding queue wait).
type Metrics struct {
	TasksTotal int `json:"tasks_total"`
	Pending int `json:"pending"`
	Assigned int `json:"assigned"`
	Running int `json:"running"`
	Done int `json:"done"`
	Failed int `json:"failed"`
	// Rebalances is the total number of task requeues driven by a dead worker or a
	// retry (each retry increments a task's Retries). It is the count of extra
	// processing attempts caused by failures -- 0 in a clean run.
	Rebalances int `json:"rebalances"`
	// Latency of completed tasks (StartedAt -> FinishedAt), in milliseconds.
	LatencyP50Ms float64 `json:"latency_p50_ms"`
	LatencyP95Ms float64 `json:"latency_p95_ms"`
	LatencyP99Ms float64 `json:"latency_p99_ms"`
	LatencyMaxMs float64 `json:"latency_max_ms"`
	LatencyMeanMs float64 `json:"latency_mean_ms"`
}

// Metrics returns an aggregate snapshot of task state and completed-task latency.
func (s *Scheduler) Metrics() Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var m Metrics
	m.TasksTotal = len(s.tasks)
	latencies := make([]float64, 0, len(s.tasks))
	var sum float64
	for _, t := range s.tasks {
		switch t.Status {
		case TaskPending:
			m.Pending++
		case TaskAssigned:
			m.Assigned++
		case TaskRunning:
			m.Running++
		case TaskDone:
			m.Done++
		case TaskFailed:
			m.Failed++
		}
		m.Rebalances += t.Retries
		if t.Status == TaskDone && t.StartedAt != nil && t.FinishedAt != nil {
			ms := float64(t.FinishedAt.Sub(*t.StartedAt)) / float64(time.Millisecond)
			latencies = append(latencies, ms)
			sum += ms
		}
	}
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		m.LatencyP50Ms = percentile(latencies, 0.50)
		m.LatencyP95Ms = percentile(latencies, 0.95)
		m.LatencyP99Ms = percentile(latencies, 0.99)
		m.LatencyMaxMs = latencies[len(latencies)-1]
		m.LatencyMeanMs = sum / float64(len(latencies))
	}
	return m
}

// percentile returns the p-quantile (0..1) of a pre-sorted slice using
// nearest-rank; the slice must be non-empty and ascending.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := int(p * float64(len(sorted)-1))
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
