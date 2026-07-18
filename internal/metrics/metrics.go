package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	value int64
}

func (c *Counter) Inc() { atomic.AddInt64(&c.value, 1) }
func (c *Counter) Add(n int64) { atomic.AddInt64(&c.value, n) }
func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.value) }

type Gauge struct {
	value int64
}

func (g *Gauge) Set(n int64) { atomic.StoreInt64(&g.value, n) }
func (g *Gauge) Inc() { atomic.AddInt64(&g.value, 1) }
func (g *Gauge) Dec() { atomic.AddInt64(&g.value, -1) }
func (g *Gauge) Value() int64 { return atomic.LoadInt64(&g.value) }

type Histogram struct {
	mu sync.Mutex
	count int64
	sum int64
	buckets []int64
	bounds []int64
}

func NewHistogram(bounds []int64) *Histogram {
	return &Histogram{
		bounds: bounds,
		buckets: make([]int64, len(bounds)+1),
	}
}

func (h *Histogram) Observe(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range h.bounds {
		if v <= b {
			h.buckets[i]++
			return
		}
	}
	h.buckets[len(h.bounds)]++
}

func (h *Histogram) Snapshot() map[string]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]int64{"count": h.count, "sum": h.sum}
	for i, b := range h.bounds {
		out[fmt.Sprintf("le_%d", b)] = h.buckets[i]
	}
	out["le_inf"] = h.buckets[len(h.bounds)]
	return out
}

type Collector struct {
	JobsSubmitted *Counter
	JobsCompleted *Counter
	JobsFailed *Counter
	TasksDispatched *Counter
	TasksCompleted *Counter
	TasksFailed *Counter
	BytesProcessed *Counter
	RecordsProcessed *Counter
	ActiveWorkers *Gauge
	ActiveTasks *Gauge
	PendingJobs *Gauge
	TaskDuration *Histogram
	startTime time.Time
}

func NewCollector() *Collector {
	return &Collector{
		JobsSubmitted: &Counter{},
		JobsCompleted: &Counter{},
		JobsFailed: &Counter{},
		TasksDispatched: &Counter{},
		TasksCompleted: &Counter{},
		TasksFailed: &Counter{},
		BytesProcessed: &Counter{},
		RecordsProcessed: &Counter{},
		ActiveWorkers: &Gauge{},
		ActiveTasks: &Gauge{},
		PendingJobs: &Gauge{},
		TaskDuration: NewHistogram([]int64{100, 500, 1000, 5000, 10000, 30000, 60000, 300000}),
		startTime: time.Now(),
	}
}

func (c *Collector) Snapshot() map[string]any {
	return map[string]any{
		"uptime_seconds": time.Since(c.startTime).Seconds(),
		"jobs_submitted": c.JobsSubmitted.Value(),
		"jobs_completed": c.JobsCompleted.Value(),
		"jobs_failed": c.JobsFailed.Value(),
		"tasks_dispatched": c.TasksDispatched.Value(),
		"tasks_completed": c.TasksCompleted.Value(),
		"tasks_failed": c.TasksFailed.Value(),
		"bytes_processed": c.BytesProcessed.Value(),
		"records_processed": c.RecordsProcessed.Value(),
		"active_workers": c.ActiveWorkers.Value(),
		"active_tasks": c.ActiveTasks.Value(),
		"pending_jobs": c.PendingJobs.Value(),
		"task_duration": c.TaskDuration.Snapshot(),
	}
}

func (c *Collector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(c.Snapshot())
	}
}
