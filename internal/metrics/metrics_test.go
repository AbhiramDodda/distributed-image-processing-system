package metrics

import (
	"sync"
	"testing"
)

func TestCounter_concurrentInc(t *testing.T) {
	c := &Counter{}
	const goroutines = 50
	const perGoroutine = 1000

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()

	if got := c.Value(); got != goroutines*perGoroutine {
		t.Errorf("counter = %d, want %d", got, goroutines*perGoroutine)
	}
}

func TestCounter_add(t *testing.T) {
	c := &Counter{}
	c.Add(5)
	c.Add(10)
	if c.Value() != 15 {
		t.Errorf("counter = %d, want 15", c.Value())
	}
}

func TestGauge_setIncDec(t *testing.T) {
	g := &Gauge{}
	g.Set(10)
	g.Inc()
	g.Inc()
	g.Dec()
	if g.Value() != 11 {
		t.Errorf("gauge = %d, want 11", g.Value())
	}
}

func TestHistogram_bucketing(t *testing.T) {
	h := NewHistogram([]int64{10, 100, 1000})
	values := []int64{5, 50, 500, 5000, 8} // buckets: ≤10, ≤100, ≤1000, inf, ≤10
	for _, v := range values {
		h.Observe(v)
	}

	snap := h.Snapshot()
	if snap["count"] != 5 {
		t.Errorf("count = %d, want 5", snap["count"])
	}
	if snap["sum"] != 5563 {
		t.Errorf("sum = %d, want 5563", snap["sum"])
	}
	if snap["le_10"] != 2 { // 5 and 8
		t.Errorf("le_10 = %d, want 2", snap["le_10"])
	}
	if snap["le_100"] != 1 { // 50
		t.Errorf("le_100 = %d, want 1", snap["le_100"])
	}
	if snap["le_1000"] != 1 { // 500
		t.Errorf("le_1000 = %d, want 1", snap["le_1000"])
	}
	if snap["le_inf"] != 1 { // 5000
		t.Errorf("le_inf = %d, want 1", snap["le_inf"])
	}
}

func TestCollector_snapshotKeys(t *testing.T) {
	c := NewCollector()
	c.JobsSubmitted.Inc()
	c.TasksDispatched.Add(3)
	c.ActiveWorkers.Set(4)

	snap := c.Snapshot()
	if snap["jobs_submitted"].(int64) != 1 {
		t.Errorf("jobs_submitted = %v, want 1", snap["jobs_submitted"])
	}
	if snap["tasks_dispatched"].(int64) != 3 {
		t.Errorf("tasks_dispatched = %v, want 3", snap["tasks_dispatched"])
	}
	if snap["active_workers"].(int64) != 4 {
		t.Errorf("active_workers = %v, want 4", snap["active_workers"])
	}
	if _, ok := snap["uptime_seconds"]; !ok {
		t.Error("snapshot missing uptime_seconds")
	}
}
