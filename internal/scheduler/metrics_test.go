package scheduler

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func metricsTestScheduler() *Scheduler {
	return New(nil, 3, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
}

func doneTask(id string, latency time.Duration, retries int) *Task {
	start := time.Now()
	fin := start.Add(latency)
	return &Task{ID: id, Status: TaskDone, Retries: retries, StartedAt: &start, FinishedAt: &fin}
}

func TestMetrics_countsAndLatency(t *testing.T) {
	s := metricsTestScheduler()
	// Five completed tasks with known latencies, one still running, one pending.
	s.tasks["a"] = doneTask("a", 100*time.Millisecond, 0)
	s.tasks["b"] = doneTask("b", 200*time.Millisecond, 1) // one rebalance
	s.tasks["c"] = doneTask("c", 300*time.Millisecond, 0)
	s.tasks["d"] = doneTask("d", 400*time.Millisecond, 2) // two rebalances
	s.tasks["e"] = doneTask("e", 500*time.Millisecond, 0)
	s.tasks["f"] = &Task{ID: "f", Status: TaskRunning}
	s.tasks["g"] = &Task{ID: "g", Status: TaskPending}

	m := s.Metrics()
	if m.TasksTotal != 7 || m.Done != 5 || m.Running != 1 || m.Pending != 1 {
		t.Fatalf("counts wrong: %+v", m)
	}
	if m.Rebalances != 3 {
		t.Fatalf("rebalances = %d, want 3", m.Rebalances)
	}
	if m.LatencyMaxMs != 500 {
		t.Fatalf("max latency = %v, want 500", m.LatencyMaxMs)
	}
	if m.LatencyMeanMs != 300 { // (100+200+300+400+500)/5
		t.Fatalf("mean latency = %v, want 300", m.LatencyMeanMs)
	}
	// nearest-rank p50 over [100,200,300,400,500] -> index int(0.5*4)=2 -> 300.
	if m.LatencyP50Ms != 300 {
		t.Fatalf("p50 = %v, want 300", m.LatencyP50Ms)
	}
}

func TestMetrics_emptyIsZero(t *testing.T) {
	m := metricsTestScheduler().Metrics()
	if m.TasksTotal != 0 || m.LatencyP95Ms != 0 || m.Rebalances != 0 {
		t.Fatalf("empty scheduler metrics not zero: %+v", m)
	}
}
