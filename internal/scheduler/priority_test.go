package scheduler_test

import (
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// A worker with no locally-owned task should be handed the highest-priority
// pending work, even when a lower-priority job was submitted (and enqueued)
// first. "worker-idle" is not in the ring, so it never matches a shard's
// preferred owner and always takes the priority-ordered fallback path.
func TestPriority_idleWorkerTakesHighestPriorityFirst(t *testing.T) {
	s := newScheduler(2)
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa"}, Priority: 0,
	})
	s.Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"bb"}, Priority: 10,
	})

	a, err := s.PollTasks("worker-idle")
	if err != nil {
		t.Fatalf("PollTasks: %v", err)
	}
	if a == nil {
		t.Fatal("no assignment with two pending tasks")
	}
	if a.Shard != "bb" {
		t.Errorf("dispatched shard = %q, want bb (the higher-priority job)", a.Shard)
	}

	// The remaining lower-priority task is served next.
	b, _ := s.PollTasks("worker-idle")
	if b == nil || b.Shard != "aa" {
		t.Fatalf("second dispatch = %+v, want shard aa", b)
	}
}
