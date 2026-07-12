package coordinator_test

import (
	"context"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/coordinator"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

func persistCfg() *config.Config {
	return &config.Config{
		Coordinator: config.CoordinatorConfig{
			SuspectTimeout: 10 * time.Second,
			DeadTimeout:    20 * time.Second,
			VnodesPerNode:  50,
			TaskMaxRetries: 2,
		},
	}
}

// Simulates a coordinator crash (no clean Stop, so no final checkpoint): a
// fresh coordinator over the same WAL dir must recover job/task state by
// replaying the appended records.
func TestCoordinator_recoversAfterCrash(t *testing.T) {
	dir := t.TempDir()

	c1 := coordinator.New(persistCfg(), testLog)
	if err := c1.EnablePersistence(dir, time.Hour); err != nil {
		t.Fatalf("EnablePersistence c1: %v", err)
	}
	job, err := c1.Scheduler().Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"aa", "bb", "cc", "dd"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	a, err := c1.Scheduler().PollTasks("w1")
	if err != nil || a == nil {
		t.Fatalf("PollTasks: %v", err)
	}
	if err := c1.Scheduler().ReportResult(context.Background(), a.TaskID, scheduler.ResultRequest{WorkerID: "w1"}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}
	// No c1.Stop(): the WAL is left as a crashed process would leave it.

	c2 := coordinator.New(persistCfg(), testLog)
	if err := c2.EnablePersistence(dir, time.Hour); err != nil {
		t.Fatalf("EnablePersistence c2: %v", err)
	}
	t.Cleanup(c2.Stop)

	rj, err := c2.Scheduler().GetJob(job.ID)
	if err != nil {
		t.Fatalf("recovered GetJob: %v", err)
	}
	if rj.DoneTasks != 1 {
		t.Fatalf("recovered DoneTasks = %d, want 1", rj.DoneTasks)
	}
	if got := c2.Scheduler().PendingCount(); got != 3 {
		t.Fatalf("recovered PendingCount = %d, want 3", got)
	}
}

// After a clean Stop (which writes a final checkpoint and truncates the log),
// a restart must recover the same state from the snapshot alone.
func TestCoordinator_recoversAfterCleanCheckpoint(t *testing.T) {
	dir := t.TempDir()

	c1 := coordinator.New(persistCfg(), testLog)
	if err := c1.EnablePersistence(dir, time.Hour); err != nil {
		t.Fatalf("EnablePersistence c1: %v", err)
	}
	job, err := c1.Scheduler().Submit(scheduler.SubmitJobRequest{
		Dataset: "train", Algorithm: "resnet", Shards: []string{"00", "01"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	c1.Stop() // writes final checkpoint, truncates log, closes WAL

	c2 := coordinator.New(persistCfg(), testLog)
	if err := c2.EnablePersistence(dir, time.Hour); err != nil {
		t.Fatalf("EnablePersistence c2: %v", err)
	}
	t.Cleanup(c2.Stop)

	if _, err := c2.Scheduler().GetJob(job.ID); err != nil {
		t.Fatalf("recovered GetJob after checkpoint: %v", err)
	}
	if got := c2.Scheduler().PendingCount(); got != 2 {
		t.Fatalf("recovered PendingCount = %d, want 2", got)
	}
}
