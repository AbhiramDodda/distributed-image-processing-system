package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/diag"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

// With diagnostics on, a full work-stealing run must not trip a single runtime
// invariant: the lease-ordering and steal-safety assertions stay silent,
// proving the scheduler upholds them under real repeated splits. It also
// confirms the diag layer is genuinely wired onto the scheduler's lock path
// (acquisitions were recorded through the instrumented mutex) — otherwise the
// invariant checks would be a false comfort.
func TestDiag_workStealingUpholdsInvariants(t *testing.T) {
	diag.Enable(nil, diag.Config{WaitWarn: time.Hour, HoldWarn: time.Hour})
	t.Cleanup(diag.Disable)
	before := diag.ViolationCount()

	s := newScheduler(3)
	s.SetLeaseChunk(10)
	job := submitOneShard(t, s, "ff")

	a, _ := s.PollTasks("w0")
	s.RenewLease(a.TaskID, scheduler.RenewLeaseRequest{WorkerID: "w0", Generation: a.Generation, Total: 1000})

	// Idle workers steal repeatedly; each renews its stolen piece with a little
	// progress, exercising grant/steal/renew — every path that asserts.
	for i := 1; i < 200; i++ {
		wid := fmt.Sprintf("w%d", i)
		got, _ := s.PollTasks(wid)
		if got == nil {
			break
		}
		s.RenewLease(got.TaskID, scheduler.RenewLeaseRequest{
			WorkerID: wid, Generation: got.Generation, Frontier: got.RangeStart + 1,
		})
	}

	assertTiles(t, tasksForJob(s, job), 1000)

	if got := diag.ViolationCount() - before; got != 0 {
		t.Fatalf("work-stealing tripped %d invariant violation(s): %+v", got, diag.RecentViolations())
	}

	var acq int64
	for _, ls := range diag.LockStats() {
		if ls.Name == "scheduler.mu" {
			acq = ls.Acquisitions
		}
	}
	if acq == 0 {
		t.Error("scheduler.mu recorded no acquisitions: the instrumented lock is not on the scheduler path")
	}
}
