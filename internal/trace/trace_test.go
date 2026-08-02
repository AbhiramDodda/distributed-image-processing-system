package trace

import (
	"testing"
)

func setup(t *testing.T) {
	t.Cleanup(func() {
		Disable()
		reset()
	})
	reset()
	Enable()
}

func TestEmit_noopWhenDisabled(t *testing.T) {
	reset()
	Disable()
	Emit(Event{Kind: KindAssigned, TaskID: "t1"})
	if got := Count(); got != 0 {
		t.Fatalf("Count=%d, want 0 when disabled", got)
	}
	if len(Events()) != 0 {
		t.Fatalf("Events non-empty when disabled")
	}
}

func TestEmit_recordsWhenEnabled(t *testing.T) {
	setup(t)
	Emit(Event{Kind: KindAssigned, TaskID: "t1", TraceID: "tr-x"})
	evs := Events()
	if len(evs) != 1 {
		t.Fatalf("len(Events)=%d, want 1", len(evs))
	}
	if evs[0].TaskID != "t1" || evs[0].Kind != KindAssigned {
		t.Fatalf("unexpected event %+v", evs[0])
	}
	if evs[0].Time.IsZero() {
		t.Fatalf("Emit did not stamp Time")
	}
}

func TestTraceID_deterministicAndAttemptIndependent(t *testing.T) {
	// Same (job, shard, range) -> same id, regardless of who computes it.
	a := TraceID("job-1", "shard-07", 0, 100)
	b := TraceID("job-1", "shard-07", 0, 100)
	if a != b {
		t.Fatalf("TraceID not deterministic: %s vs %s", a, b)
	}
	// A different range (e.g. a stolen sub-task) gets a distinct id.
	if c := TraceID("job-1", "shard-07", 50, 100); c == a {
		t.Fatalf("different range produced same TraceID")
	}
	// Different job or shard also differs.
	if TraceID("job-2", "shard-07", 0, 100) == a {
		t.Fatalf("different job produced same TraceID")
	}
}

func TestEvents_ringBounded(t *testing.T) {
	setup(t)
	total := maxEvents + 50
	for i := 0; i < total; i++ {
		Emit(Event{Kind: KindGranted, TaskID: "t"})
	}
	if got := Count(); got != int64(total) {
		t.Fatalf("Count=%d, want %d", got, total)
	}
	if n := len(Events()); n != maxEvents {
		t.Fatalf("retained %d events, want cap %d", n, maxEvents)
	}
}

func TestHandlerGroup_causalLineage(t *testing.T) {
	setup(t)
	// A victim task and a stolen child that names it as parent, under the same
	// job/shard but different ranges (hence different TraceIDs).
	victimTrace := TraceID("job-1", "s1", 0, 200)
	childTrace := TraceID("job-1", "s1", 150, 200)
	Emit(Event{Kind: KindAssigned, TraceID: victimTrace, TaskID: "victim", JobID: "job-1", Shard: "s1"})
	Emit(Event{Kind: KindStolen, TraceID: childTrace, TaskID: "child", JobID: "job-1", Shard: "s1", Parent: "victim"})

	groups := group(Events())
	if len(groups) != 2 {
		t.Fatalf("group count=%d, want 2", len(groups))
	}
	// Find the child's group and confirm the parent link is recorded.
	var found bool
	for _, g := range groups {
		if g.TraceID == childTrace {
			found = true
			if len(g.Parents) != 1 || g.Parents[0] != "victim" {
				t.Fatalf("child group parents=%v, want [victim]", g.Parents)
			}
		}
	}
	if !found {
		t.Fatalf("child trace group %s not found", childTrace)
	}
}
