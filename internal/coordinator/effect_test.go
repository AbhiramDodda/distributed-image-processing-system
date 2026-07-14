package coordinator

import (
	"context"
	"sync"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/effect"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// The completion sink emits a committed task's event exactly once, no matter how
// many times the scheduler delivers it (at-least-once across retries/failover),
// and emits once per distinct committed unit of work.
func TestCompletionSink_emitsOncePerKey(t *testing.T) {
	var mu sync.Mutex
	var got []CompletionEvent
	emit := func(_ context.Context, ev CompletionEvent) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, ev)
		return nil
	}
	sink := &completionSink{ledger: effect.NewMemLedger(), emit: emit}

	keyA := scheduler.SideEffectKey("job1", "ff", scheduler.Range{End: -1})
	keyB := scheduler.SideEffectKey("job1", "a3", scheduler.Range{End: -1})
	decA := scheduler.CommitDecision{TaskID: "tA", JobID: "job1", FinalKey: "results/job1/ff.json"}
	decB := scheduler.CommitDecision{TaskID: "tB", JobID: "job1", FinalKey: "results/job1/a3.json"}

	// Deliver A four times (duplicates) and B once.
	for i := 0; i < 4; i++ {
		if err := sink.Apply(context.Background(), keyA, decA); err != nil {
			t.Fatalf("Apply A #%d: %v", i, err)
		}
	}
	if err := sink.Apply(context.Background(), keyB, decB); err != nil {
		t.Fatalf("Apply B: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("emitted %d events, want 2 (one per committed unit)", len(got))
	}
	seen := map[string]bool{got[0].Key: true, got[1].Key: true}
	if !seen[keyA] || !seen[keyB] {
		t.Fatalf("emitted keys %v, want {%s,%s}", seen, keyA, keyB)
	}
}
