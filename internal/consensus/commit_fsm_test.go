package consensus

import "testing"

func mustEncode(t *testing.T, c CommitCommand) []byte {
	t.Helper()
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func TestCommitFSM_recordsAndReads(t *testing.T) {
	f := NewCommitFSM()
	if err := f.Apply(mustEncode(t, CommitCommand{TaskID: "t1", JobID: "j1", Generation: 1, FinalKey: "results/j1/aa.json"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	rec, ok := f.Committed("t1")
	if !ok || rec.FinalKey != "results/j1/aa.json" || rec.Generation != 1 {
		t.Fatalf("committed = %+v (ok=%v), want gen 1 / results/j1/aa.json", rec, ok)
	}
}

// Re-applying the same (or an equal-generation) command must leave exactly one
// record: that is what makes an at-least-once retry commit once.
func TestCommitFSM_idempotent(t *testing.T) {
	f := NewCommitFSM()
	cmd := CommitCommand{TaskID: "t1", JobID: "j1", Generation: 2, FinalKey: "k"}
	for i := 0; i < 3; i++ {
		if err := f.Apply(mustEncode(t, cmd)); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if f.Len() != 1 {
		t.Fatalf("Len = %d after duplicate applies, want 1", f.Len())
	}
	if rec, _ := f.Committed("t1"); rec.Generation != 2 {
		t.Fatalf("generation = %d, want 2", rec.Generation)
	}
}

// A command with a generation not strictly greater than the recorded one is
// fenced: a stale/zombie attempt can never overwrite a newer commit, but a newer
// generation wins.
func TestCommitFSM_fencesByGeneration(t *testing.T) {
	f := NewCommitFSM()
	if err := f.Apply(mustEncode(t, CommitCommand{TaskID: "t1", Generation: 5, FinalKey: "new"})); err != nil {
		t.Fatal(err)
	}
	// Stale (lower generation) must not overwrite.
	if err := f.Apply(mustEncode(t, CommitCommand{TaskID: "t1", Generation: 3, FinalKey: "stale"})); err != nil {
		t.Fatal(err)
	}
	if rec, _ := f.Committed("t1"); rec.FinalKey != "new" || rec.Generation != 5 {
		t.Fatalf("stale attempt overwrote newer commit: %+v", rec)
	}
	// A strictly-newer generation wins.
	if err := f.Apply(mustEncode(t, CommitCommand{TaskID: "t1", Generation: 6, FinalKey: "newer"})); err != nil {
		t.Fatal(err)
	}
	if rec, _ := f.Committed("t1"); rec.FinalKey != "newer" || rec.Generation != 6 {
		t.Fatalf("newer generation did not win: %+v", rec)
	}
}

func TestCommitFSM_rejectsMalformed(t *testing.T) {
	f := NewCommitFSM()
	if err := f.Apply([]byte("not json")); err == nil {
		t.Fatal("malformed command applied without error")
	}
	if err := f.Apply(mustEncode(t, CommitCommand{TaskID: "", Generation: 1})); err == nil {
		t.Fatal("command missing task_id applied without error")
	}
}
