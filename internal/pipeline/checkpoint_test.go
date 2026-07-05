package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func openWAL(t *testing.T, dir string) (*WAL, *Recovery) {
	t.Helper()
	w, rec, err := OpenWAL(dir, discardLog())
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, rec
}

func recordStrings(rec *Recovery) []string {
	out := make([]string, len(rec.Records))
	for i, r := range rec.Records {
		out[i] = string(r)
	}
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestWAL_appendAndRecover(t *testing.T) {
	dir := t.TempDir()
	w, rec := openWAL(t, dir)
	if rec.Snapshot != nil || len(rec.Records) != 0 {
		t.Fatalf("fresh WAL not empty: %+v", rec)
	}
	for _, r := range []string{"reg-worker-1", "submit-job-a", "assign-task-7"} {
		if err := w.Append([]byte(r)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	w.Close()

	_, rec2 := openWAL(t, dir)
	if rec2.Snapshot != nil {
		t.Fatalf("unexpected snapshot: %q", rec2.Snapshot)
	}
	want := []string{"reg-worker-1", "submit-job-a", "assign-task-7"}
	if got := recordStrings(rec2); !eqStrings(got, want) {
		t.Fatalf("recovered %v, want %v", got, want)
	}
}

func TestWAL_checkpointCompactsAndReplaysTail(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	w.Append([]byte("a"))
	w.Append([]byte("b"))
	if err := w.Checkpoint([]byte("STATE@2")); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	w.Append([]byte("c"))
	w.Close()

	_, rec := openWAL(t, dir)
	if string(rec.Snapshot) != "STATE@2" {
		t.Fatalf("snapshot = %q, want STATE@2", rec.Snapshot)
	}
	if got := recordStrings(rec); !eqStrings(got, []string{"c"}) {
		t.Fatalf("post-checkpoint records = %v, want [c]", got)
	}

	// The log file should have been compacted to just the tail record.
	info, err := os.Stat(filepath.Join(dir, walLogName))
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if want := int64(len(frame(3, []byte("c")))); info.Size() != want {
		t.Fatalf("compacted log size = %d, want %d", info.Size(), want)
	}
}

// Simulates a crash after the snapshot is written but before the log is
// truncated: the snapshot covers seq<=2 while the log still holds records 1..2.
// Recovery must not replay them.
func TestWAL_snapshotSeqPreventsDoubleApply(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	w.Append([]byte("a")) // seq 1
	w.Append([]byte("b")) // seq 2
	w.Close()

	// Write a snapshot at high-water seq 2 by hand, leaving the log intact.
	snap := frame(2, []byte("STATE@2"))
	if err := os.WriteFile(filepath.Join(dir, walSnapName), snap, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	w2, rec := openWAL(t, dir)
	if string(rec.Snapshot) != "STATE@2" {
		t.Fatalf("snapshot = %q", rec.Snapshot)
	}
	if len(rec.Records) != 0 {
		t.Fatalf("records seq<=snapshot were replayed: %v", recordStrings(rec))
	}
	// A new append must continue past the high-water seq.
	w2.Append([]byte("c"))
	w2.Close()

	_, rec2 := openWAL(t, dir)
	if got := recordStrings(rec2); !eqStrings(got, []string{"c"}) {
		t.Fatalf("after new append recovered %v, want [c]", got)
	}
}

func TestWAL_tornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	w.Append([]byte("r1"))
	w.Append([]byte("r2"))
	w.Close()

	logPath := filepath.Join(dir, walLogName)
	intact, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// Simulate a partially-flushed trailing frame.
	f, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0x00, 0x00, 0x05}) // 3 bytes: shorter than a frame header
	f.Close()

	_, rec := openWAL(t, dir)
	if got := recordStrings(rec); !eqStrings(got, []string{"r1", "r2"}) {
		t.Fatalf("recovered %v, want [r1 r2]", got)
	}
	if info, _ := os.Stat(logPath); info.Size() != intact.Size() {
		t.Fatalf("torn tail not truncated: size %d, want %d", info.Size(), intact.Size())
	}
}

func TestWAL_crcCorruptionDropsFrame(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	w.Append([]byte("r1"))
	w.Append([]byte("r2"))
	w.Append([]byte("r3"))
	w.Close()

	logPath := filepath.Join(dir, walLogName)
	data, _ := os.ReadFile(logPath)
	data[len(data)-1] ^= 0xFF // corrupt the last frame's payload
	os.WriteFile(logPath, data, 0o644)

	_, rec := openWAL(t, dir)
	if got := recordStrings(rec); !eqStrings(got, []string{"r1", "r2"}) {
		t.Fatalf("recovered %v, want [r1 r2] (corrupt r3 dropped)", got)
	}
}

func TestWAL_corruptSnapshotErrors(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	bad := frame(1, []byte("snap"))
	bad[len(bad)-1] ^= 0xFF
	if err := os.WriteFile(filepath.Join(dir, walSnapName), bad, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenWAL(dir, discardLog()); err == nil {
		t.Fatal("expected error opening WAL with corrupt snapshot")
	}
}
