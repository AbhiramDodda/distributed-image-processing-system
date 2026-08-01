package worker

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/formats"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// fakeStore is an in-memory objectStore: a fixed set of listable objects plus a
// captured record of what runAlgorithm stages.
type fakeStore struct {
	objects map[string][]byte // key -> bytes, for ListPrefix/Get

	mu sync.Mutex
	puts map[string][]byte // key -> staged bytes, captured
	putContentType map[string]string
}

func (f *fakeStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	// runAlgorithm relies on lexicographic order (as S3 lists); mimic it.
	sortStrings(keys)
	return keys, nil
}

func (f *fakeStore) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	b := f.objects[key]
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

func (f *fakeStore) Put(ctx context.Context, key string, r io.Reader, contentType string) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.puts == nil {
		f.puts = map[string][]byte{}
		f.putContentType = map[string]string{}
	}
	f.puts[key] = b
	f.putContentType[key] = contentType
	return nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// TestRunAlgorithm_stagesParquetResult verifies the worker stages its result as a
// single-row Parquet file (not JSON) at the task's staging key, with the fields
// mapped correctly and the vector of bytes actually decodable by the same formats
// reader an Athena/DuckDB consumer would use.
func TestRunAlgorithm_stagesParquetResult(t *testing.T) {
	store := &fakeStore{objects: map[string][]byte{
		"cifar/shard-01/a.jpg": bytes.Repeat([]byte{0x1}, 10),
		"cifar/shard-01/b.jpg": bytes.Repeat([]byte{0x2}, 20),
		"cifar/shard-01/c.jpg": bytes.Repeat([]byte{0x3}, 30),
	}}
	w := &Worker{
		id: "worker-test",
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store: store,
	}

	a := scheduler.TaskAssignment{
		TaskID: "task-xyz",
		JobID: "job-abc",
		Shard: "shard-01",
		Dataset: "cifar",
		RangeStart: 0,
		RangeEnd: -1, // whole shard
		Bound: 1 << 30, // large enough that runAlgorithm never renews (no HTTP)
		Generation: 1,
	}

	processed, totalBytes, stagingKey, err := w.runAlgorithm(context.Background(), a)
	if err != nil {
		t.Fatalf("runAlgorithm: %v", err)
	}
	if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}
	if totalBytes != 60 {
		t.Fatalf("totalBytes = %d, want 60", totalBytes)
	}
	if want := scheduler.StagingResultKey("job-abc", "task-xyz"); stagingKey != want {
		t.Fatalf("stagingKey = %q, want %q", stagingKey, want)
	}
	if !strings.HasSuffix(stagingKey, ".parquet") {
		t.Fatalf("staging key %q is not a .parquet object", stagingKey)
	}

	staged, ok := store.puts[stagingKey]
	if !ok {
		t.Fatalf("nothing staged at %q; puts=%v", stagingKey, keysOf(store.puts))
	}
	if ct := store.putContentType[stagingKey]; ct != "application/vnd.apache.parquet" {
		t.Fatalf("staged content-type = %q, want application/vnd.apache.parquet", ct)
	}

	rows, err := formats.ReadResultsParquet(staged)
	if err != nil {
		t.Fatalf("staged object is not valid Parquet: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("staged rows = %d, want 1", len(rows))
	}
	got := rows[0]
	wantFinal := scheduler.FinalResultKey("job-abc", "shard-01", scheduler.Range{Start: 0, End: -1})
	want := formats.ResultRow{
		TaskID: "task-xyz",
		Shard: "shard-01",
		OutputKey: wantFinal,
		ImagesProcessed: 3,
		BytesRead: 60,
	}
	if got != want {
		t.Fatalf("staged row = %+v, want %+v", got, want)
	}
}

func keysOf(m map[string][]byte) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
