package ingestion

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/metadata"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

// fakeS3 answers every request 200 with an ETag, draining the body so the SDK's
// PutObject completes. It is enough to drive the small-file Put path end to end
// without a real MinIO, so the phase-timing instrumentation can be asserted.
func fakeS3(t *testing.T) *storage.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	store, err := storage.NewClient(context.Background(), storage.ClientConfig{
		Endpoint: srv.URL,
		Region: "us-east-1",
		Bucket: "test",
		AccessKeyID: "test",
		SecretAccessKey: "test",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("new storage client: %v", err)
	}
	return store
}

// TestIngestDirPhaseBreakdown ingests a handful of real temp files and asserts the
// per-phase timers are populated and internally consistent. This is what makes the
// "where does ingest time go" breakdown trustworthy: the counters demonstrably move.
func TestIngestDirPhaseBreakdown(t *testing.T) {
	dir := t.TempDir()
	const n = 12
	for i := 0; i < n; i++ {
		p := filepath.Join(dir, "img"+string(rune('a'+i))+".jpg")
		if err := os.WriteFile(p, []byte("fake-jpeg-bytes-"+string(rune('a'+i))), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	idx, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	defer idx.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := New(fakeS3(t), idx, 4, log)

	prog, err := p.IngestDir(context.Background(), dir, "train", []string{"test"})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	if prog.Processed != n {
		t.Fatalf("processed = %d, want %d (failed=%d)", prog.Processed, n, prog.Failed)
	}
	if prog.Failed != 0 {
		t.Fatalf("failed = %d, want 0", prog.Failed)
	}

	// Every phase that runs for a successful file must have registered time. A zero
	// here means the timer was wired to the wrong span or dropped by the worker loop.
	if prog.ChecksumNanos <= 0 {
		t.Errorf("checksum time not recorded: %d", prog.ChecksumNanos)
	}
	if prog.UploadNanos <= 0 {
		t.Errorf("upload time not recorded: %d", prog.UploadNanos)
	}
	if prog.IndexNanos <= 0 {
		t.Errorf("index time not recorded: %d", prog.IndexNanos)
	}
	if prog.OpenNanos < 0 {
		t.Errorf("open time negative: %d", prog.OpenNanos)
	}
}
