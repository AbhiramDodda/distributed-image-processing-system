package sandbox

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeStore is an in-memory ObjectStore for exercising the runner without MinIO.
type fakeStore struct {
	objects map[string][]byte
	puts map[string][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, puts: map[string][]byte{}}
}

func (s *fakeStore) ListPrefix(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (s *fakeStore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	data := s.objects[key]
	return io.NopCloser(strings.NewReader(string(data))), int64(len(data)), nil
}

func (s *fakeStore) Put(_ context.Context, key string, r io.Reader, _ string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.puts[key] = data
	return nil
}

// fakeRuntime simulates a successful algorithm run: it verifies the input
// manifest was staged, then writes a result.json + artifact into the output dir.
type fakeRuntime struct {
	sawSpec ExecutionSpec
	sawManifestFiles []string
}

func (rt *fakeRuntime) Run(_ context.Context, spec ExecutionSpec) (*ExecutionResult, error) {
	rt.sawSpec = spec

	raw, err := os.ReadFile(filepath.Join(spec.InputDir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var m struct {
		Files []string `json:"files"`
	}
	json.Unmarshal(raw, &m)
	rt.sawManifestFiles = m.Files

	os.WriteFile(filepath.Join(spec.OutputDir, "embeddings.npy"), []byte("fake-embeddings"), 0o644)
	os.WriteFile(filepath.Join(spec.OutputDir, resultFile),
		[]byte(`{"images_processed": 2, "artifacts": ["embeddings.npy"]}`), 0o644)
	return &ExecutionResult{ExitCode: 0}, nil
}

func TestRunner_executeEndToEnd(t *testing.T) {
	store := newFakeStore()
	store.objects["train/a3/cat_0001.jpg"] = []byte("img-bytes-1")
	store.objects["train/a3/dog_0002.jpg"] = []byte("img-bytes-22")

	rt := &fakeRuntime{}
	runner := Runner{Runtime: rt, Store: store}
	spec := RunnerSpec{
		TaskID: "task-1",
		JobID: "job-1",
		Shard: "a3",
		Dataset: "train",
		Image: "registry.internal/petabyte/resnet:abc",
		Limits: ResourceLimits{CPUCores: 1, NetworkMode: "none"},
	}

	res, err := runner.Execute(context.Background(), spec, t.TempDir())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if res.ImagesProcessed != 2 {
		t.Errorf("ImagesProcessed = %d, want 2", res.ImagesProcessed)
	}
	if res.BytesRead != int64(len("img-bytes-1")+len("img-bytes-22")) {
		t.Errorf("BytesRead = %d, want %d", res.BytesRead, len("img-bytes-1")+len("img-bytes-22"))
	}
	if res.OutputKey != "results/job-1/a3/task-1.json" {
		t.Errorf("OutputKey = %q", res.OutputKey)
	}

	// The runtime must have seen both shard files staged into the input manifest.
	if len(rt.sawManifestFiles) != 2 {
		t.Errorf("staged files = %v, want 2 entries", rt.sawManifestFiles)
	}
	// The declared artifact must have been uploaded to the results bucket.
	if _, ok := store.puts["results/job-1/a3/embeddings.npy"]; !ok {
		t.Errorf("artifact not uploaded; puts = %v", keysOf(store.puts))
	}
	// The sandbox must have been given a read-only input mount and no network.
	if rt.sawSpec.Limits.NetworkMode != "none" {
		t.Errorf("runtime saw network %q, want none", rt.sawSpec.Limits.NetworkMode)
	}
}

// The task's idempotency key must reach the algorithm as an env var so it can
// forward it to any downstream it mutates -- the propagation half of exactly-once
// side effects. It must not clobber the caller's own env entries.
func TestRunner_injectsIdempotencyKey(t *testing.T) {
	store := newFakeStore()
	store.objects["train/a3/cat.jpg"] = []byte("x")

	rt := &fakeRuntime{}
	runner := Runner{Runtime: rt, Store: store}
	spec := RunnerSpec{
		TaskID: "task-1", JobID: "job-1", Shard: "a3", Dataset: "train", Image: "img",
		Env: map[string]string{"MODEL": "resnet"},
		IdempotencyKey: "sfx-deadbeef",
	}

	if _, err := runner.Execute(context.Background(), spec, t.TempDir()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := rt.sawSpec.Env[IdempotencyKeyEnv]; got != "sfx-deadbeef" {
		t.Errorf("%s = %q, want sfx-deadbeef", IdempotencyKeyEnv, got)
	}
	if got := rt.sawSpec.Env["MODEL"]; got != "resnet" {
		t.Errorf("caller env clobbered: MODEL = %q, want resnet", got)
	}
	// The caller's map must not have been mutated in place.
	if _, leaked := spec.Env[IdempotencyKeyEnv]; leaked {
		t.Error("injected key leaked back into caller's Env map")
	}
}

// errRuntime always fails, to verify the runner surfaces sandbox failures.
type errRuntime struct{}

func (errRuntime) Run(context.Context, ExecutionSpec) (*ExecutionResult, error) {
	return &ExecutionResult{ExitCode: 137, OOMKilled: true}, io.ErrUnexpectedEOF
}

func TestRunner_surfacesRuntimeFailure(t *testing.T) {
	store := newFakeStore()
	store.objects["train/a3/cat.jpg"] = []byte("x")
	runner := Runner{Runtime: errRuntime{}, Store: store}
	spec := RunnerSpec{TaskID: "t", JobID: "j", Shard: "a3", Dataset: "train", Image: "img"}

	res, err := runner.Execute(context.Background(), spec, t.TempDir())
	if err == nil {
		t.Fatal("expected error when runtime fails")
	}
	// Even on failure the runner reports what it read so the coordinator can bill.
	if res == nil || res.BytesRead != 1 {
		t.Errorf("expected partial result with BytesRead=1, got %+v", res)
	}
}

func keysOf(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
