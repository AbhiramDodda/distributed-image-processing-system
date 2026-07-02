package registry

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/abhiramd/petabyte-platform/internal/sandbox"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func sampleAlgo(name, version string) Algorithm {
	return Algorithm{
		Name: name,
		Version: version,
		Owner: "univ-a",
		ImageRef: "registry.internal/petabyte/" + name + ":deadbeef",
		Digest: "deadbeef",
		Manifest: sandbox.Manifest{
			Name: name,
			Version: version,
			BaseImage: "pytorch/pytorch:2.3.0",
			Entrypoint: "main.py",
			MemoryGB: 16,
			TimeoutSec: 300,
			Parallelism: sandbox.DataParallel,
		},
	}
}

func TestStore_registerAndGet(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.Register(ctx, sampleAlgo("clip", "1.0.0")); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := s.Get(ctx, "clip", "1.0.0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Owner != "univ-a" || got.Manifest.MemoryGB != 16 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt not populated on register")
	}
}

func TestStore_versionImmutable(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.Register(ctx, sampleAlgo("clip", "1.0.0")); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// Re-registering the same name+version must be rejected — a job's code
	// cannot change under it.
	if err := s.Register(ctx, sampleAlgo("clip", "1.0.0")); err == nil {
		t.Fatal("expected conflict re-registering same version")
	}
}

func TestStore_getMissing(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Get(context.Background(), "nope", "9.9.9"); err == nil {
		t.Fatal("expected not-found error")
	}
}

func TestStore_listAndVersions(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	for _, v := range []string{"1.0.0", "1.1.0", "2.0.0"} {
		if err := s.Register(ctx, sampleAlgo("clip", v)); err != nil {
			t.Fatalf("register %s: %v", v, err)
		}
	}
	if err := s.Register(ctx, sampleAlgo("resnet", "1.0.0")); err != nil {
		t.Fatalf("register resnet: %v", err)
	}

	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 4 {
		t.Errorf("List returned %d, want 4", len(all))
	}

	versions, err := s.ListVersions(ctx, "clip")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("clip has %d versions, want 3", len(versions))
	}
}
