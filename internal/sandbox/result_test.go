package sandbox

import (
	"os"
	"path/filepath"
	"testing"
)

func writeResult(t *testing.T, dir, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, resultFile), []byte(contents), 0o644); err != nil {
		t.Fatalf("write result: %v", err)
	}
}

func TestCollectOutput_valid(t *testing.T) {
	dir := t.TempDir()
	writeResult(t, dir, `{"images_processed": 1200, "images_failed": 3, "artifacts": ["embeddings.npy"]}`)

	out, err := CollectOutput(dir)
	if err != nil {
		t.Fatalf("CollectOutput: %v", err)
	}
	if out.ImagesProcessed != 1200 {
		t.Errorf("ImagesProcessed = %d, want 1200", out.ImagesProcessed)
	}
	if len(out.Artifacts) != 1 || out.Artifacts[0] != "embeddings.npy" {
		t.Errorf("Artifacts = %v, want [embeddings.npy]", out.Artifacts)
	}
}

func TestCollectOutput_missing(t *testing.T) {
	if _, err := CollectOutput(t.TempDir()); err == nil {
		t.Fatal("expected error when result.json is absent")
	}
}

func TestCollectOutput_malformed(t *testing.T) {
	dir := t.TempDir()
	writeResult(t, dir, "{not json")
	if _, err := CollectOutput(dir); err == nil {
		t.Fatal("expected error for malformed result.json")
	}
}

func TestCollectOutput_rejectsArtifactTraversal(t *testing.T) {
	dir := t.TempDir()
	writeResult(t, dir, `{"images_processed": 1, "artifacts": ["../../etc/passwd"]}`)
	if _, err := CollectOutput(dir); err == nil {
		t.Fatal("expected error for artifact path escaping output volume")
	}
}
