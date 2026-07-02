package sandbox

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"testing"
)

// validManifest is a well-formed manifest reused across package tests.
func validManifest() Manifest {
	return Manifest{
		Name: "resnet-embed",
		Version: "1.2.0",
		BaseImage: "pytorch/pytorch:2.3.0-cuda12.1",
		Entrypoint: "main.py",
		GPURequired: true,
		GPUCount: 1,
		MemoryGB: 32,
		TimeoutSec: 600,
		Parallelism: DataParallel,
	}
}

// buildZip assembles an in-memory algorithm package from a file map.
func buildZip(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func validPackageFiles(t *testing.T, m Manifest) map[string][]byte {
	t.Helper()
	mj, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return map[string][]byte{
		"Dockerfile": []byte("FROM pytorch/pytorch:2.3.0-cuda12.1\nCOPY . /app\n"),
		"main.py": []byte("print('hello')\n"),
		"requirements.txt": []byte("torch\n"),
		"manifest.json": mj,
	}
}

func TestParsePackage_valid(t *testing.T) {
	m := validManifest()
	pkg, err := ParsePackage(buildZip(t, validPackageFiles(t, m)))
	if err != nil {
		t.Fatalf("ParsePackage: %v", err)
	}
	if pkg.Manifest.Name != m.Name || pkg.Manifest.Version != m.Version {
		t.Errorf("manifest = %+v, want name/version %s/%s", pkg.Manifest, m.Name, m.Version)
	}
	if len(pkg.Digest) != 64 {
		t.Errorf("digest = %q, want 64 hex chars", pkg.Digest)
	}
	// Identical bytes must yield an identical digest (dedupe guarantee).
	files := validPackageFiles(t, m)
	pkg2, _ := ParsePackage(buildZip(t, files))
	pkg3, _ := ParsePackage(buildZip(t, files))
	if pkg2.Digest != pkg3.Digest {
		t.Errorf("digests differ for identical content: %s vs %s", pkg2.Digest, pkg3.Digest)
	}
}

func TestParsePackage_missingRequiredFile(t *testing.T) {
	files := validPackageFiles(t, validManifest())
	delete(files, "requirements.txt")
	if _, err := ParsePackage(buildZip(t, files)); err == nil {
		t.Fatal("expected error for missing requirements.txt")
	}
}

func TestParsePackage_badManifestJSON(t *testing.T) {
	files := validPackageFiles(t, validManifest())
	files["manifest.json"] = []byte("{not json")
	if _, err := ParsePackage(buildZip(t, files)); err == nil {
		t.Fatal("expected error for malformed manifest.json")
	}
}

func TestParsePackage_invalidManifestShape(t *testing.T) {
	m := validManifest()
	m.TimeoutSec = 0 // must be positive
	files := validPackageFiles(t, m)
	if _, err := ParsePackage(buildZip(t, files)); err == nil {
		t.Fatal("expected shape error for timeout_s = 0")
	}
}

func TestParsePackage_gpuRequiredWithoutCount(t *testing.T) {
	m := validManifest()
	m.GPURequired = true
	m.GPUCount = 0
	files := validPackageFiles(t, m)
	if _, err := ParsePackage(buildZip(t, files)); err == nil {
		t.Fatal("expected error when gpu_required but gpu_count is 0")
	}
}

func TestParsePackage_notAZip(t *testing.T) {
	if _, err := ParsePackage([]byte("plain text, not a zip")); err == nil {
		t.Fatal("expected error for non-zip input")
	}
}
