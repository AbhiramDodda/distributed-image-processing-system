package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/config"
)

func TestDefaultConfig_saneDefaults(t *testing.T) {
	cfg := config.DefaultConfig()

	if cfg.Coordinator.Port != 8090 {
		t.Errorf("coordinator port = %d, want 8090", cfg.Coordinator.Port)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("server port = %d, want 8080", cfg.Server.Port)
	}
	if cfg.Coordinator.VnodesPerNode != 150 {
		t.Errorf("vnodes = %d, want 150 (Cassandra default)", cfg.Coordinator.VnodesPerNode)
	}
	if cfg.Coordinator.SuspectTimeout != 10*time.Second {
		t.Errorf("suspect timeout = %v, want 10s", cfg.Coordinator.SuspectTimeout)
	}
	if cfg.Coordinator.DeadTimeout != 20*time.Second {
		t.Errorf("dead timeout = %v, want 20s", cfg.Coordinator.DeadTimeout)
	}
	// Multipart threshold should match the documented 100 MB
	if cfg.Storage.MultipartThreshold != 100*1024*1024 {
		t.Errorf("multipart threshold = %d, want 100 MB", cfg.Storage.MultipartThreshold)
	}
	if cfg.Operator.Namespace == "" {
		t.Error("operator namespace should have a default")
	}
}

func TestLoad_missingFileReturnsDefaults(t *testing.T) {
	cfg, err := config.Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("Load of missing file should return defaults, got error: %v", err)
	}
	if cfg.Coordinator.Port != 8090 {
		t.Errorf("default not applied: coordinator port = %d", cfg.Coordinator.Port)
	}
}

func TestLoad_overridesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	content := `
coordinator:
  port: 9999
  vnodes_per_node: 64
storage:
  bucket: my-bucket
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Coordinator.Port != 9999 {
		t.Errorf("port = %d, want 9999 (overridden)", cfg.Coordinator.Port)
	}
	if cfg.Coordinator.VnodesPerNode != 64 {
		t.Errorf("vnodes = %d, want 64 (overridden)", cfg.Coordinator.VnodesPerNode)
	}
	if cfg.Storage.Bucket != "my-bucket" {
		t.Errorf("bucket = %q, want my-bucket", cfg.Storage.Bucket)
	}
	// Unset fields must retain their defaults
	if cfg.Server.Port != 8080 {
		t.Errorf("server port = %d, want 8080 (default retained)", cfg.Server.Port)
	}
}

func TestLoad_invalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(path, []byte("coordinator: [this is not valid: yaml"), 0o644)
	if _, err := config.Load(path); err == nil {
		t.Fatal("Load of malformed YAML should return an error")
	}
}
