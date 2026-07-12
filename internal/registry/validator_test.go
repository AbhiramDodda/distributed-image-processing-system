package registry

import (
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/sandbox"
)

func baseManifest() sandbox.Manifest {
	return sandbox.Manifest{
		Name: "clip",
		Version: "1.0.0",
		BaseImage: "pytorch/pytorch:2.3.0-cuda12.1",
		Entrypoint: "main.py",
		GPUCount: 1,
		MemoryGB: 32,
		TimeoutSec: 600,
		Parallelism: sandbox.DataParallel,
	}
}

func TestValidate_withinQuota(t *testing.T) {
	if err := Validate(baseManifest(), DefaultQuota()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidate_quotaBreaches(t *testing.T) {
	q := DefaultQuota()
	cases := map[string]func(m *sandbox.Manifest){
		"memory": func(m *sandbox.Manifest) { m.MemoryGB = q.MaxMemoryGB + 1 },
		"gpu": func(m *sandbox.Manifest) { m.GPUCount = q.MaxGPUCount + 1 },
		"timeout": func(m *sandbox.Manifest) { m.TimeoutSec = q.MaxTimeoutSec + 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := baseManifest()
			mutate(&m)
			if err := Validate(m, q); err == nil {
				t.Errorf("expected quota breach for %s", name)
			}
		})
	}
}

func TestValidate_baseImageAllowlist(t *testing.T) {
	m := baseManifest()
	m.BaseImage = "evilhub.io/backdoored:latest"
	if err := Validate(m, DefaultQuota()); err == nil {
		t.Fatal("expected rejection of non-allowlisted base image")
	}

	// Every allowlisted prefix must pass.
	for _, img := range []string{
		"python:3.11-slim",
		"nvidia/cuda:12.1-runtime",
		"tensorflow/tensorflow:2.15.0-gpu",
		"petabyte/base:v1",
	} {
		m.BaseImage = img
		if err := Validate(m, DefaultQuota()); err != nil {
			t.Errorf("base image %q should be allowed: %v", img, err)
		}
	}
}
