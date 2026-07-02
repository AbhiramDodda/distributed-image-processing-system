package sandbox

import (
	"slices"
	"strings"
	"testing"
)

func TestBuildRunArgs_isolationFlags(t *testing.T) {
	r := RunscRuntime{}
	spec := ExecutionSpec{
		Image: "registry.internal/petabyte/resnet:abc123",
		TaskID: "task-1",
		InputDir: "/scratch/input",
		OutputDir: "/scratch/output",
		Args: []string{"/input/manifest.json"},
		Limits: ResourceLimits{
			CPUCores: 4,
			MemoryBytes: 32 << 30,
			GPUCount: 1,
			NetworkMode: "none",
			TimeoutSec: 600,
		},
	}
	args := r.buildRunArgs(spec)
	joined := strings.Join(args, " ")

	// The security-critical flags must always be present.
	if !containsPair(args, "--runtime", "runsc") {
		t.Errorf("missing --runtime runsc in %v", args)
	}
	if !containsPair(args, "--network", "none") {
		t.Errorf("missing --network none in %v", args)
	}
	if !containsPair(args, "--cpus", "4") {
		t.Errorf("missing --cpus 4 in %v", args)
	}
	if !containsPair(args, "--gpus", "1") {
		t.Errorf("missing --gpus 1 in %v", args)
	}
	// Input must be mounted read-only; output writable.
	if !slices.Contains(args, "/scratch/input:/input:ro") {
		t.Errorf("input not mounted read-only: %v", args)
	}
	if !slices.Contains(args, "/scratch/output:/output") {
		t.Errorf("output not mounted: %v", args)
	}
	// Image and its args come last, in order.
	if !strings.HasSuffix(joined, "resnet:abc123 /input/manifest.json") {
		t.Errorf("image/args not trailing: %q", joined)
	}
}

func TestBuildRunArgs_noGPUWhenZero(t *testing.T) {
	r := RunscRuntime{}
	args := r.buildRunArgs(ExecutionSpec{Image: "img", TaskID: "t", Limits: ResourceLimits{CPUCores: 1}})
	if slices.Contains(args, "--gpus") {
		t.Errorf("--gpus should be absent when GPUCount is 0: %v", args)
	}
}

func TestBuildRunArgs_defaultsNetworkNone(t *testing.T) {
	r := RunscRuntime{}
	// Even if a caller forgets to set NetworkMode, the sandbox defaults to none.
	args := r.buildRunArgs(ExecutionSpec{Image: "img", TaskID: "t"})
	if !containsPair(args, "--network", "none") {
		t.Errorf("network should default to none: %v", args)
	}
}

func TestLimitsFromManifest_forcesNetworkNone(t *testing.T) {
	lim := LimitsFromManifest(Manifest{MemoryGB: 64, GPUCount: 2, TimeoutSec: 900})
	if lim.NetworkMode != "none" {
		t.Errorf("NetworkMode = %q, want none", lim.NetworkMode)
	}
	if lim.MemoryBytes != 64<<30 {
		t.Errorf("MemoryBytes = %d, want %d", lim.MemoryBytes, int64(64)<<30)
	}
	if lim.GPUCount != 2 {
		t.Errorf("GPUCount = %d, want 2", lim.GPUCount)
	}
}

func containsPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}
