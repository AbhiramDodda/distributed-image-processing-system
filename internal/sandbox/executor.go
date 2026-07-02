package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

// ResourceLimits are the hard caps the container runtime and Linux cgroups
// enforce on one algorithm run. NetworkMode "none" is the security-critical
// default: untrusted code must not be able to phone home or exfiltrate raw
// image data — it may only read the mounted input and write the mounted output.
type ResourceLimits struct {
	CPUCores float64
	MemoryBytes int64
	GPUCount int
	DiskBytes int64
	NetworkMode string
	TimeoutSec int
}

// LimitsFromManifest derives runtime limits from a validated manifest. Network
// is forced to "none" regardless of what the manifest says; users do not get to
// opt out of the network sandbox.
func LimitsFromManifest(m Manifest) ResourceLimits {
	return ResourceLimits{
		CPUCores: float64(max(1, m.MemoryGB/8)),
		MemoryBytes: int64(m.MemoryGB) << 30,
		GPUCount: m.GPUCount,
		DiskBytes: 10 << 30,
		NetworkMode: "none",
		TimeoutSec: m.TimeoutSec,
	}
}

// ExecutionSpec fully describes one sandboxed run. InputDir is mounted
// read-only; OutputDir is the only writable mount.
type ExecutionSpec struct {
	Image string
	TaskID string
	InputDir string
	OutputDir string
	Args []string
	Env map[string]string
	Limits ResourceLimits
}

// ExecutionResult captures the outcome of a run. TimedOut and OOMKilled are
// distinguished because they drive different scheduler decisions: a timeout may
// be retried on a bigger box, an OOM should fail the task and bill the tenant.
type ExecutionResult struct {
	ExitCode int
	Stdout string
	Stderr string
	Duration time.Duration
	TimedOut bool
	OOMKilled bool
}

// Runtime abstracts the container runtime so the executor is testable without a
// real gVisor/containerd host. RunscRuntime is the production implementation;
// tests inject a fake.
type Runtime interface {
	Run(ctx context.Context, spec ExecutionSpec) (*ExecutionResult, error)
}

// RunscRuntime runs containers under gVisor (runsc) via the docker CLI. gVisor
// interposes syscalls in userspace, so a kernel exploit in user code cannot
// reach the host kernel — the ~15% CPU overhead buys that boundary.
type RunscRuntime struct {
	// DockerBin lets tests point at a stub; empty means "docker" on PATH.
	DockerBin string
	// Runtime is the containerd runtime handler name; empty means "runsc".
	Runtime string
}

func (r RunscRuntime) bin() string {
	if r.DockerBin != "" {
		return r.DockerBin
	}
	return "docker"
}

// buildRunArgs assembles the `docker run` argument vector for a spec. It is a
// pure function so the exact isolation flags can be asserted in tests without
// invoking docker.
func (r RunscRuntime) buildRunArgs(spec ExecutionSpec) []string {
	runtime := r.Runtime
	if runtime == "" {
		runtime = "runsc"
	}
	net := spec.Limits.NetworkMode
	if net == "" {
		net = "none"
	}

	args := []string{
		"run", "--rm",
		"--runtime", runtime,
		"--network", net,
		"--name", "petabyte-" + spec.TaskID,
	}
	if spec.Limits.CPUCores > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(spec.Limits.CPUCores, 'f', -1, 64))
	}
	if spec.Limits.MemoryBytes > 0 {
		args = append(args, "--memory", strconv.FormatInt(spec.Limits.MemoryBytes, 10))
	}
	if spec.Limits.GPUCount > 0 {
		args = append(args, "--gpus", strconv.Itoa(spec.Limits.GPUCount))
	}
	if spec.InputDir != "" {
		args = append(args, "-v", spec.InputDir+":/input:ro")
	}
	if spec.OutputDir != "" {
		args = append(args, "-v", spec.OutputDir+":/output")
	}
	for k, v := range spec.Env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, spec.Image)
	args = append(args, spec.Args...)
	return args
}

func (r RunscRuntime) Run(ctx context.Context, spec ExecutionSpec) (*ExecutionResult, error) {
	if spec.Limits.TimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(spec.Limits.TimeoutSec)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, r.bin(), r.buildRunArgs(spec)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	res := &ExecutionResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Duration: time.Since(start),
	}

	if ctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
		res.ExitCode = -1
		return res, fmt.Errorf("run %s: timed out after %ds", spec.TaskID, spec.Limits.TimeoutSec)
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		res.ExitCode = exitErr.ExitCode()
		// Docker reports OOM kills as exit code 137 (128 + SIGKILL).
		res.OOMKilled = res.ExitCode == 137
		return res, fmt.Errorf("run %s: exit %d", spec.TaskID, res.ExitCode)
	}
	if err != nil {
		return res, fmt.Errorf("run %s: %w", spec.TaskID, err)
	}
	return res, nil
}
